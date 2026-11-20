use ckb_app_config::{ExitCode, ProfileArgs, ReplayArgs};
use ckb_async_runtime::Handle;
use ckb_chain::ChainController;
use ckb_chain_iter::ChainIterator;
use ckb_instrument::{ProgressBar, ProgressStyle};
use ckb_shared::{ChainServicesBuilder, Shared, SharedBuilder};
use ckb_store::ChainStore;
use ckb_types::{
    core::cell::{
        BlockCellProvider, CellMeta, CellProvider, CellStatus, OverlayCellProvider,
        ResolvedTransaction, resolve_transaction,
    },
    packed::{CellOutput, OutPoint},
};
use ckb_verification::cache::init_cache;
use ckb_verification_contextual::{ContextualBlockVerifier, VerifyContext};
use ckb_verification_traits::Switch;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const MIN_PROFILING_TIME: u64 = 2;
const VERIFY_THREADS_ENV: &str = "CKB_CONTEXTUAL_VERIFY_THREADS";

pub fn replay(args: ReplayArgs, async_handle: Handle) -> Result<(), ExitCode> {
    let shared_builder = SharedBuilder::new(
        &args.config.bin_name,
        args.config.root_dir.as_path(),
        &args.config.db,
        None,
        async_handle.clone(),
        args.consensus.clone(),
    )?;
    let (shared, _) = shared_builder
        .tx_pool_config(args.config.tx_pool.clone())
        .build()?;

    if args.verify_only {
        return verify_range(
            &shared,
            &async_handle,
            args.profile,
            args.timing_csv.as_deref(),
        );
    }

    let tmp_target = args.tmp_target.as_ref().ok_or_else(|| {
        eprintln!("Replay error: --tmp-target is required unless --verify-only is used");
        ExitCode::Cli
    })?;
    if !tmp_target.is_dir() {
        eprintln!(
            "Replay error: {:?}",
            "The specified path does not exist or not directory"
        );
        return Err(ExitCode::Failure);
    }
    let tmp_db_dir = tempfile::tempdir_in(tmp_target).map_err(|err| {
        eprintln!("Replay error: {err:?}");
        ExitCode::Failure
    })?;
    {
        let mut tmp_db_config = args.config.db.clone();
        tmp_db_config.path = tmp_db_dir.path().to_path_buf();

        let shared_builder = SharedBuilder::new(
            &args.config.bin_name,
            args.config.root_dir.as_path(),
            &tmp_db_config,
            None,
            async_handle,
            args.consensus,
        )?;
        let (_tmp_shared, mut pack) = shared_builder.tx_pool_config(args.config.tx_pool).build()?;
        let chain_service_builder: ChainServicesBuilder = pack.take_chain_services_builder();
        let chain_controller = ckb_chain::start_chain_services(chain_service_builder);

        if let Some((from, to)) = args.profile {
            profile(shared, chain_controller, from, to);
        } else if args.sanity_check {
            sanity_check(shared, chain_controller, args.full_verification);
        }
    }
    tmp_db_dir.close().map_err(|err| {
        eprintln!("Replay error: {err:?}");
        ExitCode::Failure
    })?;

    Ok(())
}

fn profile(shared: Shared, chain_controller: ChainController, from: Option<u64>, to: Option<u64>) {
    let tip_number = shared.snapshot().tip_number();
    let from = from.map(|v| std::cmp::max(1, v)).unwrap_or(1);
    let to = to
        .map(|v| std::cmp::min(v, tip_number))
        .unwrap_or(tip_number);
    process_range_block(&shared, chain_controller.clone(), 1..from);
    println!("Start profiling, re-process blocks {from}..{to}:");
    let now = std::time::Instant::now();
    let tx_count = process_range_block(&shared, chain_controller, from..=to);
    let duration = std::time::Instant::now().saturating_duration_since(now);
    if duration.as_secs() < MIN_PROFILING_TIME {
        println!(
            concat!(
                "----------------------------\n",
                r#"Profiling with too short time({:?}) is inaccurate and referential; it's recommended to modify"#,
                "\n",
                r#"parameters(--from, --to) to increase block range, to make profiling time is greater than "#,
                "{} seconds\n----------------------------",
            ),
            duration, MIN_PROFILING_TIME
        );
    }
    let tps = if duration.as_secs() == 0 {
        0
    } else {
        tx_count as u64 / duration.as_secs()
    };
    println!(
        "\n----------------------------\nEnd profiling, duration:{:?}, txs:{}, tps:{}\n----------------------------",
        duration, tx_count, tps
    );
}

fn process_range_block(
    shared: &Shared,
    chain_controller: ChainController,
    range: impl Iterator<Item = u64>,
) -> usize {
    let mut tx_count = 0;
    let snapshot = shared.snapshot();
    for index in range {
        let block = snapshot
            .get_block_hash(index)
            .and_then(|hash| snapshot.get_block(&hash))
            .expect("read block from store");
        tx_count += block.transactions().len().saturating_sub(1);
        chain_controller
            .blocking_process_block_with_switch(Arc::new(block), Switch::NONE)
            .unwrap();
    }
    tx_count
}

fn sanity_check(shared: Shared, chain_controller: ChainController, full_verification: bool) {
    let tip_header = shared.snapshot().tip_header().clone();
    let chain_iter = ChainIterator::new(shared.store());
    let pb = ProgressBar::new(chain_iter.len());
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .expect("Failed to set progress bar template")
            .progress_chars("#>-"),
    );
    let switch = if full_verification {
        Switch::NONE
    } else {
        Switch::DISABLE_ALL - Switch::DISABLE_NON_CONTEXTUAL
    };
    let mut cursor = shared.consensus().genesis_block().header();
    for block in chain_iter {
        let header = block.header();
        if let Err(e) = chain_controller.blocking_process_block_with_switch(Arc::new(block), switch)
        {
            eprintln!(
                "Replay sanity-check error: {:?} at block({}-{})",
                e,
                header.number(),
                header.hash(),
            );
            pb.finish_with_message("replay finish");
            return;
        } else {
            pb.inc(1);
            cursor = header;
        }
    }
    pb.finish_with_message("finish");

    if cursor != tip_header {
        eprintln!(
            "Sanity-check break at block({}-{}); expect tip({}-{})",
            cursor.number(),
            cursor.hash(),
            tip_header.number(),
            tip_header.hash(),
        );
    } else {
        println!(
            "Sanity-check pass, tip({}-{})",
            tip_header.number(),
            tip_header.hash()
        );
    }

    println!("Finishing replay; please wait...");
}

/// Cell provider for contextual verification of historical blocks.
///
/// The chain DB only keeps live cells, so inputs that have been spent since the
/// verified block are normally unavailable. Reconstruct those cells from the
/// transaction that created them, which the transaction index keeps. Cells that
/// are still live take the regular path.
///
/// This does not check whether a reconstructed cell was already spent before
/// the verified block; it is meant for benchmarking known-valid mainnet blocks.
struct HistoricalCellProvider<S> {
    store: S,
}

impl<S: ChainStore> CellProvider for HistoricalCellProvider<&S> {
    fn cell(&self, out_point: &OutPoint, eager_load: bool) -> CellStatus {
        if let Some(mut cell_meta) = self.store.get_cell(out_point) {
            if eager_load && let Some((data, data_hash)) = self.store.get_cell_data(out_point) {
                cell_meta.mem_cell_data = Some(data);
                cell_meta.mem_cell_data_hash = Some(data_hash);
            }
            return CellStatus::live_cell(cell_meta);
        }

        let tx_hash = out_point.tx_hash();
        let (transaction, _) = match self.store.get_transaction(&tx_hash) {
            Some(transaction) => transaction,
            None => return CellStatus::Unknown,
        };
        let index: usize = out_point.index().into();
        let cell_output = match transaction.outputs().get(index) {
            Some(cell_output) => cell_output,
            None => return CellStatus::Unknown,
        };
        let data = transaction
            .outputs_data()
            .get(index)
            .map(|data| data.raw_data())
            .unwrap_or_default();
        let data_hash = CellOutput::calc_data_hash(&data);
        CellStatus::live_cell(CellMeta {
            cell_output,
            out_point: out_point.clone(),
            transaction_info: self.store.get_transaction_info(&tx_hash),
            data_bytes: data.len() as u64,
            mem_cell_data: Some(data),
            mem_cell_data_hash: Some(data_hash),
        })
    }
}

/// Contextually verify blocks `[from, to]` against the existing chain DB.
///
/// Unlike the profiling mode, this neither replays the chain prefix nor attaches
/// the blocks: it resolves each block against the current DB and runs the same
/// [`ContextualBlockVerifier`] used during block import. The parallelism of the
/// transaction verification is controlled by `CKB_CONTEXTUAL_VERIFY_THREADS`.
///
/// Besides the total contextual verification time, the per-transaction
/// verification phase is reported separately.
fn verify_range(
    shared: &Shared,
    async_handle: &Handle,
    range: ProfileArgs,
    timing_csv: Option<&Path>,
) -> Result<(), ExitCode> {
    let snapshot = Arc::clone(&shared.snapshot());
    let tip_number = snapshot.tip_number();
    let (from, to) = range.unwrap_or((None, None));
    let from = from.map(|value| std::cmp::max(1, value)).unwrap_or(1);
    let to = to
        .map(|value| std::cmp::min(value, tip_number))
        .unwrap_or(tip_number);
    if from > to {
        eprintln!("Verify error: invalid block range {from}..={to}");
        return Err(ExitCode::Cli);
    }

    let mut timing_writer = match timing_csv {
        Some(path) => {
            let file = File::create(path).map_err(|err| {
                eprintln!("Verify error: create {}: {err}", path.display());
                ExitCode::Failure
            })?;
            let mut writer = BufWriter::new(file);
            writeln!(writer, "block,tx_count,txs_verify_ns,block_level_ns").map_err(|err| {
                eprintln!("Verify error: write {}: {err}", path.display());
                ExitCode::Failure
            })?;
            Some(writer)
        }
        None => None,
    };

    let context = VerifyContext::new(Arc::clone(&snapshot), Arc::new(shared.consensus().clone()));
    let txs_verify_cache = Arc::new(RwLock::new(init_cache()));
    let cell_provider = HistoricalCellProvider { store: &*snapshot };

    // Skip uncles: their double-inclusion check consults the DB's global uncle
    // index, which at tip already contains the uncles included by this very
    // block (and by later blocks).
    let switch = Switch::DISABLE_UNCLES;

    let blocks = to - from + 1;
    let mut tx_count: usize = 0;
    let mut cycles_total: u64 = 0;
    let mut resolve_elapsed = Duration::ZERO;
    let mut verify_elapsed = Duration::ZERO;
    let mut block_level_elapsed = Duration::ZERO;
    let mut txs_verify_elapsed = Duration::ZERO;

    for number in from..=to {
        let block_hash = snapshot.get_block_hash(number).ok_or_else(|| {
            eprintln!("Verify error: block {number} not found");
            ExitCode::Failure
        })?;
        let block = snapshot.get_block(&block_hash).ok_or_else(|| {
            eprintln!("Verify error: block body {number} not found");
            ExitCode::Failure
        })?;

        let block_cell_provider = BlockCellProvider::new(&block).map_err(|err| {
            eprintln!("Verify error: cell provider for block {number}: {err:?}");
            ExitCode::Failure
        })?;
        let overlay = OverlayCellProvider::new(&block_cell_provider, &cell_provider);

        let resolve_start = Instant::now();
        let mut seen_inputs = HashSet::new();
        let resolved = block
            .transactions()
            .iter()
            .cloned()
            .map(|tx| resolve_transaction(tx, &mut seen_inputs, &overlay, &*snapshot).map(Arc::new))
            .collect::<Result<Vec<Arc<ResolvedTransaction>>, _>>()
            .map_err(|err| {
                eprintln!("Verify error: resolve block {number}: {err:?}");
                ExitCode::Failure
            })?;
        resolve_elapsed += resolve_start.elapsed();
        tx_count += resolved.len().saturating_sub(1);

        let mmr = snapshot.chain_root_mmr(number.saturating_sub(1));
        let verifier = ContextualBlockVerifier::new(
            context.clone(),
            async_handle,
            switch,
            Arc::clone(&txs_verify_cache),
            &mmr,
        );

        let verify_start = Instant::now();
        let (cycles, _, timings) =
            verifier
                .verify_with_timing(&resolved, &block)
                .map_err(|err| {
                    eprintln!("Verify error: contextual verification of block {number}: {err:?}");
                    ExitCode::Failure
                })?;
        verify_elapsed += verify_start.elapsed();
        block_level_elapsed += timings.block_level;
        txs_verify_elapsed += timings.txs;
        cycles_total += cycles;

        if let Some(writer) = timing_writer.as_mut() {
            let block_tx_count = resolved.len().saturating_sub(1);
            writeln!(
                writer,
                "{number},{block_tx_count},{},{}",
                timings.txs.as_nanos(),
                timings.block_level.as_nanos()
            )
            .map_err(|err| {
                eprintln!("Verify error: write per-block timing: {err}");
                ExitCode::Failure
            })?;
        }
    }

    if let Some(mut writer) = timing_writer {
        writer.flush().map_err(|err| {
            eprintln!("Verify error: flush per-block timings: {err}");
            ExitCode::Failure
        })?;
    }

    let verify_secs = verify_elapsed.as_secs_f64();
    let blocks_per_sec = blocks as f64 / verify_secs.max(f64::EPSILON);
    let txs_per_sec = tx_count as f64 / verify_secs.max(f64::EPSILON);
    let txs_secs = txs_verify_elapsed.as_secs_f64();
    let txs_blocks_per_sec = blocks as f64 / txs_secs.max(f64::EPSILON);
    let txs_txs_per_sec = tx_count as f64 / txs_secs.max(f64::EPSILON);

    println!("verified blocks {from}..={to} ({blocks} blocks, {tx_count} txs)");
    println!("  resolve:           {:.3}s", resolve_elapsed.as_secs_f64());
    println!(
        "  block-level:       {:.3}s",
        block_level_elapsed.as_secs_f64()
    );
    println!(
        "  txs verify:        {:.3}s ({txs_blocks_per_sec:.2} blocks/s, {txs_txs_per_sec:.2} txs/s)",
        txs_secs
    );
    println!(
        "  contextual verify: {:.3}s ({blocks_per_sec:.2} blocks/s, {txs_per_sec:.2} txs/s)",
        verify_secs
    );
    println!(
        "  total:             {:.3}s",
        (resolve_elapsed + verify_elapsed).as_secs_f64()
    );
    println!("  total cycles:      {cycles_total}");
    println!(
        "  threads:           {}",
        std::env::var(VERIFY_THREADS_ENV).unwrap_or_else(|_| "all CPU cores".to_string())
    );

    Ok(())
}
