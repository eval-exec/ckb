use ckb_app_config::StoreConfig;
use ckb_db::{Direction, IteratorMode, RocksDB};
use ckb_db_migration::{Migration, ProgressBar, ProgressStyle};
use ckb_db_schema::{
    COLUMN_BLOCK_BODY, COLUMN_BLOCK_EPOCH, COLUMN_BLOCK_EXT, COLUMN_BLOCK_EXTENSION,
    COLUMN_BLOCK_FILTER, COLUMN_BLOCK_FILTER_HASH, COLUMN_BLOCK_HEADER, COLUMN_BLOCK_PROPOSAL_IDS,
    COLUMN_BLOCK_UNCLE, COLUMN_HASH_INDEX, COLUMN_INDEX, COLUMN_META, COLUMN_NUMBER_HASH, Col,
    META_TIP_HEADER_KEY,
};
use ckb_error::{Error, InternalErrorKind};
use ckb_store::{ChainDB, ChainStore, StoreWriteBatch};
use ckb_types::{
    core::BlockNumber,
    packed,
    prelude::{Builder, Entity, FromSliceShouldBeOk, Reader},
};
use std::cmp;
use std::sync::Arc;

pub struct RefactorRocksdbSchema;

const VERSION: &str = "20260304120000";
const MAX_THREAD: u64 = 6;
const MIN_THREAD: u64 = 2;
const BATCH: usize = 1_000;

fn data_corrupted(message: String) -> Error {
    InternalErrorKind::DataCorrupted.other(message).into()
}

fn scan_max_number_from_index(chain_db: &ChainDB) -> Option<BlockNumber> {
    chain_db
        .get_iter(COLUMN_INDEX, IteratorMode::Start)
        .filter_map(|(key, _)| {
            if key.len() == 8 {
                Some(packed::Uint64Reader::from_slice_should_be_ok(key.as_ref()).into())
            } else {
                None
            }
        })
        .max()
}

fn scan_max_number_from_number_hash(chain_db: &ChainDB) -> Option<BlockNumber> {
    chain_db
        .get_iter(COLUMN_NUMBER_HASH, IteratorMode::Start)
        .map(|(key, _)| {
            let reader = packed::NumberHashReader::from_slice_should_be_ok(key.as_ref());
            reader.number().into()
        })
        .max()
}

fn scan_max_number_from_block_header(chain_db: &ChainDB) -> Option<BlockNumber> {
    let mut max_number = None;
    let mut block_key_count = 0u64;
    let mut old_hash_key_count = 0u64;
    for (key, _) in chain_db.get_iter(COLUMN_BLOCK_HEADER, IteratorMode::Start) {
        match key.len() {
            40 => {
                block_key_count += 1;
                if let Some(number) = packed::Byte32::block_number_from_key(key.as_ref()) {
                    max_number = Some(max_number.map_or(number, |old| cmp::max(old, number)));
                }
            }
            32 => {
                old_hash_key_count += 1;
            }
            _ => {}
        }
    }
    eprintln!(
        "[refactor-migration] scan COLUMN_BLOCK_HEADER: block_key_count={block_key_count}, old_hash_key_count={old_hash_key_count}, max_number={max_number:?}"
    );
    max_number
}

fn get_tip_number(chain_db: &ChainDB) -> Result<BlockNumber, Error> {
    let tip_hash_raw = chain_db
        .get(COLUMN_META, META_TIP_HEADER_KEY)
        .ok_or_else(|| data_corrupted("missing META_TIP_HEADER_KEY".to_owned()))?;
    let tip_hash = packed::Byte32Reader::from_slice_should_be_ok(tip_hash_raw.as_ref()).to_entity();
    eprintln!("[refactor-migration] META_TIP_HEADER_KEY={tip_hash}");

    let mut tip_number = None;

    // Old schema path: COLUMN_INDEX stores hash -> number.
    if let Some(raw) = chain_db.get(COLUMN_INDEX, tip_hash.as_slice()) {
        let number: BlockNumber =
            packed::Uint64Reader::from_slice_should_be_ok(raw.as_ref()).into();
        eprintln!("[refactor-migration] tip number from COLUMN_INDEX(hash->number): {number}");
        tip_number = Some(number);
    } else {
        eprintln!("[refactor-migration] no COLUMN_INDEX(hash->number) for tip hash");
    }

    // New schema path: COLUMN_HASH_INDEX stores hash -> [number + is_main_chain].
    if let Some(raw) = chain_db.get(COLUMN_HASH_INDEX, tip_hash.as_slice())
        && let Some(number) = packed::Byte32::number_from_index_value(raw.as_ref())
    {
        let is_main_chain = packed::Byte32::is_main_chain_from_index_value(raw.as_ref());
        eprintln!(
            "[refactor-migration] tip number from COLUMN_HASH_INDEX: {number}, is_main_chain={is_main_chain:?}"
        );
        tip_number = Some(tip_number.map_or(number, |old| cmp::max(old, number)));
    } else {
        eprintln!("[refactor-migration] no valid COLUMN_HASH_INDEX for tip hash");
    }

    // Fallback for very old DB states: resolve tip number from old header payload.
    if let Some(raw_header) = chain_db.get(COLUMN_BLOCK_HEADER, tip_hash.as_slice()) {
        let header_reader = packed::HeaderViewReader::from_slice_should_be_ok(raw_header.as_ref());
        let number: BlockNumber = header_reader.data().raw().number().into();
        eprintln!("[refactor-migration] tip number from old COLUMN_BLOCK_HEADER(hash): {number}");
        tip_number = Some(tip_number.map_or(number, |old| cmp::max(old, number)));
    } else {
        eprintln!("[refactor-migration] no old COLUMN_BLOCK_HEADER(hash) for tip hash");
    }

    if let Some(number) = tip_number
        && number > 0
    {
        eprintln!("[refactor-migration] resolved tip number directly: {number}");
        return Ok(number);
    }

    // When tip resolves to genesis (or cannot be resolved from tip hash),
    // cross-check by scanning height indices. This prevents false 0/1 migrations
    // when META_TIP_HEADER_KEY is stale or hash->number mappings are absent.
    let mut scanned_max = 0;
    let scanned_max_index = scan_max_number_from_index(chain_db);
    let scanned_max_number_hash = scan_max_number_from_number_hash(chain_db);
    let scanned_max_block_header = scan_max_number_from_block_header(chain_db);
    eprintln!(
        "[refactor-migration] scanned max from COLUMN_INDEX(number->hash): {scanned_max_index:?}, COLUMN_NUMBER_HASH: {scanned_max_number_hash:?}, COLUMN_BLOCK_HEADER(block_key): {scanned_max_block_header:?}"
    );
    if let Some(number) = scanned_max_index {
        scanned_max = cmp::max(scanned_max, number);
    }
    if let Some(number) = scanned_max_number_hash {
        scanned_max = cmp::max(scanned_max, number);
    }
    if let Some(number) = scanned_max_block_header {
        scanned_max = cmp::max(scanned_max, number);
    }
    if scanned_max > 0 {
        eprintln!("[refactor-migration] resolved tip number from scan fallback: {scanned_max}");
        return Ok(scanned_max);
    }

    if let Some(number) = tip_number {
        eprintln!("[refactor-migration] fallback to resolved tip number: {number}");
        return Ok(number);
    }

    Err(data_corrupted(format!(
        "cannot resolve tip number from tip hash {}",
        tip_hash
    )))
}

fn move_required_block_column(
    chain_db: &ChainDB,
    wb: &mut StoreWriteBatch,
    col: Col,
    column_name: &str,
    block_hash: &packed::Byte32,
    block_key: &[u8],
) -> Result<(), Error> {
    if let Some(raw) = chain_db.get(col, block_hash.as_slice()) {
        wb.put(col, block_key, raw.as_ref())?;
        wb.delete(col, block_hash.as_slice())?;
        return Ok(());
    }

    if chain_db.get(col, block_key).is_some() {
        return Ok(());
    }

    Err(data_corrupted(format!(
        "missing {column_name} for block {}",
        block_hash
    )))
}

fn move_conditionally_required_block_column(
    chain_db: &ChainDB,
    wb: &mut StoreWriteBatch,
    col: Col,
    column_name: &str,
    block_hash: &packed::Byte32,
    block_key: &[u8],
    allow_missing: bool,
) -> Result<(), Error> {
    if let Some(raw) = chain_db.get(col, block_hash.as_slice()) {
        wb.put(col, block_key, raw.as_ref())?;
        wb.delete(col, block_hash.as_slice())?;
        return Ok(());
    }

    if chain_db.get(col, block_key).is_some() || allow_missing {
        return Ok(());
    }

    Err(data_corrupted(format!(
        "missing {column_name} for block {}",
        block_hash
    )))
}

fn move_optional_block_column(
    chain_db: &ChainDB,
    wb: &mut StoreWriteBatch,
    col: Col,
    block_hash: &packed::Byte32,
    block_key: &[u8],
) -> Result<(), Error> {
    if let Some(raw) = chain_db.get(col, block_hash.as_slice()) {
        wb.put(col, block_key, raw.as_ref())?;
        wb.delete(col, block_hash.as_slice())?;
    }
    Ok(())
}

fn move_block_body(
    chain_db: &ChainDB,
    wb: &mut StoreWriteBatch,
    block_hash: &packed::Byte32,
    number: BlockNumber,
    txs_len: u32,
) -> Result<(), Error> {
    for index in 0..txs_len {
        let old_tx_key = packed::TransactionKey::new_builder()
            .block_hash(block_hash.clone())
            .index(index)
            .build();
        let new_tx_key = block_hash.to_tx_key(number, index);

        if let Some(raw) = chain_db.get(COLUMN_BLOCK_BODY, old_tx_key.as_slice()) {
            wb.put(COLUMN_BLOCK_BODY, &new_tx_key, raw.as_ref())?;
            wb.delete(COLUMN_BLOCK_BODY, old_tx_key.as_slice())?;
            continue;
        }

        if chain_db.get(COLUMN_BLOCK_BODY, &new_tx_key).is_none() {
            return Err(data_corrupted(format!(
                "missing tx index {} for block {}",
                index, block_hash
            )));
        }
    }

    Ok(())
}

fn migrate_one_block(
    chain_db: &ChainDB,
    wb: &mut StoreWriteBatch,
    number: BlockNumber,
    block_hash: &packed::Byte32,
    txs_len: u32,
) -> Result<(), Error> {
    let block_key = block_hash.to_block_key(number);
    let likely_frozen = number > 0 && txs_len == 0;

    move_required_block_column(
        chain_db,
        wb,
        COLUMN_BLOCK_HEADER,
        "COLUMN_BLOCK_HEADER",
        block_hash,
        &block_key,
    )?;
    move_conditionally_required_block_column(
        chain_db,
        wb,
        COLUMN_BLOCK_UNCLE,
        "COLUMN_BLOCK_UNCLE",
        block_hash,
        &block_key,
        likely_frozen,
    )?;
    move_conditionally_required_block_column(
        chain_db,
        wb,
        COLUMN_BLOCK_PROPOSAL_IDS,
        "COLUMN_BLOCK_PROPOSAL_IDS",
        block_hash,
        &block_key,
        likely_frozen,
    )?;
    move_optional_block_column(chain_db, wb, COLUMN_BLOCK_EXTENSION, block_hash, &block_key)?;
    move_optional_block_column(chain_db, wb, COLUMN_BLOCK_EXT, block_hash, &block_key)?;
    move_optional_block_column(chain_db, wb, COLUMN_BLOCK_EPOCH, block_hash, &block_key)?;
    move_optional_block_column(chain_db, wb, COLUMN_BLOCK_FILTER, block_hash, &block_key)?;
    move_optional_block_column(
        chain_db,
        wb,
        COLUMN_BLOCK_FILTER_HASH,
        block_hash,
        &block_key,
    )?;
    move_block_body(chain_db, wb, block_hash, number, txs_len)?;

    let number_packed: packed::Uint64 = number.into();
    let is_main_chain = chain_db
        .get(COLUMN_INDEX, number_packed.as_slice())
        .is_some_and(|raw| raw.as_ref() == block_hash.as_slice());

    let hash_index_value = packed::Byte32::to_index_value(number, is_main_chain);
    wb.put(COLUMN_HASH_INDEX, block_hash.as_slice(), &hash_index_value)?;

    // Old schema stores canonical hash->number in COLUMN_INDEX; remove it.
    if chain_db.get(COLUMN_INDEX, block_hash.as_slice()).is_some() {
        wb.delete(COLUMN_INDEX, block_hash.as_slice())?;
    }

    Ok(())
}

fn collect_blocks_of_number(
    chain_db: &ChainDB,
    number: BlockNumber,
) -> Result<Vec<(packed::Byte32, u32)>, Error> {
    let number_packed: packed::Uint64 = number.into();
    let prefix = number_packed.as_slice();

    let mut blocks: Vec<(packed::Byte32, u32)> = chain_db
        .get_iter(
            COLUMN_NUMBER_HASH,
            IteratorMode::From(prefix, Direction::Forward),
        )
        .take_while(|(key, _)| key.starts_with(prefix))
        .map(|(key, value)| {
            let reader = packed::NumberHashReader::from_slice_should_be_ok(key.as_ref());
            let key_number: BlockNumber = reader.number().into();
            if key_number != number {
                return Err(data_corrupted(format!(
                    "number mismatch in COLUMN_NUMBER_HASH, expected {}, got {}",
                    number, key_number
                )));
            }

            let txs_len: u32 = packed::Uint32Reader::from_slice_should_be_ok(value.as_ref()).into();
            Ok((reader.block_hash().to_entity(), txs_len))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // Fallback for very old DB states where COLUMN_NUMBER_HASH does not exist.
    if blocks.is_empty()
        && let Some(raw_hash) = chain_db.get(COLUMN_INDEX, number_packed.as_slice())
    {
        let block_hash =
            packed::Byte32Reader::from_slice_should_be_ok(raw_hash.as_ref()).to_entity();
        let txs_len = chain_db
            .get_iter(
                COLUMN_BLOCK_BODY,
                IteratorMode::From(block_hash.as_slice(), Direction::Forward),
            )
            .take_while(|(key, _)| key.starts_with(block_hash.as_slice()))
            .count() as u32;
        blocks.push((block_hash, txs_len));
    }

    Ok(blocks)
}

impl Migration for RefactorRocksdbSchema {
    fn migrate(
        &self,
        db: RocksDB,
        pb: Arc<dyn Fn(u64) -> ProgressBar + Send + Sync>,
    ) -> Result<RocksDB, Error> {
        let chain_db = ChainDB::new(db, StoreConfig::default());
        let tip_number = get_tip_number(&chain_db)?;

        let worker_count = cmp::min(cmp::max(MIN_THREAD, num_cpus::get() as u64), MAX_THREAD);
        let total_numbers = tip_number + 1;
        let chunk_size = total_numbers / worker_count;
        let remainder = total_numbers % worker_count;
        eprintln!(
            "[refactor-migration] plan: tip_number={tip_number}, total_numbers={total_numbers}, worker_count={worker_count}, chunk_size={chunk_size}, remainder={remainder}"
        );

        let handles: Vec<_> = (0..worker_count)
            .map(|i| {
                let chain_db = chain_db.clone();
                let pb = Arc::clone(&pb);
                let start = i * chunk_size + cmp::min(i, remainder);
                let len = chunk_size + u64::from(i < remainder);
                let end = start + len;
                eprintln!(
                    "[refactor-migration] worker#{i} range=[{start}, {end}) len={len}"
                );

                let pbi = pb(len);
                pbi.set_style(
                    ProgressStyle::default_bar()
                        .template(
                            "{prefix:.bold.dim} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
                        )
                        .expect("Failed to set progress bar template")
                        .progress_chars("#>-"),
                );
                pbi.set_position(0);
                pbi.enable_steady_tick(std::time::Duration::from_millis(5000));

                std::thread::spawn(move || -> Result<(), Error> {
                    eprintln!(
                        "[refactor-migration] worker#{i} started range=[{start}, {end})"
                    );
                    let mut wb = chain_db.new_write_batch();

                    for number in start..end {
                        let blocks = collect_blocks_of_number(&chain_db, number)?;
                        for (block_hash, txs_len) in blocks {
                            migrate_one_block(&chain_db, &mut wb, number, &block_hash, txs_len)?;
                        }

                        if wb.len() > BATCH {
                            chain_db.write(&wb)?;
                            wb.clear()?;
                        }

                        pbi.inc(1);
                    }

                    if !wb.is_empty() {
                        chain_db.write(&wb)?;
                    }
                    pbi.finish_with_message("done");
                    eprintln!(
                        "[refactor-migration] worker#{i} finished range=[{start}, {end})"
                    );
                    Ok(())
                })
            })
            .collect();

        for handle in handles {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => {
                    return Err(InternalErrorKind::Database
                        .other("schema migration worker panicked")
                        .into());
                }
            }
        }

        Ok(chain_db.into_inner())
    }

    fn version(&self) -> &str {
        VERSION
    }
}
