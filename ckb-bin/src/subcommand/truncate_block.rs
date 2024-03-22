use ckb_app_config::{ExitCode, TruncateBlockArgs};
use ckb_async_runtime::Handle;
use ckb_chain::consume_unverified::ConsumeUnverifiedBlockProcessor;
use ckb_logger::info;
use ckb_shared::{ChainServicesBuilder, SharedBuilder};
use ckb_store::ChainStore;
use ckb_types::core::BlockNumber;

pub fn truncate_block(args: TruncateBlockArgs, async_handle: Handle) -> Result<(), ExitCode> {
    let truncate_block = TruncateBlock::build(args, async_handle)?;
    truncate_block.truncate_blocks()
}

struct TruncateBlock {
    chain_service_builder: ChainServicesBuilder,
    target: BlockNumber,
}

impl TruncateBlock {
    pub fn build(args: TruncateBlockArgs, async_handle: Handle) -> Result<Self, ExitCode> {
        let shared_builder = SharedBuilder::new(
            &args.config.bin_name,
            args.config.root_dir.as_path(),
            &args.config.db,
            None,
            async_handle,
            args.consensus,
        )?;

        if args.target.is_none() {
            eprintln!("--target is required");
            return Err(ExitCode::Cli);
        }

        let target = args.target.expect("expect a target param");
        let chain_service_builder = shared_builder.build()?.1.take_chain_services_builder();

        Ok(TruncateBlock {
            chain_service_builder,
            target,
        })
    }

    pub fn truncate_blocks(self) -> Result<(), ExitCode> {
        let ChainServicesBuilder {
            shared,
            proposal_table,
        } = self.chain_service_builder;

        let target_tip_hash = shared
            .snapshot()
            .get_block_hash(self.target)
            .ok_or(ExitCode::IO)?;
        let current_tip = shared
            .snapshot()
            .get_tip_header()
            .ok_or(ExitCode::Failure)?;
        info!(
            "current tip: {}-{}",
            current_tip.number(),
            current_tip.hash()
        );
        info!("truncating to: {}-{}", self.target, target_tip_hash);

        let mut processor = ConsumeUnverifiedBlockProcessor {
            shared: shared.clone(),
            proposal_table,
        };
        while shared.snapshot().tip_hash() != target_tip_hash {
            processor.truncate_tip().map_err(|err| {
                eprintln!("truncate block error: {:?}", err);
                ExitCode::Failure
            })?;
            let current_tip = shared
                .snapshot()
                .get_tip_header()
                .ok_or(ExitCode::Failure)?;
            info!(
                "truncated to tip: {}-{}",
                current_tip.number(),
                current_tip.hash()
            );
        }
        Ok(())
    }
}
