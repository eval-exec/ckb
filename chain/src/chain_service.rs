//! CKB chain service.
#![allow(missing_docs)]

use crate::consume_orphan::ConsumeOrphan;
use crate::{LonelyBlock, ProcessBlockRequest};
use ckb_channel::{select, Receiver};
use ckb_error::{Error, InternalErrorKind};
use ckb_logger::{self, debug, error, info, warn};
use ckb_shared::block_status::BlockStatus;
use ckb_shared::shared::Shared;
use ckb_stop_handler::new_crossbeam_exit_rx;
use ckb_types::core::{service::Request, BlockView};
use ckb_verification::{BlockVerifier, NonContextualBlockTxsVerifier};
use ckb_verification_traits::Verifier;

/// Chain background service to receive LonelyBlock and only do `non_contextual_verify`
pub(crate) struct ChainService {
    shared: Shared,

    process_block_rx: Receiver<ProcessBlockRequest>,

    consume_orphan: ConsumeOrphan,
}

impl ChainService {
    /// Create a new ChainService instance with shared.
    pub(crate) fn new(
        shared: Shared,
        process_block_rx: Receiver<ProcessBlockRequest>,
        consume_orphan: ConsumeOrphan,
    ) -> ChainService {
        ChainService {
            shared,
            process_block_rx,
            consume_orphan,
        }
    }

    /// Receive block from `process_block_rx` and do `non_contextual_verify`
    pub(crate) fn start_process_block(self) {
        let signal_receiver = new_crossbeam_exit_rx();

        loop {
            select! {
                recv(self.process_block_rx) -> msg => match msg {
                    Ok(Request { responder, arguments: lonely_block }) => {
                        // asynchronous_process_block doesn't interact with tx-pool,
                        // no need to pause tx-pool's chunk_process here.
                        let _trace_now = minstant::Instant::now();
                        self.asynchronous_process_block(lonely_block);
                        if let Some(handle) = ckb_metrics::handle(){
                            handle.ckb_chain_async_process_block_duration.observe(_trace_now.elapsed().as_secs_f64())
                        }
                        let _ = responder.send(());
                    },
                    _ => {
                        error!("process_block_receiver closed");
                        break;
                    },
                },
                recv(signal_receiver) -> _ => {
                    info!("ChainService received exit signal, exit now");
                    break;
                }
            }
        }
    }

    fn non_contextual_verify(&self, block: &BlockView) -> Result<(), Error> {
        let consensus = self.shared.consensus();
        BlockVerifier::new(consensus).verify(block).map_err(|e| {
            debug!("[process_block] BlockVerifier error {:?}", e);
            e
        })?;

        NonContextualBlockTxsVerifier::new(consensus)
            .verify(block)
            .map_err(|e| {
                debug!(
                    "[process_block] NonContextualBlockTxsVerifier error {:?}",
                    e
                );
                e
            })
            .map(|_| ())
    }

    // `self.non_contextual_verify` is very fast.
    fn asynchronous_process_block(&self, lonely_block: LonelyBlock) {
        let block_number = lonely_block.block().number();
        let block_hash = lonely_block.block().hash();
        // Skip verifying a genesis block if its hash is equal to our genesis hash,
        // otherwise, return error and ban peer.
        if block_number < 1 {
            if self.shared.genesis_hash() != block_hash {
                warn!(
                    "receive 0 number block: 0-{}, expect genesis hash: {}",
                    block_hash,
                    self.shared.genesis_hash()
                );
                self.shared
                    .insert_block_status(lonely_block.block().hash(), BlockStatus::BLOCK_INVALID);
                let error = InternalErrorKind::System
                    .other("Invalid genesis block received")
                    .into();
                lonely_block.execute_callback(Err(error));
            } else {
                warn!("receive 0 number block: 0-{}", block_hash);
                lonely_block.execute_callback(Ok(false));
            }
            return;
        }

        if lonely_block.switch().is_none()
            || matches!(lonely_block.switch(), Some(switch) if !switch.disable_non_contextual())
        {
            let result = self.non_contextual_verify(lonely_block.block());
            if let Err(err) = result {
                error!(
                    "block {}-{} verify failed: {:?}",
                    block_number, block_hash, err
                );
                self.shared
                    .insert_block_status(lonely_block.block().hash(), BlockStatus::BLOCK_INVALID);
                lonely_block.execute_callback(Err(err));
                return;
            }
        }
        self.consume_orphan.process_lonely_block(lonely_block);

        debug!(
            "processing block: {}-{}, (tip:unverified_tip):({}:{})",
            block_number,
            block_hash,
            self.shared.snapshot().tip_number(),
            self.shared.get_unverified_tip().number(),
        );
    }
}
