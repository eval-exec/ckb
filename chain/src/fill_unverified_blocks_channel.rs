use crate::{LonelyBlockHash, UnverifiedBlock};
use ckb_channel::{Receiver, Sender};
use ckb_logger::{debug, error, info};
use ckb_shared::Shared;
use ckb_store::ChainStore;
use crossbeam::select;
use std::sync::Arc;

pub(crate) struct FillUnverifiedBlocksChannel {
    shared: Shared,
    fill_unverified_rx: Receiver<LonelyBlockHash>,

    unverified_block_tx: Sender<UnverifiedBlock>,

    stop_rx: Receiver<()>,
}

impl FillUnverifiedBlocksChannel {
    pub(crate) fn new(
        shared: Shared,
        fill_unverified_rx: Receiver<LonelyBlockHash>,
        unverified_block_tx: Sender<UnverifiedBlock>,
        stop_rx: Receiver<()>,
    ) -> Self {
        FillUnverifiedBlocksChannel {
            shared,
            fill_unverified_rx,
            unverified_block_tx,
            stop_rx,
        }
    }

    pub(crate) fn start(&self) {
        loop {
            select! {
                recv(self.fill_unverified_rx) -> msg => match msg {
                    Ok(fill_unverified_block_task) =>{
                        self.fill_unverified_channel(fill_unverified_block_task);
                    },
                    Err(err) =>{
                        error!("recv fill_task_rx failed, err: {:?}", err);
                    }
                },
                recv(self.stop_rx) -> _ => {
                    info!("fill_unverified_blocks thread received exit signal, exit now");
                    break;
                }
            }
        }
    }

    fn fill_unverified_channel(&self, task: LonelyBlockHash) {
        let block_number = task.block_number_and_hash.number();
        let block_hash = task.block_number_and_hash.hash();
        let unverified_block: UnverifiedBlock = self.load_full_unverified_block_by_hash(task);

        ckb_metrics::handle().map(|metrics| {
            metrics
                .ckb_chain_unverified_block_ch_len
                .set(self.unverified_block_tx.len() as i64)
        });

        if let Err(_) = self.unverified_block_tx.send(unverified_block) {
            error!(
                "send unverified_block to unverified_block_tx failed, the receiver has been closed"
            );
        } else {
            debug!("fill unverified block {}-{}", block_number, block_hash,);
        }
    }

    fn load_full_unverified_block_by_hash(&self, task: LonelyBlockHash) -> UnverifiedBlock {
        let _trace_timecost = ckb_metrics::handle()
            .map(|metrics| metrics.ckb_chain_load_full_unverified_block.start_timer());

        let LonelyBlockHash {
            block_number_and_hash,
            switch,
            verify_callback,
        } = task;

        let block_view = self
            .shared
            .store()
            .get_block(&block_number_and_hash.hash())
            .expect("block stored");
        let block = Arc::new(block_view);
        let parent_header = {
            let _trace_timecost = ckb_metrics::handle().map(|metrics| {
                metrics
                    .ckb_chain_load_full_unverified_block_header
                    .start_timer()
            });

            self.shared
                .store()
                .get_block_header(&block.data().header().raw().parent_hash())
                .expect("parent header stored")
        };

        UnverifiedBlock {
            block,
            switch,
            verify_callback,
            parent_header,
        }
    }
}
