#![allow(missing_docs)]

use crate::utils::orphan_block_pool::OrphanBlockPool;
use crate::{LonelyBlock, LonelyBlockHash};
use ckb_channel::Sender;
use ckb_error::Error;
use ckb_logger::internal::trace;
use ckb_logger::{debug, error, info};
use ckb_shared::block_status::BlockStatus;
use ckb_shared::Shared;
use ckb_store::ChainStore;
use ckb_systemtime::unix_time_as_millis;
use ckb_types::core::{BlockExt, BlockView, HeaderView};
use ckb_types::U256;
use ckb_verification::InvalidParentError;
use std::sync::Arc;

pub(crate) struct ConsumeOrphan {
    shared: Shared,

    orphan_blocks_broker: Arc<OrphanBlockPool>,
    fill_unverified_tx: Sender<LonelyBlockHash>,
}

impl ConsumeOrphan {
    pub(crate) fn new(
        shared: Shared,
        orphan_block_pool: Arc<OrphanBlockPool>,
        fill_unverified_tx: Sender<LonelyBlockHash>,
    ) -> ConsumeOrphan {
        ConsumeOrphan {
            shared: shared.clone(),
            orphan_blocks_broker: orphan_block_pool,
            fill_unverified_tx,
        }
    }

    fn search_orphan_pool(&self) {
        for leader_hash in self.orphan_blocks_broker.clone_leaders() {
            if !self.shared.contains_block_status(
                self.shared.store(),
                &leader_hash,
                BlockStatus::BLOCK_STORED,
            ) {
                trace!("orphan leader: {} not stored", leader_hash);
                continue;
            }

            let descendants: Vec<LonelyBlockHash> = self
                .orphan_blocks_broker
                .remove_blocks_by_parent(&leader_hash);
            if descendants.is_empty() {
                error!(
                    "leader {} does not have any descendants, this shouldn't happen",
                    leader_hash
                );
                continue;
            }
            self.accept_descendants(descendants);
        }
    }

    pub(crate) fn process_lonely_block(&self, lonely_block: LonelyBlockHash) {
        let parent_hash = lonely_block.block().parent_hash();
        let parent_status = self
            .shared
            .get_block_status(self.shared.store(), &parent_hash);
        if parent_status.contains(BlockStatus::BLOCK_STORED) {
            debug!(
                "parent {} has stored: {:?}, processing descendant directly {}-{}",
                parent_hash,
                parent_status,
                lonely_block.block().number(),
                lonely_block.block().hash()
            );
            self.process_descendant(lonely_block);
        } else if parent_status.eq(&BlockStatus::BLOCK_INVALID) {
            // ignore this block, because parent block is invalid
            info!(
                "parent: {} is INVALID, ignore this block {}-{}",
                parent_hash,
                lonely_block.block().number(),
                lonely_block.block().hash()
            );
        } else {
            self.orphan_blocks_broker.insert(lonely_block);
        }
        self.search_orphan_pool();

        ckb_metrics::handle().map(|handle| {
            handle
                .ckb_chain_orphan_count
                .set(self.orphan_blocks_broker.len() as i64)
        });
    }
    fn send_unverified_block(&self, lonely_block: LonelyBlockHash) {
        let block_number = lonely_block.block_number_and_hash.number();
        let block_hash = lonely_block.block_number_and_hash.hash();
        if let Some(metrics) = ckb_metrics::handle() {
            metrics
                .ckb_chain_fill_unverified_block_ch_len
                .set(self.fill_unverified_tx.len() as i64)
        }

        match self.fill_unverified_tx.send(lonely_block) {
            Ok(_) => {
                debug!(
                    "process desendant block success {}-{}",
                    block_number, block_hash
                );
            }
            Err(_) => {
                error!("send unverified_block_tx failed, the receiver has been closed");
                return;
            }
        };
        if lonely_block.block_number_and_hash.number() > self.shared.snapshot().tip_number() {
            self.shared.set_unverified_tip(ckb_shared::HeaderIndex::new(
                block_number,
                block_hash.clone(),
                0.into(),
            ));
            self.shared
                .get_unverified_index()
                .insert(block_number, block_hash.clone());

            if let Some(handle) = ckb_metrics::handle() {
                handle.ckb_chain_unverified_tip.set(block_number as i64);
            }
            debug!(
                "set unverified_tip to {}-{}, while unverified_tip - verified_tip = {}",
                block_number.clone(),
                block_hash.clone(),
                block_number.saturating_sub(self.shared.snapshot().tip_number())
            )
        }
    }

    pub(crate) fn process_descendant(&self, lonely_block: LonelyBlockHash) {
        self.shared
            .insert_block_status(lonely_block.block().hash(), BlockStatus::BLOCK_STORED);
        self.shared.remove_header_view(&lonely_block.block().hash());

        self.send_unverified_block(lonely_block)
    }

    fn accept_descendants(&self, descendants: Vec<LonelyBlockHash>) {
        for descendant_block in descendants {
            self.process_descendant(descendant_block);
        }
    }
}
