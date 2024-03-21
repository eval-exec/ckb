#![allow(missing_docs)]

use crate::utils::orphan_block_pool::OrphanBlockPool;
use crate::LonelyBlockHash;
use ckb_channel::{select, Receiver, Sender};
use ckb_error::Error;
use ckb_logger::internal::trace;
use ckb_logger::{debug, error, info};
use ckb_shared::block_status::BlockStatus;
use ckb_shared::Shared;
use ckb_store::ChainStore;
use ckb_systemtime::unix_time_as_millis;
use ckb_types::core::{BlockExt, EpochNumber, HeaderView};
use ckb_types::U256;
use ckb_verification::InvalidParentError;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

pub(crate) struct ConsumeDescendantProcessor {
    pub shared: Shared,
    pub unverified_blocks_tx: Sender<LonelyBlockHash>,
}

// Store the an unverified block to the database. We may usually do this
// for an orphan block with unknown parent. But this function is also useful in testing.
pub fn store_unverified_block(
    shared: &Shared,
    block: &LonelyBlockHash,
) -> Result<(HeaderView, U256), Error> {
    let (block_number, block_hash) = (block.number(), block.hash());

    let parent_header = shared
        .store()
        .get_block_header(&block.parent_hash())
        .expect("parent already store");

    if let Some(ext) = shared.store().get_block_ext(&block.hash()) {
        debug!(
            "block {}-{} has stored BlockExt: {:?}",
            block_number, block_hash, ext
        );
        match ext.verified {
            Some(true) => {
                return Ok((parent_header, ext.total_difficulty));
            }
            Some(false) => {
                return Err(InvalidParentError {
                    parent_hash: parent_header.hash(),
                }
                .into());
            }
            None => {
                // continue to process
            }
        }
    }

    trace!("begin accept block: {}-{}", block.number(), block.hash());

    let parent_ext = shared
        .store()
        .get_block_ext(&block.parent_hash())
        .expect("parent already store");

    if parent_ext.verified == Some(false) {
        return Err(InvalidParentError {
            parent_hash: parent_header.hash(),
        }
        .into());
    }

    let cannon_total_difficulty = parent_ext.total_difficulty.to_owned() + block.difficulty();

    let db_txn = Arc::new(shared.store().begin_transaction());

    let next_block_epoch = shared
        .consensus()
        .next_epoch_ext(&parent_header, &db_txn.borrow_as_data_loader())
        .expect("epoch should be stored");
    let new_epoch = next_block_epoch.is_head();
    let epoch = next_block_epoch.epoch();

    db_txn.insert_block_epoch_index(&block.hash(), &epoch.last_block_hash_in_previous_epoch())?;
    if new_epoch {
        db_txn.insert_epoch_ext(&epoch.last_block_hash_in_previous_epoch(), &epoch)?;
    }

    let ext = BlockExt {
        received_at: unix_time_as_millis(),
        total_difficulty: cannon_total_difficulty.clone(),
        total_uncles_count: parent_ext.total_uncles_count + block.uncles_len() as u64,
        verified: None,
        txs_fees: vec![],
        cycles: None,
        txs_sizes: None,
    };

    db_txn.insert_block_ext(&block.hash(), &ext)?;

    db_txn.commit()?;

    Ok((parent_header, cannon_total_difficulty))
}

impl ConsumeDescendantProcessor {
    fn send_unverified_block(&self, lonely_block: LonelyBlockHash, total_difficulty: U256) {
        let block_number = lonely_block.block_number_and_hash.number();
        let block_hash = lonely_block.block_number_and_hash.hash();
        if let Some(metrics) = ckb_metrics::handle() {
            metrics
                .ckb_chain_unverified_block_ch_len
                .set(self.unverified_blocks_tx.len() as i64)
        };

        match self.unverified_blocks_tx.send(lonely_block) {
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

        if total_difficulty.gt(self.shared.get_unverified_tip().total_difficulty()) {
            self.shared.set_unverified_tip(ckb_shared::HeaderIndex::new(
                block_number,
                block_hash.clone(),
                total_difficulty,
            ));
            if let Some(handle) = ckb_metrics::handle() {
                handle.ckb_chain_unverified_tip.set(block_number as i64);
            }
            debug!(
                "set unverified_tip to {}-{}, while unverified_tip - verified_tip = {}",
                block_number.clone(),
                block_hash.clone(),
                block_number.saturating_sub(self.shared.snapshot().tip_number())
            )
        } else {
            debug!(
                "received a block {}-{} with lower or equal difficulty than unverified_tip {}-{}",
                block_number,
                block_hash,
                self.shared.get_unverified_tip().number(),
                self.shared.get_unverified_tip().hash(),
            );
        }
    }

    pub(crate) fn process_descendant(&self, lonely_block: LonelyBlockHash) -> Result<(), Error> {
        return match store_unverified_block(&self.shared, &lonely_block) {
            Ok((_parent_header, total_difficulty)) => {
                self.shared
                    .insert_block_status(lonely_block.hash(), BlockStatus::BLOCK_STORED);

                self.send_unverified_block(lonely_block, total_difficulty);
                Ok(())
            }

            Err(err) => {
                if let Some(_invalid_parent_err) = err.downcast_ref::<InvalidParentError>() {
                    self.shared
                        .block_status_map()
                        .insert(lonely_block.hash(), BlockStatus::BLOCK_INVALID);
                }

                lonely_block.execute_callback(Err(err.clone()));
                Err(err)
            }
        };
    }

    fn accept_descendants(&self, descendants: Vec<LonelyBlockHash>) {
        let mut has_parent_invalid_error = false;
        for descendant_block in descendants {
            let block_number = descendant_block.block_number_and_hash.number();
            let block_hash = descendant_block.block_number_and_hash.hash();

            if has_parent_invalid_error {
                self.shared
                    .block_status_map()
                    .insert(block_hash.clone(), BlockStatus::BLOCK_INVALID);
                let err = Err(InvalidParentError {
                    parent_hash: descendant_block.parent_hash(),
                }
                .into());

                error!(
                    "process descendant {}-{}, failed {:?}",
                    block_number,
                    block_hash.clone(),
                    err
                );

                descendant_block.execute_callback(err);
                continue;
            }

            if let Err(err) = self.process_descendant(descendant_block) {
                error!(
                    "process descendant {}-{}, failed {:?}",
                    block_number, block_hash, err
                );

                if let Some(_invalid_parent_err) = err.downcast_ref::<InvalidParentError>() {
                    has_parent_invalid_error = true;
                }
            }
        }
    }
}

pub(crate) struct ConsumeOrphan {
    shared: Shared,

    descendant_processor: ConsumeDescendantProcessor,

    orphan_blocks_broker: Arc<OrphanBlockPool>,
    lonely_blocks_rx: Receiver<LonelyBlockHash>,

    stop_rx: Receiver<()>,
}

impl ConsumeOrphan {
    pub(crate) fn new(
        shared: Shared,
        orphan_block_pool: Arc<OrphanBlockPool>,
        unverified_blocks_tx: Sender<LonelyBlockHash>,
        lonely_blocks_rx: Receiver<LonelyBlockHash>,
        stop_rx: Receiver<()>,
    ) -> ConsumeOrphan {
        ConsumeOrphan {
            shared: shared.clone(),
            descendant_processor: ConsumeDescendantProcessor {
                shared,
                unverified_blocks_tx,
            },
            orphan_blocks_broker: orphan_block_pool,
            lonely_blocks_rx,
            stop_rx,
        }
    }

    pub(crate) fn start(&self) {
        let mut last_check_expired_orphans_epoch: EpochNumber = 0;
        loop {
            select! {
                recv(self.lonely_blocks_rx) -> msg => match msg {
                    Ok(lonely_block) => {
                        let lonely_block_epoch_number: EpochNumber = lonely_block.epoch_number();

                        let _trace_now = minstant::Instant::now();
                        self.process_lonely_block(lonely_block);
                        if let Some(handle) = ckb_metrics::handle() {
                            handle.ckb_chain_process_lonely_block_duration.observe(_trace_now.elapsed().as_secs_f64())
                        }

                        if lonely_block_epoch_number > last_check_expired_orphans_epoch {
                            self.clean_expired_orphan_blocks();
                            last_check_expired_orphans_epoch = lonely_block_epoch_number;
                        }
                    },
                    Err(err) => {
                        error!("lonely_block_rx err: {}", err);
                        return
                    }
                },
                recv(self.stop_rx) -> _ => {
                    info!("unverified_queue_consumer got exit signal, exit now");
                    return;
                },
            }
        }
    }

    fn clean_expired_orphan_blocks(&self) {
        let epoch = self.shared.snapshot().tip_header().epoch();
        let expired_blocks = self
            .orphan_blocks_broker
            .clean_expired_blocks(epoch.number());
        if expired_blocks.is_empty() {
            return;
        }
        let expired_blocks_count = expired_blocks.len();
        for block_hash in expired_blocks {
            self.shared.remove_header_view(&block_hash);
        }
        debug!("cleaned {} expired orphan blocks", expired_blocks_count);
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
            self.descendant_processor.accept_descendants(descendants);
        }
    }

    fn process_lonely_block(&self, lonely_block_hash: LonelyBlockHash) {
        let parent_hash = lonely_block_hash.parent_hash();
        let block_hash = lonely_block_hash.hash();
        let block_number = lonely_block_hash.number();

        {
            // Is this block still verifying by ConsumeUnverified?
            // If yes, skip it.
            if let Entry::Occupied(entry) = self.shared.block_status_map().entry(block_hash.clone())
            {
                if entry.get().eq(&BlockStatus::BLOCK_STORED) {
                    debug!(
                        "in process_lonely_block, {} is BLOCK_STORED in block_status_map, it is still verifying by ConsumeUnverified thread",
                        block_hash,
                    );
                    return;
                }
            }
        }

        let parent_status = self
            .shared
            .get_block_status(self.shared.store(), &parent_hash);
        if parent_status.contains(BlockStatus::BLOCK_STORED) {
            debug!(
                "parent {} has stored: {:?}, processing descendant directly {}-{}",
                parent_hash, parent_status, block_number, block_hash,
            );

            if let Err(err) = self
                .descendant_processor
                .process_descendant(lonely_block_hash)
            {
                error!(
                    "process descendant {}-{}, failed {:?}",
                    block_number, block_hash, err
                );
            }
        } else if parent_status.eq(&BlockStatus::BLOCK_INVALID) {
            // don't accept this block, because parent block is invalid
            error!(
                "parent: {} is INVALID, won't accept this block {}-{}",
                parent_hash, block_number, block_hash,
            );
            self.shared
                .block_status_map()
                .insert(block_hash, BlockStatus::BLOCK_INVALID);
            let err = Err(InvalidParentError {
                parent_hash: parent_hash.clone(),
            }
            .into());
            lonely_block_hash.execute_callback(err);
        } else {
            self.orphan_blocks_broker.insert(lonely_block_hash);
        }
        self.search_orphan_pool();

        if let Some(metrics) = ckb_metrics::handle() {
            metrics
                .ckb_chain_orphan_count
                .set(self.orphan_blocks_broker.len() as i64)
        };
    }
}
