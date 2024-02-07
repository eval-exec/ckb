#![allow(missing_docs)]

//! Bootstrap ChainService, ConsumeOrphan and ConsumeUnverified threads.
use crate::chain_service::ChainService;
use crate::consume_unverified::ConsumeUnverifiedBlocks;
use crate::utils::orphan_block_pool::OrphanBlockPool;
use crate::{ChainController, LonelyBlock, LonelyBlockHash};
use ckb_channel::{self as channel, SendError};
use ckb_constant::sync::BLOCK_DOWNLOAD_WINDOW;
use ckb_db::{Direction, IteratorMode};
use ckb_db_schema::COLUMN_NUMBER_HASH;
use ckb_logger::{info, warn};
use ckb_shared::{ChainServicesBuilder, Shared};
use ckb_stop_handler::register_thread;
use ckb_store::ChainStore;
use ckb_types::core::{BlockNumber, BlockView};
use ckb_types::packed;
use ckb_types::prelude::{Builder, Entity, FromSliceShouldBeOk, Pack, Reader};
use std::sync::Arc;
use std::thread;

const ORPHAN_BLOCK_SIZE: usize = (BLOCK_DOWNLOAD_WINDOW * 2) as usize;

fn bootstrap_chain_services(builder: ChainServicesBuilder) -> ChainController {
    let orphan_blocks_broker = Arc::new(OrphanBlockPool::with_capacity(ORPHAN_BLOCK_SIZE));

    let (truncate_block_tx, truncate_block_rx) = channel::bounded(1);

    let (unverified_queue_stop_tx, unverified_queue_stop_rx) = ckb_channel::bounded::<()>(1);
    let (unverified_tx, unverified_rx) =
        channel::bounded::<LonelyBlockHash>(BLOCK_DOWNLOAD_WINDOW as usize * 3);

    let consumer_unverified_thread = thread::Builder::new()
        .name("consume_unverified_blocks".into())
        .spawn({
            let shared = builder.shared.clone();
            move || {
                let consume_unverified = ConsumeUnverifiedBlocks::new(
                    shared,
                    unverified_rx,
                    truncate_block_rx,
                    builder.proposal_table,
                    unverified_queue_stop_rx,
                );

                consume_unverified.start();
            }
        })
        .expect("start unverified_queue consumer thread should ok");

    let (lonely_block_tx, lonely_block_rx) =
        channel::bounded::<LonelyBlock>(BLOCK_DOWNLOAD_WINDOW as usize);

    let (search_orphan_pool_stop_tx, search_orphan_pool_stop_rx) = ckb_channel::bounded::<()>(1);

    let search_orphan_pool_thread = thread::Builder::new()
        .name("consume_orphan_blocks".into())
        .spawn({
            let orphan_blocks_broker = Arc::clone(&orphan_blocks_broker);
            let shared = builder.shared.clone();
            use crate::consume_orphan::ConsumeOrphan;
            move || {
                let consume_orphan = ConsumeOrphan::new(
                    shared,
                    orphan_blocks_broker,
                    unverified_tx,
                    lonely_block_rx,
                    search_orphan_pool_stop_rx,
                );
                consume_orphan.start();
            }
        })
        .expect("start search_orphan_pool thread should ok");

    let (process_block_tx, process_block_rx) = channel::bounded(BLOCK_DOWNLOAD_WINDOW as usize);

    let chain_service: ChainService =
        ChainService::new(builder.shared, process_block_rx, lonely_block_tx);
    let chain_service_thread = thread::Builder::new()
        .name("ChainService".into())
        .spawn({
            move || {
                chain_service.start_process_block();

                if let Err(SendError(_)) = search_orphan_pool_stop_tx.send(()) {
                    warn!("trying to notify search_orphan_pool thread to stop, but search_orphan_pool_stop_tx already closed")
                }
                let _ = search_orphan_pool_thread.join();

                if let Err(SendError(_))= unverified_queue_stop_tx.send(()){
                    warn!("trying to notify consume unverified thread to stop, but unverified_queue_stop_tx already closed");
                }
                let _ = consumer_unverified_thread.join();
            }
        })
        .expect("start chain_service thread should ok");
    register_thread("ChainServices", chain_service_thread);

    ChainController::new(process_block_tx, truncate_block_tx, orphan_blocks_broker)
}

fn find_unverified_block_hashes(
    shared: &Shared,
    check_unverified_number: u64,
) -> Vec<packed::Byte32> {
    let prefix_number_hash = packed::NumberHash::new_builder()
        .number(check_unverified_number.pack())
        .block_hash(packed::Byte32::default())
        .build();
    let prefix = prefix_number_hash.as_slice();

    let unverified_hashes: Vec<packed::Byte32> = shared
        .store()
        .get_iter(
            COLUMN_NUMBER_HASH,
            IteratorMode::From(prefix, Direction::Forward),
        )
        .take_while(|(key, _)| key.starts_with(prefix))
        .map(|(key_number_hash, _v)| {
            let reader =
                packed::NumberHashReader::from_slice_should_be_ok(key_number_hash.as_ref());
            let unverified_block_hash = reader.block_hash().to_entity();
            unverified_block_hash
        })
        .collect::<Vec<packed::Byte32>>();
    unverified_hashes
}

fn find_and_verify_unverified_blocks(shared: Shared, chain_controller: ChainController) {
    let tip_number: BlockNumber = shared.snapshot().tip_number();

    let mut check_unverified_number = tip_number + 1;

    loop {
        // start checking `check_unverified_number` have COLUMN_NUMBER_HASH value in db?
        let unverified_hashes: Vec<packed::Byte32> =
            find_unverified_block_hashes(&shared, check_unverified_number);

        if unverified_hashes.is_empty() {
            if check_unverified_number == tip_number + 1 {
                info!("no unverified blocks found.");
            } else {
                info!(
                    "found and verify unverified blocks finish, current tip: {}-{}",
                    shared.snapshot().tip_number(),
                    shared.snapshot().tip_header()
                );
            }
            return;
        }

        for unverified_hash in unverified_hashes {
            let unverified_block: BlockView = shared
                .store()
                .get_block(&unverified_hash)
                .expect("unverified block must be in db");
            chain_controller.asynchronous_process_lonely_block(LonelyBlock {
                block: Arc::new(unverified_block),
                switch: None,
                verify_callback: None,
            });
        }

        check_unverified_number += 1;
    }
}

pub fn start_chain_services(builder: ChainServicesBuilder) -> ChainController {
    let shared = builder.shared.clone();
    let chain_controller = bootstrap_chain_services(builder);

    info!(
        "finding unverified blocks, tip: {}-{}",
        shared.snapshot().tip_number(),
        shared.snapshot().tip_hash()
    );
    find_and_verify_unverified_blocks(shared, chain_controller.clone());

    chain_controller
}
