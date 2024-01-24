#![allow(missing_docs)]

//! Bootstrap ChainService, ConsumeOrphan and ConsumeUnverified threads.
use crate::chain_service::ChainService;
use crate::consume_orphan::ConsumeOrphan;
use crate::consume_unverified::ConsumeUnverifiedBlocks;
use crate::fill_unverified_blocks_channel::FillUnverifiedBlocksChannel;
use crate::init_load_unverified::InitLoadUnverified;
use crate::utils::orphan_block_pool::OrphanBlockPool;
use crate::{ChainController, LonelyBlockHash, UnverifiedBlock};
use ckb_channel::{self as channel, SendError};
use ckb_constant::sync::BLOCK_DOWNLOAD_WINDOW;
use ckb_logger::warn;
use ckb_shared::ChainServicesBuilder;
use ckb_stop_handler::{new_crossbeam_exit_rx, register_thread};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

const ORPHAN_BLOCK_SIZE: usize = (BLOCK_DOWNLOAD_WINDOW * 2) as usize;

pub fn start_chain_services(builder: ChainServicesBuilder) -> ChainController {
    let orphan_blocks_broker = Arc::new(OrphanBlockPool::with_capacity(ORPHAN_BLOCK_SIZE));

    let (truncate_block_tx, truncate_block_rx) = channel::bounded(1);

    let (fill_unverified_stop_tx, fill_unverified_stop_rx) = ckb_channel::bounded::<()>(1);

    let (unverified_queue_stop_tx, unverified_queue_stop_rx) = ckb_channel::bounded::<()>(1);

    let (fill_unverified_tx, fill_unverified_rx) = channel::unbounded::<LonelyBlockHash>();

    let (unverified_block_tx, unverified_block_rx) =
        channel::bounded::<UnverifiedBlock>(BLOCK_DOWNLOAD_WINDOW as usize * 3);

    let consumer_unverified_thread = thread::Builder::new()
        .name("consume_unverified_block".into())
        .spawn({
            let shared = builder.shared.clone();
            move || {
                let consume_unverified = ConsumeUnverifiedBlocks::new(
                    shared,
                    truncate_block_rx,
                    builder.proposal_table,
                    unverified_block_rx,
                    unverified_queue_stop_rx,
                );

                consume_unverified.start();
            }
        })
        .expect("start unverified_queue consumer thread should ok");


    let fill_unverified_block_thread = thread::Builder::new()
        .name("fill_unverified_block".into())
        .spawn({
            let shared = builder.shared.clone();
            move || {
                let fill_unverified_block = FillUnverifiedBlocksChannel::new(
                    shared,
                    fill_unverified_rx,
                    unverified_block_tx,
                    fill_unverified_stop_rx,
                );
                fill_unverified_block.start()
            }
        })
        .expect("start fill_unverified_block should ok");

    let (process_block_tx, process_block_rx) = channel::bounded(BLOCK_DOWNLOAD_WINDOW as usize);

    let is_verifying_unverified_blocks_on_startup = Arc::new(AtomicBool::new(true));

    let chain_controller = ChainController::new(
        process_block_tx,
        truncate_block_tx,
        orphan_blocks_broker.clone(),
        Arc::clone(&is_verifying_unverified_blocks_on_startup),
    );

    let init_load_unverified_thread = thread::Builder::new()
        .name("init_load_unverified_blocks".into())
        .spawn({
            let chain_controller = chain_controller.clone();
            let signal_receiver = new_crossbeam_exit_rx();
            let shared = builder.shared.clone();

            move || {
                let init_load_unverified: InitLoadUnverified = InitLoadUnverified::new(
                    shared,
                    chain_controller,
                    signal_receiver,
                    is_verifying_unverified_blocks_on_startup,
                );
                init_load_unverified.start();
            }
        })
        .expect("start unverified_queue consumer thread should ok");

    let consume_orphan = ConsumeOrphan::new(
        builder.shared.clone(),
        orphan_blocks_broker,
        fill_unverified_tx,
    );

    let chain_service: ChainService =
        ChainService::new(builder.shared, process_block_rx, consume_orphan);
    let chain_service_thread = thread::Builder::new()
        .name("ChainService".into())
        .spawn({
            move || {
                chain_service.start_process_block();

                let _ = init_load_unverified_thread.join();
                
                if let Err(_) = fill_unverified_stop_tx.send(()){
                    warn!("trying to notify fill unverified thread to stop, but fill_unverified_stop_tx already closed");
                }
                let _ = fill_unverified_block_thread.join();

                if let Err(SendError(_)) = unverified_queue_stop_tx.send(()) {
                    warn!("trying to notify consume unverified thread to stop, but unverified_queue_stop_tx already closed");
                }
                let _ = consumer_unverified_thread.join();
            }
        })
        .expect("start chain_service thread should ok");
    register_thread("ChainServices", chain_service_thread);

    chain_controller
}
