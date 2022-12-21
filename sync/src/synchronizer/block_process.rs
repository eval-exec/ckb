use crate::synchronizer::SendBlockMsgInfo;
use crate::{synchronizer::Synchronizer, utils::is_internal_db_error, Status, StatusCode};
use ckb_logger::{debug, error};
use ckb_network::PeerIndex;
use ckb_types::core::BlockView;
use ckb_types::{packed, prelude::*};
use std::sync::Arc;

pub struct BlockProcess<'a> {
    message: packed::SendBlockReader<'a>,
    synchronizer: &'a Synchronizer,
    peer: PeerIndex,
}

impl<'a> BlockProcess<'a> {
    pub fn new(
        message: packed::SendBlockReader<'a>,
        synchronizer: &'a Synchronizer,
        peer: PeerIndex,
    ) -> Self {
        BlockProcess {
            message,
            synchronizer,
            peer,
        }
    }

    pub fn execute(self) -> Status {
        let block = Arc::new(self.message.block().to_entity().into_view());
        let hash = block.hash();
        debug!("BlockProcess received block {} {}", block.number(), hash);
        let shared = self.synchronizer.shared();
        let state = shared.state();

        if !shared.active_chain().is_initial_block_download() {
            let _producer= self.synchronizer.block_producer.lock().expect(
                "BlockProcess want to drop inner value of block_producer, but the lock is poisoned",
            ).take();
            drop(_producer);
            // not in IBD mode, consume block by internal_process_block
            return self.internal_process_block(block);
        }

        if !state.new_block_received(&block) {
            return Status::ignored();
        }

        let msg_info = SendBlockMsgInfo {
            peer: self.peer,
            item_name: "SendBlock".to_string(),
            item_bytes_length: self.message.as_slice().len() as u64,
            item_id: 2,
        };

        if let Some(producer) = self
            .synchronizer
            .block_producer
            .lock()
            .expect("failed to lock block producer")
            .as_mut()
        {
            if producer.push((Arc::clone(&block), msg_info)).is_ok() {
                // block has pushed into blocks ringbuffer, return Status::ignore
                return Status::ignored();
            }
        }

        // the block did not pushed into blocks ringbuffer, process it now
        if let Err(err) = self.synchronizer.process_new_block(block) {
            if !is_internal_db_error(&err) {
                error!("block {} is invalid: {}", hash, err);
                return StatusCode::BlockIsInvalid
                    .with_context(format!("{}, error: {}", hash, err,));
            }
        }
        Status::ignored()
    }

    fn internal_process_block(&self, block: Arc<BlockView>) -> Status {
        if self
            .synchronizer
            .shared()
            .state()
            .new_block_received(&block)
        {
            let hash = block.hash();
            if let Err(err) = self.synchronizer.process_new_block(block) {
                if !is_internal_db_error(&err) {
                    return StatusCode::BlockIsInvalid
                        .with_context(format!("{}, error: {}", hash, err,));
                }
            }
        }

        Status::ok()
    }
}
