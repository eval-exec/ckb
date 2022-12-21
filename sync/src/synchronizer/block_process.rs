use crate::synchronizer::SendBlockMsgInfo;
use crate::{synchronizer::Synchronizer, utils::is_internal_db_error, Status, StatusCode};
use ckb_logger::{debug, error};
use ckb_network::PeerIndex;
use ckb_types::core::BlockView;
use ckb_types::{packed, prelude::*};
use rtrb::PushError;

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
        let block = self.message.block().to_entity().into_view();
        debug!(
            "BlockProcess received block {} {}",
            block.number(),
            block.hash(),
        );
        let shared = self.synchronizer.shared();
        let state = shared.state();

        if !shared.active_chain().is_initial_block_download() {
            // not in IBD mode, consume block by internal_process_block
            return self.internal_process_block(block);
        }

        let mut producer = self.synchronizer.rtrb_producer.lock();
        if state.new_block_received(&block) {
            let msg_info = SendBlockMsgInfo {
                peer: self.peer,
                item_name: "SendBlock".to_string(),
                item_bytes_length: self.message.as_slice().len() as u64,
                item_id: 2,
            };
            if let Err(PushError::Full(not_pushed_block)) = producer.push((block, msg_info)) {
                // block_queue is full, so Process the block now
                // This rarely happens
                let hash = not_pushed_block.0.hash();
                if let Err(err) = self.synchronizer.process_new_block(not_pushed_block.0) {
                    if !is_internal_db_error(&err) {
                        error!("block {} is invalid: {}", hash, err);
                        return StatusCode::BlockIsInvalid
                            .with_context(format!("{}, error: {}", hash, err,));
                    }
                }
            }
        }
        Status::ignored()
    }

    fn internal_process_block(&self, block: BlockView) -> Status {
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
