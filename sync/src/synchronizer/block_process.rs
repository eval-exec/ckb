use crate::{synchronizer::Synchronizer, Status, StatusCode};
use ckb_error::is_internal_db_error;
use ckb_logger::debug;
use ckb_network::PeerIndex;
use ckb_types::{packed, prelude::*};

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
            "BlockProcess received block {} {} from peer-{}",
            block.number(),
            block.hash(),
            self.peer,
        );
        let shared = self.synchronizer.shared();

        if shared.new_block_received(&block, self.peer) {
            if let Err(err) = self.synchronizer.process_new_block(block.clone()) {
                if !is_internal_db_error(&err) {
                    return StatusCode::BlockIsInvalid.with_context(format!(
                        "{}, error: {}",
                        block.hash(),
                        err,
                    ));
                }
            }
        } else {
            debug!("BlockProcess received a block {}-{} which we doesn't in inflight_blocks from peer-{}", block.number(), block.hash(), self.peer);
        }

        Status::ok()
    }
}
