use crate::synchronizer::TimeCost;
use crate::{synchronizer::Synchronizer, utils::is_internal_db_error, Status, StatusCode};
use ckb_logger::{debug, info};
use ckb_network::PeerIndex;
use ckb_types::{packed, prelude::*};

pub struct BlockProcess<'a> {
    message: packed::SendBlockReader<'a>,
    synchronizer: &'a Synchronizer,
    _peer: PeerIndex,
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
            _peer: peer,
        }
    }

    pub fn execute(self, time_cost: &TimeCost) -> Status {
        let block = self.message.block().to_entity().into_view();
        debug!(
            "BlockProcess received block {} {}",
            block.number(),
            block.hash(),
        );

        let block_number = block.number();
        if block_number == 1 || block_number % 100_000 == 0 {
            info!(
                "block:{}, synchronizer: time_cost {:?}",
                block_number, time_cost
            );
        }

        let shared = self.synchronizer.shared();
        let state = shared.state();

        if state.new_block_received(&block) {
            if let Err(err) = self.synchronizer.process_new_block(block.clone()) {
                if !is_internal_db_error(&err) {
                    return StatusCode::BlockIsInvalid.with_context(format!(
                        "{}, error: {}",
                        block.hash(),
                        err,
                    ));
                }
            }
        }

        Status::ok()
    }
}
