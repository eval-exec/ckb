#![allow(missing_docs)]

//! CKB chain service.
//!
//! [`ChainService`] background base on database, handle block importing,
//! the [`ChainController`] is responsible for receive the request and returning response
//!
//! [`ChainService`]: chain/struct.ChainService.html
//! [`ChainController`]: chain/struct.ChainController.html
use ckb_error::Error;
use ckb_shared::types::BlockNumberAndHash;
use ckb_types::core::service::Request;
use ckb_types::core::{BlockNumber, BlockView, EpochNumber};
use ckb_types::packed::Byte32;
use ckb_verification_traits::Switch;
use std::sync::Arc;

mod chain_controller;
mod chain_service;
mod consume_orphan;
mod consume_unverified;
mod init;
mod init_load_unverified;
#[cfg(test)]
mod tests;
mod utils;

pub use chain_controller::ChainController;
pub use consume_orphan::store_unverified_block;
pub use init::start_chain_services;

type ProcessBlockRequest = Request<LonelyBlock, ()>;
type TruncateRequest = Request<Byte32, Result<(), Error>>;

/// VerifyResult is the result type to represent the result of block verification
///
/// Ok(true) : it's a newly verified block
/// Ok(false): it's a block which has been verified before
/// Err(err) : it's a block which failed to verify
pub type VerifyResult = Result<bool, Error>;

/// VerifyCallback is the callback type to be called after block verification
pub type VerifyCallback = Box<dyn FnOnce(VerifyResult) + Send + Sync>;

/// RemoteBlock is received from ckb-sync and ckb-relayer
pub struct RemoteBlock {
    /// block
    pub block: Arc<BlockView>,

    /// Relayer and Synchronizer will have callback to ban peer
    pub verify_callback: VerifyCallback,
}

/// LonelyBlock is the block which we have not check weather its parent is stored yet
pub struct LonelyBlock {
    /// block
    pub block: Arc<BlockView>,

    /// The Switch to control the verification process
    pub switch: Option<Switch>,

    /// The optional verify_callback
    pub verify_callback: Option<VerifyCallback>,
}

/// LonelyBlock is the block which we have not check weather its parent is stored yet
pub struct LonelyBlockHash {
    /// block
    pub block_number_and_hash: BlockNumberAndHash,
    /// parent hash
    pub parent_hash: Byte32,

    pub epoch_number: EpochNumber,

    /// The Switch to control the verification process
    pub switch: Option<Switch>,

    /// The optional verify_callback
    pub verify_callback: Option<VerifyCallback>,
}

impl From<LonelyBlock> for LonelyBlockHash {
    fn from(val: LonelyBlock) -> Self {
        LonelyBlockHash {
            block_number_and_hash: BlockNumberAndHash {
                number: val.block.number(),
                hash: val.block.hash(),
            },
            epoch_number: val.block.epoch().number(),

            parent_hash: val.block.parent_hash(),
            switch: val.switch,
            verify_callback: val.verify_callback,
        }
    }
}

impl LonelyBlockHash {
    pub fn execute_callback(self, verify_result: VerifyResult) {
        if let Some(verify_callback) = self.verify_callback {
            verify_callback(verify_result);
        }
    }

    pub fn epoch_number(&self) -> EpochNumber {
        self.epoch_number
    }

    pub fn number_hash(&self) -> BlockNumberAndHash {
        self.block_number_and_hash.clone()
    }

    pub fn hash(&self) -> Byte32 {
        self.block_number_and_hash.hash()
    }

    pub fn number(&self) -> BlockNumber {
        self.block_number_and_hash.number()
    }

    pub fn parent_hash(&self) -> Byte32 {
        self.parent_hash.clone()
    }
}

impl LonelyBlock {
    pub(crate) fn block(&self) -> &Arc<BlockView> {
        &self.block
    }

    pub fn switch(&self) -> Option<Switch> {
        self.switch
    }

    pub fn execute_callback(self, verify_result: VerifyResult) {
        if let Some(verify_callback) = self.verify_callback {
            verify_callback(verify_result);
        }
    }
}

pub(crate) struct GlobalIndex {
    pub(crate) number: BlockNumber,
    pub(crate) hash: Byte32,
    pub(crate) unseen: bool,
}

impl GlobalIndex {
    pub(crate) fn new(number: BlockNumber, hash: Byte32, unseen: bool) -> GlobalIndex {
        GlobalIndex {
            number,
            hash,
            unseen,
        }
    }

    pub(crate) fn forward(&mut self, hash: Byte32) {
        self.number -= 1;
        self.hash = hash;
    }
}
