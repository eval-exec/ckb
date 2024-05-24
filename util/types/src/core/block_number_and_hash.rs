use crate::prelude::*;

use crate::core;
use crate::core::BlockNumber;
use crate::packed::Byte32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockNumberAndHash {
    pub number: BlockNumber,
    pub hash: Byte32,
}

impl BlockNumberAndHash {
    pub fn new(number: BlockNumber, hash: Byte32) -> Self {
        Self { number, hash }
    }

    pub fn number(&self) -> BlockNumber {
        self.number
    }

    pub fn hash(&self) -> Byte32 {
        self.hash.clone()
    }
    // to_db_key let keys sort by number in storage
    pub fn to_db_key(&self) -> Vec<u8> {
        let mut number = self.number.to_be_bytes().to_vec();
        let hash = self.hash.clone();

        number.extend(hash.as_slice());
        number
    }
}

impl From<(BlockNumber, Byte32)> for BlockNumberAndHash {
    fn from(inner: (BlockNumber, Byte32)) -> Self {
        Self {
            number: inner.0,
            hash: inner.1,
        }
    }
}

impl From<&core::HeaderView> for BlockNumberAndHash {
    fn from(header: &core::HeaderView) -> Self {
        Self {
            number: header.number(),
            hash: header.hash(),
        }
    }
}

impl From<core::HeaderView> for BlockNumberAndHash {
    fn from(header: core::HeaderView) -> Self {
        Self {
            number: header.number(),
            hash: header.hash(),
        }
    }
}
