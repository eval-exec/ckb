//! TODO(doc): @quake

// num_cpus is used in proc_macro
mod block_status;
pub mod shared;
mod types;

pub use block_status::BlockStatus;
pub use ckb_snapshot::{Snapshot, SnapshotMgr};
pub use shared::Shared;
pub use types::{HeaderMap, HeaderView};
