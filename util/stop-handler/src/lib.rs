//! TODO(doc): @keroro520
use ckb_logger::error;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use tokio::sync::oneshot as tokio_oneshot;
use tokio::sync::watch as tokio_watch;

pub use stop_handler::{SignalSender, StopHandler, WATCH_INIT, WATCH_STOP};

pub use stop_register::{
    broadcast_exit_signals, new_crossbeam_exit_rx, new_tokio_exit_rx, register_thread,
    wait_all_ckb_services_exit,
};

#[cfg(test)]
mod tests;
