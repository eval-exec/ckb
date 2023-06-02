mod stop_handler;
mod stop_register;

pub use stop_handler::{SignalSender, StopHandler, WATCH_INIT, WATCH_STOP};

pub use stop_register::{
    broadcast_exit_signals, new_crossbeam_exit_rx, new_tokio_exit_rx, register_thread,
    register_tokio, wait_all_ckb_services_exit,
};
