use ckb_util::Mutex;

struct CkbServiceHandles {
    thread_handles: Vec<std::thread::JoinHandle<()>>,
    tokio_handles: Vec<tokio::task::JoinHandle<()>>,
}

pub fn wait_all_ckb_services_exit() {
    let mut handles = CKB_HANDLES.lock();
    for handle in handles.thread_handles.drain(..) {
        match handle.join() {
            Ok(_) => {}
            Err(e) => {
                todo!("log error")
            }
        }
    }
    for handle in handles.tokio_handles.drain(..) {
        match tokio::runtime::Handle::current().block_on(handle) {
            Ok(_) => {}
            Err(e) => {
                todo!("log error")
            }
        }
    }
}

static CKB_HANDLES: once_cell::sync::Lazy<Mutex<CkbServiceHandles>> =
    once_cell::sync::Lazy::new(|| {
        Mutex::new(CkbServiceHandles {
            thread_handles: vec![],
            tokio_handles: vec![],
        })
    });

static TOKIO_EXIT: once_cell::sync::Lazy<(
    tokio::sync::watch::Sender<bool>,
    tokio::sync::watch::Receiver<bool>,
)> = once_cell::sync::Lazy::new(|| {
    let (tx, rx) = tokio::sync::watch::channel(false);
    (tx, rx)
});

static CROSSBEAM_EXIT_SENDERS: once_cell::sync::Lazy<Mutex<Vec<ckb_channel::Sender<()>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(vec![]));

pub fn new_tokio_exit_rx() -> tokio::sync::watch::Receiver<bool> {
    TOKIO_EXIT.1.clone()
}

pub fn new_crossbeam_exit_rx() -> ckb_channel::Receiver<()> {
    let (tx, rx) = ckb_channel::bounded(1);
    CROSSBEAM_EXIT_SENDERS.lock().push(tx);
    rx
}

pub fn broadcast_exit_signals() {
    TOKIO_EXIT.0.send_modify(|x| *x = true);
    CROSSBEAM_EXIT_SENDERS.lock().iter().for_each(|tx| {
        if let Err(e) = tx.try_send(()) {
            todo!("log error")
        }
    });
}

pub fn register_thread(thread_handle: std::thread::JoinHandle<()>) {
    CKB_HANDLES.lock().thread_handles.push(thread_handle);
}
pub fn register_tokio(tokio_handle: tokio::task::JoinHandle<()>) {
    CKB_HANDLES.lock().tokio_handles.push(tokio_handle);
}
