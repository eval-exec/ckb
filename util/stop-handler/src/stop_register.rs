use ckb_logger::info;
use ckb_util::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

struct CkbServiceHandles {
    thread_handles: Vec<(String, std::thread::JoinHandle<()>)>,
}

pub fn wait_all_ckb_services_exit() {
    info!("wait all ckb service exit");
    let mut handles = CKB_HANDLES.lock();
    for (name, join_handle) in handles.thread_handles.drain(..) {
        match join_handle.join() {
            Ok(_) => {
                println!("wait thread {} done", name);
            }
            Err(e) => {
                println!("wait thread {}: ERROR: {:?}", name, e)
            }
        }
    }
    // tokio::task::block_in_place(|| async {
    //     tokio_exit_recv.recv().await;
    // });
}

static CKB_HANDLES: once_cell::sync::Lazy<Mutex<CkbServiceHandles>> =
    once_cell::sync::Lazy::new(|| {
        Mutex::new(CkbServiceHandles {
            thread_handles: vec![],
        })
    });

static TOKIO_EXIT: once_cell::sync::Lazy<CancellationToken> =
    once_cell::sync::Lazy::new(CancellationToken::new);

static CROSSBEAM_EXIT_SENDERS: once_cell::sync::Lazy<Mutex<Vec<ckb_channel::Sender<()>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(vec![]));

pub fn new_tokio_exit_rx() -> CancellationToken {
    TOKIO_EXIT.clone()
}

pub fn new_crossbeam_exit_rx() -> ckb_channel::Receiver<()> {
    let (tx, rx) = ckb_channel::bounded(1);
    CROSSBEAM_EXIT_SENDERS.lock().push(tx);
    rx
}

pub fn broadcast_exit_signals() {
    TOKIO_EXIT.cancel();
    CROSSBEAM_EXIT_SENDERS.lock().iter().for_each(|tx| {
        if let Err(e) = tx.try_send(()) {
            println!("broadcast thread: ERROR: {:?}", e)
        } else {
            println!("send a crossbeam exit signal");
        }
    });
}

pub fn register_thread(name: &str, thread_handle: std::thread::JoinHandle<()>) {
    CKB_HANDLES
        .lock()
        .thread_handles
        .push((name.into(), thread_handle));
}
