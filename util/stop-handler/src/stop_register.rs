use ckb_util::Mutex;

struct CkbHandle {
    thread_handles: Vec<std::thread::JoinHandle<()>>,
    tokio_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for CkbHandle {
    fn drop(&mut self) {
        for handle in self.thread_handles.drain(..) {
            match handle.join() {
                Ok(_) => {}
                Err(e) => {
                    todo!("log error")
                }
            }
        }
        for handle in self.tokio_handles.drain(..) {
            match tokio::runtime::Handle::current().block_on(handle) {
                Ok(_) => {}
                Err(e) => {
                    todo!("log error")
                }
            }
        }
    }
}

static CKB_HANDLES: once_cell::sync::Lazy<Mutex<CkbHandle>> = once_cell::sync::Lazy::new(|| {
    Mutex::new(CkbHandle {
        thread_handles: vec![],
        tokio_handles: vec![],
    })
});

pub fn register_thread(thread_handle: std::thread::JoinHandle<()>) {
    CKB_HANDLES.lock().thread_handles.push(thread_handle);
}
pub fn register_tokio(tokio_handle: tokio::task::JoinHandle<()>) {
    CKB_HANDLES.lock().tokio_handles.push(tokio_handle);
}
