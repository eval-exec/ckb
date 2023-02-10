use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

/// open a temporary dir, the dir will be deleted when current process exit.
pub fn long_live_temp_dir() -> Arc<TempDir> {
    static TEMP_DIRS: OnceCell<Mutex<Vec<Arc<TempDir>>>> = OnceCell::new();
    let tmp_dir = Arc::new(tempfile::tempdir().unwrap());
    TEMP_DIRS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .push(Arc::clone(&tmp_dir));
    tmp_dir
}
