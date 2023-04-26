use crate::types::HeaderView;
use crate::types::SHRINK_THRESHOLD;
use ckb_logger::debug;
use ckb_types::packed::Byte32;
use ckb_util::shrink_to_fit;
use ckb_util::LinkedHashMap;
use ckb_util::RwLock;
use std::default;

pub(crate) struct MemoryMap(RwLock<LinkedHashMap<Byte32, HeaderView>>);

impl default::Default for MemoryMap {
    fn default() -> Self {
        Self(RwLock::new(LinkedHashMap::with_capacity(SHRINK_THRESHOLD)))
    }
}

impl MemoryMap {
    #[cfg(feature = "stats")]
    pub(crate) fn len(&self) -> usize {
        self.0.read().len()
    }

    pub(crate) fn contains_key(&self, key: &Byte32) -> bool {
        self.0.read().contains_key(key)
    }

    pub(crate) fn get_refresh(&self, key: &Byte32) -> Option<HeaderView> {
        let mut guard = self.0.write();
        guard.get_refresh(key).cloned()
    }

    pub(crate) fn insert(&self, key: Byte32, value: HeaderView) -> Option<()> {
        let log_now = std::time::Instant::now();
        debug!("begin insert memory map");
        let mut guard = self.0.write();
        let acquire_write_lock = log_now.elapsed();
        let r = guard.insert(key, value).map(|_| ());
        debug!(
            "insert memory map acquire write lock cost: {:?}, acquire + insert: {:?}, have old_value: {}, len/cap:{}/{}",
            acquire_write_lock,
            log_now.elapsed(),
            r.is_some(),
            guard.len(),
            guard.capacity()
        );
        r
    }

    pub(crate) fn remove(&self, key: &Byte32) -> Option<HeaderView> {
        let log_now = std::time::Instant::now();
        let mut guard = self.0.write();
        debug!(
            "remove memory map acquire write lock cost: {:?}",
            log_now.elapsed()
        );
        let ret = guard.remove(key);
        // shrink_to_fit!(guard, SHRINK_THRESHOLD);
        debug!(
            "remove memory map remove and shrink cost: {:?}",
            log_now.elapsed()
        );
        ret
    }

    pub(crate) fn front_n(&self, size_limit: usize) -> Option<Vec<HeaderView>> {
        let log_now = std::time::Instant::now();
        let guard = self.0.read();
        let size = guard.len();
        debug!(
            "limit_memory: front_n memory map acquire read lock cost: {:?}, size:{}, size_limit:{}",
            log_now.elapsed(),
            size,
            size_limit,
        );
        if size > size_limit {
            let num = size - size_limit;
            Some(guard.values().take(num).cloned().collect())
        } else {
            None
        }
    }

    pub(crate) fn remove_batch(&self, keys: impl Iterator<Item = Byte32>) {
        let mut guard = self.0.write();
        for key in keys {
            guard.remove(&key);
        }
        shrink_to_fit!(guard, SHRINK_THRESHOLD);
    }
}
