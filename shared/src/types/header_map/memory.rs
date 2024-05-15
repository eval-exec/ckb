use crate::types::HeaderIndexView;
use ckb_types::{
    core::{BlockNumber, EpochNumberWithFraction},
    packed::Byte32,
    U256,
};
use ckb_util::{shrink_to_fit, LinkedHashMap, RwLock};
use either::Either;
use std::collections::HashSet;
use std::default;

const SHRINK_THRESHOLD: usize = 300;

#[derive(Clone, Debug, PartialEq, Eq)]
struct HeaderIndexViewInner {
    number: BlockNumber,
    epoch: EpochNumberWithFraction,
    timestamp: u64,
    parent_hash: Byte32,
    total_difficulty: U256,
    skip_hash: Option<Byte32>,
}

impl From<(Byte32, HeaderIndexViewInner)> for HeaderIndexView {
    fn from((hash, inner): (Byte32, HeaderIndexViewInner)) -> Self {
        let HeaderIndexViewInner {
            number,
            epoch,
            timestamp,
            parent_hash,
            total_difficulty,
            skip_hash,
        } = inner;
        Self {
            hash,
            number,
            epoch,
            timestamp,
            parent_hash,
            total_difficulty,
            skip_hash,
        }
    }
}

impl From<HeaderIndexView> for (Byte32, HeaderIndexViewInner) {
    fn from(view: HeaderIndexView) -> Self {
        let HeaderIndexView {
            hash,
            number,
            epoch,
            timestamp,
            parent_hash,
            total_difficulty,
            skip_hash,
        } = view;
        (
            hash,
            HeaderIndexViewInner {
                number,
                epoch,
                timestamp,
                parent_hash,
                total_difficulty,
                skip_hash,
            },
        )
    }
}

#[derive(Default)]
struct MemoryInner {
    linked_map: LinkedHashMap<Byte32, HeaderIndexViewInner>,
    left_set: HashSet<Byte32>,
}

pub(crate) struct MemoryMap(RwLock<MemoryInner>);

impl default::Default for MemoryMap {
    fn default() -> Self {
        Self(RwLock::new(default::Default::default()))
    }
}

impl MemoryMap {
    pub(crate) fn len(&self) -> usize {
        self.0.read().linked_map.len()
    }

    pub(crate) fn contains_key(&self, key: &Byte32) -> bool {
        let inner = self.0.read();
        inner.linked_map.contains_key(key) || inner.left_set.contains(key)
    }

    pub(crate) fn get_refresh(&self, key: &Byte32) -> Option<Either<HeaderIndexView, Byte32>> {
        let mut guard = self.0.write();
        if let Some(view) = guard.linked_map.get_refresh(key) {
            if let Some(metrics) = ckb_metrics::handle() {
                metrics.ckb_header_map_memory_hit_miss_count.hit.inc()
            }
            return Some(Either::<HeaderIndexView, Byte32>::Left(
                (key.to_owned(), view.to_owned()).into(),
            ));
        }
        if let Some(_) = guard.left_set.get(key) {
            if let Some(metrics) = ckb_metrics::handle() {
                metrics.ckb_header_map_memory_hit_miss_count.miss.inc()
            }
            return Some(Either::<HeaderIndexView, Byte32>::Right(key.to_owned()));
        }
        None
    }

    pub(crate) fn insert(&self, header: HeaderIndexView) -> Option<()> {
        let mut guard = self.0.write();
        let (key, value) = header.into();
        let ret = guard.linked_map.insert(key, value);
        ret.map(|_| ())
    }

    pub(crate) fn remove_no_return(&self, key: &Byte32, shrink_to_fit: bool) {
        let mut guard = self.0.write();
        guard.linked_map.remove(key);
        guard.left_set.remove(key);

        if shrink_to_fit {
            shrink_to_fit!(guard.linked_map, SHRINK_THRESHOLD);
        }
    }

    pub(crate) fn front_n(&self, size_limit: usize) -> Option<Vec<HeaderIndexView>> {
        let guard = self.0.read();
        let size = guard.linked_map.iter().count();
        if size > size_limit {
            let num = size - size_limit;
            Some(
                guard
                    .linked_map
                    .iter()
                    .take(num)
                    .map(|(key, value)| (key.to_owned(), value.clone()).into())
                    .collect(),
            )
        } else {
            None
        }
    }

    pub(crate) fn remove_batch(&self, keys: impl Iterator<Item = Byte32>, shrink_to_fit: bool) {
        let mut guard = self.0.write();
        let mut keys_count = 0;
        for key in keys {
            guard.linked_map.remove(&key).and_then(|_| {
                keys_count += 1;
                guard.left_set.insert(key);
                Some(())
            });
        }

        if shrink_to_fit {
            shrink_to_fit!(guard.linked_map, SHRINK_THRESHOLD);
        }
    }
}
