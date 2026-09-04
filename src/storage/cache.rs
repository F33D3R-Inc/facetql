//! A bounded cache of decoded records.
//!
//! This exists purely to make repeated reads cheap. It is not the
//! database and nothing is ever true only because it is in here: every
//! entry can be re-derived from the heap through the primary index, a
//! miss is an ordinary event, and an empty cache changes how long a
//! query takes and never what it answers. That distinction is the
//! difference between a cache and the `HashMap<String, Node>` this
//! engine used to keep, which looked similar and was in fact the
//! database — unbounded, authoritative, and required to be complete.
//!
//! It sits above the page cache in `storage::pager` and saves different
//! work: the pager saves the read and the decryption of a 16 KiB page,
//! this saves the frame check and the bincode decode of one record.
//!
//! Entries are handed out as `Arc<Node>` so a hit costs a refcount
//! bump rather than a clone of the node's strings.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::core::node::Node;

/// Records held resident by default.
const DEFAULT_CAPACITY: usize = 4096;

const CAPACITY_ENV: &str = "FACETQL_RECORD_CACHE";

struct Inner {
    entries: HashMap<String, (Arc<Node>, u64)>,
    /// Use order, oldest first.
    order: BTreeMap<u64, String>,
    tick: u64,
    capacity: usize,
}

pub struct RecordCache {
    inner: Mutex<Inner>,
}

impl RecordCache {
    pub fn new() -> RecordCache {
        let capacity = std::env::var(CAPACITY_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|records| *records > 0)
            .unwrap_or(DEFAULT_CAPACITY);

        RecordCache {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                capacity,
            }),
        }
    }

    pub fn get(&self, address: &str) -> Option<Arc<Node>> {
        let mut inner = self.lock();

        let (node, previous) = {
            let (node, tick) = inner.entries.get(address)?;
            (Arc::clone(node), *tick)
        };

        inner.order.remove(&previous);
        inner.tick += 1;
        let tick = inner.tick;
        inner.order.insert(tick, address.to_string());

        if let Some(entry) = inner.entries.get_mut(address) {
            entry.1 = tick;
        }

        Some(node)
    }

    pub fn put(&self, address: &str, node: Arc<Node>) {
        let mut inner = self.lock();

        if let Some((_, previous)) = inner.entries.get(address) {
            let previous = *previous;
            inner.order.remove(&previous);
        }

        inner.tick += 1;
        let tick = inner.tick;

        inner.entries.insert(address.to_string(), (node, tick));
        inner.order.insert(tick, address.to_string());

        while inner.entries.len() > inner.capacity {
            let Some((&oldest, victim)) = inner.order.iter().next() else {
                break;
            };

            let victim = victim.clone();

            inner.order.remove(&oldest);
            inner.entries.remove(&victim);
        }
    }

    /// Drop an address, because the record it named is gone or has been
    /// replaced by something this cache has not seen.
    pub fn invalidate(&self, address: &str) {
        let mut inner = self.lock();

        if let Some((_, tick)) = inner.entries.remove(address) {
            inner.order.remove(&tick);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
