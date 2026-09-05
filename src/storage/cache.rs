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
//!
//! # Why the key is a location and not an address
//!
//! It used to be the address, and `read_node` consulted it *before* the
//! primary index — which was faster, because a hit skipped the index
//! descent as well as the heap read.
//!
//! That is only correct when there is exactly one version of a record
//! visible at a time. A [`Snapshot`](crate::storage::btree::Snapshot)
//! reader resolves an address through the index generation it pinned and
//! gets the location that was current *then*; an address-keyed cache
//! would hand it whichever version a concurrent writer had most recently
//! cached, silently, and only under concurrency. A `RecordLocation`
//! names one physical record — one version — so a hit on it is the
//! record the caller's own index lookup asked for, whatever generation
//! that lookup came from.
//!
//! The cost is that a hit no longer skips the index descent. Those pages
//! are the hottest in the buffer pool, so the descent is usually cached
//! reads and no I/O.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::core::node::Node;
use crate::storage::location::RecordLocation;

/// Records held resident by default.
const DEFAULT_CAPACITY: usize = 4096;

const CAPACITY_ENV: &str = "FACETQL_RECORD_CACHE";

struct Inner {
    entries: HashMap<RecordLocation, (Arc<Node>, u64)>,
    /// Use order, oldest first.
    order: BTreeMap<u64, RecordLocation>,
    tick: u64,
    capacity: usize,
}

pub struct RecordCache {
    inner: Mutex<Inner>,
}

impl Default for RecordCache {
    fn default() -> Self {
        RecordCache::new()
    }
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

    pub fn get(&self, location: RecordLocation) -> Option<Arc<Node>> {
        let mut inner = self.lock();

        let (node, previous) = {
            let (node, tick) = inner.entries.get(&location)?;
            (Arc::clone(node), *tick)
        };

        inner.order.remove(&previous);
        inner.tick += 1;
        let tick = inner.tick;
        inner.order.insert(tick, location);

        if let Some(entry) = inner.entries.get_mut(&location) {
            entry.1 = tick;
        }

        Some(node)
    }

    pub fn put(&self, location: RecordLocation, node: Arc<Node>) {
        let mut inner = self.lock();

        if let Some((_, previous)) = inner.entries.get(&location) {
            let previous = *previous;
            inner.order.remove(&previous);
        }

        inner.tick += 1;
        let tick = inner.tick;

        inner.entries.insert(location, (node, tick));
        inner.order.insert(tick, location);

        while inner.entries.len() > inner.capacity {
            let Some((&oldest, &victim)) = inner.order.iter().next() else {
                break;
            };

            inner.order.remove(&oldest);
            inner.entries.remove(&victim);
        }
    }

    /// Drop one version, because the record at that location is gone.
    ///
    /// Not required for correctness — a location nothing indexes is
    /// unreachable, so a stale entry can never be returned — but it frees
    /// the memory now instead of waiting for eviction to notice.
    pub fn invalidate(&self, location: RecordLocation) {
        let mut inner = self.lock();

        if let Some((_, tick)) = inner.entries.remove(&location) {
            inner.order.remove(&tick);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
