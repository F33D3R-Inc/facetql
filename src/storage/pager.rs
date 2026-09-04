//! Page-level I/O with a bounded buffer cache.
//!
//! This is the layer that makes "the database is bigger than memory" an
//! ordinary situation rather than an impossible one. Everything above it
//! — the record heap, every index — addresses storage as *pages*, and
//! reaches them only through here. Which pages happen to be resident is
//! this module's business and nobody else's, so no structure above it
//! can quietly start depending on the whole file being in RAM.
//!
//! # What is bounded, and what that costs
//!
//! The cache holds at most [`Pager::capacity`] pages per file. A miss is
//! completely normal: it seeks, reads 16 KiB, decrypts, verifies and
//! returns. Eviction picks the least recently used page and — if it is
//! dirty — writes it out first.
//!
//! Writing a dirty page out *before its transaction commits* is safe
//! here, and that is not an accident of the implementation but the
//! reason the B-tree above is copy-on-write: a modified page is written
//! to a **newly allocated** page id that no durable meta page references
//! yet, so its bytes are invisible until the meta switch publishes them.
//! Eviction can therefore flush freely without any write-ahead of page
//! images, and memory stays bounded even inside a large transaction.
//!
//! # Encryption and verification
//!
//! A page is stored as `crypto::encrypt(body)` — a 12-byte nonce, the
//! ciphertext of the [`PAGE_BODY_LEN`]-byte body, and a 16-byte GCM tag,
//! which is exactly [`PAGE_SIZE`] bytes. So index keys (addresses,
//! kinds, owners) are encrypted at rest exactly like record payloads
//! are, rather than sitting in the clear in an index file next to an
//! encrypted data file. On read the GCM tag authenticates the page and
//! the page's own CRC and structural checks run on top (see
//! [`Page::decode`]) — the two catch different things, and the CRC is
//! what distinguishes "this page is damaged" from "this file is
//! encrypted under a different key".
//!
//! # Interior mutability
//!
//! Every method takes `&self`. Read paths in the engine run under a
//! read lock and must still be able to fault a page in and record its
//! use, which is a mutation of the cache but not of the database. The
//! `Mutex` is therefore part of the contract, not an implementation
//! detail to be optimized away by handing out `&mut self`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::crypto;
use crate::storage::page::{Page, PAGE_BODY_LEN, PAGE_SIZE};

/// Default resident pages per open file. 256 × 16 KiB = 4 MiB per file;
/// with the heap's active segment and the six indexes that is a few tens
/// of megabytes of buffer pool regardless of how large the database on
/// disk becomes.
const DEFAULT_CACHE_PAGES: usize = 256;

/// Environment override for the per-file cache size, in pages.
const CACHE_PAGES_ENV: &str = "FACETQL_PAGE_CACHE_PAGES";

struct Entry {
    page: Arc<Page>,
    dirty: bool,
    tick: u64,
}

struct PagerInner {
    file: File,
    /// Pages this file is known to contain. Grows when a page beyond the
    /// current end is written; never shrinks except through
    /// [`Pager::set_page_count`].
    page_count: u32,
    cache: HashMap<u32, Entry>,
    /// Use order, oldest first: `tick → page id`. A `BTreeMap` keyed by
    /// a monotonic counter gives O(log n) eviction of the true LRU
    /// without an intrusive linked list.
    order: BTreeMap<u64, u32>,
    tick: u64,
    capacity: usize,
}

pub struct Pager {
    path: PathBuf,
    inner: Mutex<PagerInner>,
}

impl Pager {
    /// Open (creating if absent) the paged file at `path`.
    ///
    /// A file whose length is not a whole number of pages is a torn
    /// extension — a crash while growing the file. The trailing partial
    /// page is not readable as a page and nothing durable can reference
    /// it (a page becomes reachable only once the meta/catalog write
    /// that names it is durable), so the page count simply rounds down
    /// and the partial bytes are overwritten by the next allocation.
    pub fn open(path: &Path) -> Result<Pager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let len = file.metadata()?.len();
        let page_count = (len / PAGE_SIZE as u64) as u32;

        let capacity = std::env::var(CACHE_PAGES_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|pages| *pages > 0)
            .unwrap_or(DEFAULT_CACHE_PAGES);

        Ok(Pager {
            path: path.to_path_buf(),
            inner: Mutex::new(PagerInner {
                file,
                page_count,
                cache: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                capacity,
            }),
        })
    }

    pub fn page_count(&self) -> u32 {
        self.lock().page_count
    }

    /// Force the page count, used when the catalog says a heap segment
    /// is shorter than the file on disk — the pages past the catalog's
    /// count hold records no durable index references (a crash between
    /// filling a page and committing), and appending over them is how
    /// they are reclaimed.
    pub fn set_page_count(&self, pages: u32) {
        self.lock().page_count = pages;
    }

    /// Reserve the next page id. The page does not exist on disk until
    /// something writes it.
    pub fn allocate(&self) -> u32 {
        let mut inner = self.lock();
        let id = inner.page_count;
        inner.page_count += 1;
        id
    }

    /// Read a page, from the cache when it is resident and from disk
    /// when it is not.
    pub fn read(&self, id: u32) -> Result<Arc<Page>> {
        let mut inner = self.lock();

        if let Some(entry) = inner.cache.get(&id) {
            let page = Arc::clone(&entry.page);
            let old_tick = entry.tick;

            inner.order.remove(&old_tick);
            inner.tick += 1;
            let tick = inner.tick;
            inner.order.insert(tick, id);

            if let Some(entry) = inner.cache.get_mut(&id) {
                entry.tick = tick;
            }

            return Ok(page);
        }

        let page = Arc::new(inner.read_from_disk(id, &self.path)?);

        inner.admit(id, Arc::clone(&page), false, &self.path)?;

        Ok(page)
    }

    /// Install `page` as the current contents of `id`, dirty.
    ///
    /// The bytes are not written to disk here — they go out on eviction
    /// or on [`Pager::flush`], whichever comes first.
    pub fn write(&self, id: u32, page: Page) -> Result<()> {
        let mut inner = self.lock();

        if id >= inner.page_count {
            inner.page_count = id + 1;
        }

        inner.admit(id, Arc::new(page), true, &self.path)
    }

    /// Write every dirty page and fsync the file.
    ///
    /// After this returns, everything the caller has written is on
    /// stable storage. It is the step that must precede publishing a new
    /// tree root or a new catalog: a root that names pages which are
    /// still only in the buffer pool is a root that survives a crash
    /// pointing at nothing.
    pub fn flush(&self) -> Result<()> {
        let mut inner = self.lock();

        let dirty: Vec<u32> = inner
            .cache
            .iter()
            .filter(|(_, entry)| entry.dirty)
            .map(|(id, _)| *id)
            .collect();

        for id in dirty {
            inner.write_to_disk(id, &self.path)?;
        }

        inner.file.sync_all()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PagerInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl PagerInner {
    /// Put a page in the cache, evicting if that pushes it over
    /// capacity.
    fn admit(
        &mut self,
        id: u32,
        page: Arc<Page>,
        dirty: bool,
        path: &Path,
    ) -> Result<()> {
        if let Some(existing) = self.cache.get(&id) {
            let old_tick = existing.tick;
            self.order.remove(&old_tick);
        }

        self.tick += 1;
        let tick = self.tick;

        self.cache.insert(id, Entry { page, dirty, tick });
        self.order.insert(tick, id);

        while self.cache.len() > self.capacity {
            let Some((&victim_tick, &victim_id)) =
                self.order.iter().next()
            else {
                break;
            };

            // Never evict the page just admitted: a caller that reads a
            // page expects the Arc it got back to stay valid (it does —
            // it owns it), but evicting the newest entry on every admit
            // would turn a capacity-1 cache into an infinite loop.
            if victim_id == id {
                break;
            }

            self.order.remove(&victim_tick);

            if let Some(entry) = self.cache.get(&victim_id) {
                if entry.dirty {
                    self.write_to_disk(victim_id, path)?;
                }
            }

            self.cache.remove(&victim_id);
        }

        Ok(())
    }

    fn read_from_disk(&mut self, id: u32, path: &Path) -> Result<Page> {
        if id >= self.page_count {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "page {id} is past the end of {} ({} pages)",
                    path.display(),
                    self.page_count
                ),
            ));
        }

        let mut buffer = vec![0u8; PAGE_SIZE];

        self.file.seek(SeekFrom::Start(id as u64 * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut buffer).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to read page {id} of {}: {e}", path.display()),
            )
        })?;

        let body = crypto::decrypt(&buffer).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "failed to decrypt page {id} of {}: {e} — wrong \
                     ENOCHIAN_MASTER_KEY, or the page is damaged",
                    path.display()
                ),
            )
        })?;

        Page::decode(&body).map_err(|e| {
            Error::new(
                e.kind(),
                format!("page {id} of {}: {e}", path.display()),
            )
        })
    }

    fn write_to_disk(&mut self, id: u32, path: &Path) -> Result<()> {
        let Some(entry) = self.cache.get_mut(&id) else {
            return Ok(());
        };

        // The cached page is shared behind an `Arc`; encoding needs to
        // refresh the CRC, so work on a copy rather than requiring
        // exclusive ownership of something a reader may still hold.
        let mut page = (*entry.page).clone();
        let body = page.encode();

        debug_assert_eq!(body.len(), PAGE_BODY_LEN);

        let blob = crypto::encrypt(body);

        if blob.len() != PAGE_SIZE {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "encrypted page is {} bytes, expected {PAGE_SIZE} — the \
                     page body size and the cipher envelope no longer agree",
                    blob.len()
                ),
            ));
        }

        self.file.seek(SeekFrom::Start(id as u64 * PAGE_SIZE as u64))?;
        self.file.write_all(&blob).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to write page {id} of {}: {e}", path.display()),
            )
        })?;

        entry.dirty = false;

        Ok(())
    }
}
