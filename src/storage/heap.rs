//! The record heap: where node, edge and history records physically
//! live.
//!
//! # Shape
//!
//! ```text
//!   heap
//!    ├── segment 0      facetql.heap.000000.seg
//!    │    ├── page 0    16 KiB, slotted
//!    │    ├── page 1
//!    │    └── ...
//!    ├── segment 1
//!    └── ...
//! ```
//!
//! Records are appended, never updated in place: writing a node again
//! writes a new record and the primary index is repointed at it, which
//! leaves the previous record on disk as reclaimable garbage. That is
//! the same append-oriented model the engine has always had, with two
//! things it did not have — a physical address for each record
//! ([`RecordLocation`]), and a boundary at which old bytes can actually
//! be reclaimed (a segment).
//!
//! Segments exist so the heap can be compacted at all. One endlessly
//! growing file can only be rewritten in full; a set of bounded files
//! can have the dead one drained into the live one and then deleted,
//! while the database keeps serving.
//!
//! # Records larger than a page
//!
//! A record whose frame does not fit in one page is stored as a chain of
//! overflow pages, addressed by a short **stub** cell in an ordinary
//! heap page. The stub is what the index points at, so an oversized
//! record has exactly the same kind of address as any other and is found
//! by a segment scan like any other — which is what lets compaction
//! migrate it.
//!
//! # Durability
//!
//! Appends land in the buffer pool and reach disk at
//! [`RecordStore::sync`], not at the append. The WAL is what makes a
//! mutation durable; the heap catches up at the next checkpoint, and
//! until it does, the WAL replays anything missing. Records written but
//! not yet synced are invisible to a restart precisely because the
//! index that names them has not committed either.

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::config;
use crate::core::edge::Edge;
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::storage::binary;
use crate::storage::catalog::{Catalog, SegmentMeta};
use crate::storage::location::RecordLocation;
use crate::storage::page::{Page, PageKind, MAX_CELL_LEN};
use crate::storage::pager::Pager;

/// Pages a segment holds before the next append rolls to a fresh one.
/// 4096 × 16 KiB = 64 MiB, small enough that compacting one is a bounded
/// piece of work and large enough that segments do not proliferate.
const MAX_PAGES_PER_SEGMENT: u32 = 4096;

/// Marks a cell as the stub of an overflow chain rather than a record
/// frame. Chosen to be distinguishable from `binary::RECORD_MAGIC` in
/// the first byte, so classifying a cell never depends on its length.
const OVERFLOW_MAGIC: [u8; 4] = *b"FQOV";

/// `magic(4) | total length(4) | first chain page(4)`.
const OVERFLOW_STUB_LEN: usize = 12;

/// One record as it is stored.
///
/// The heap is typed because compaction has to be: to decide whether a
/// record it finds is still live, it must know which index to ask, and
/// that means knowing what the record *is* without consulting anything
/// else first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeapRecord {
    Node(Node),
    Edge(Edge),
    History(HistoryEntry),
}

/// Segment files this store keeps open at once.
///
/// # Why there has to be a limit
///
/// A segment caps at 64 MiB, so segment count grows without bound as the
/// database does: a terabyte of records is roughly sixteen thousand of
/// them. Every segment reached by a point read used to be opened and
/// then kept open forever — nothing but `drop_segment` ever removed one
/// — so two resources scaled with the number of segments a workload
/// *touches* rather than with anything the operator chose:
///
/// ```text
///   open file descriptors   one per segment, against the process rlimit
///   buffer pool             Pager::capacity pages per open segment
/// ```
///
/// At the pager's default of 256 pages that is 4 MiB per segment, so a
/// scan across ten thousand segments asks for ten thousand descriptors
/// and forty gigabytes of cache. Both fail, and the descriptor one fails
/// as `EMFILE` on some unrelated later open — a failure that names
/// nothing about its cause.
///
/// 64 open segments is far more than a point-read workload needs (reads
/// concentrate in recent segments) and bounds the pool at a predictable
/// 256 MiB worst case.
const DEFAULT_OPEN_SEGMENTS: usize = 64;

const OPEN_SEGMENTS_ENV: &str = "FACETQL_OPEN_SEGMENTS";

fn open_segment_capacity() -> usize {
    static CAPACITY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    *CAPACITY.get_or_init(|| {
        std::env::var(OPEN_SEGMENTS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|segments| *segments > 0)
            .unwrap_or(DEFAULT_OPEN_SEGMENTS)
    })
}

/// The open-segment table: the pagers, plus the use order that decides
/// which one closes when the table is full.
struct OpenSegments {
    pagers: BTreeMap<u32, (Arc<Pager>, u64)>,
    /// Use order, oldest first: `tick → segment id`.
    order: BTreeMap<u64, u32>,
    tick: u64,
}

pub struct RecordStore {
    catalog: Arc<Catalog>,
    /// Open segment files. Populated on demand and bounded — a database
    /// with many segments neither opens them all to answer one point
    /// read nor keeps every one it has ever touched.
    segments: Mutex<OpenSegments>,
}

impl OpenSegments {
    /// Mark a segment as most recently used.
    fn touch(&mut self, id: u32, previous: u64) {
        self.order.remove(&previous);

        self.tick += 1;
        let tick = self.tick;

        self.order.insert(tick, id);

        if let Some(entry) = self.pagers.get_mut(&id) {
            entry.1 = tick;
        }
    }

    /// Put a freshly opened pager in the table as most recently used.
    fn admit(&mut self, id: u32, pager: Arc<Pager>) {
        self.tick += 1;
        let tick = self.tick;

        self.pagers.insert(id, (pager, tick));
        self.order.insert(tick, id);
    }

    /// Close least-recently-used segments until the table fits in
    /// `capacity`.
    ///
    /// Two segments are never closed, and both exclusions are about
    /// correctness rather than efficiency:
    ///
    /// * **The active segment.** It is the one being appended to, and an
    ///   append in progress reads its last page, mutates it and writes
    ///   it back. Closing it mid-append would drop the buffered page
    ///   between those steps.
    ///
    /// * **Any pager somebody still holds.** `pager()` hands out an
    ///   `Arc`, and a caller may keep it across further calls that
    ///   themselves open segments — `scan_segment` holds one for the
    ///   whole walk. A strong count above the table's own reference
    ///   means exactly that, so it is the precise test for "in use", not
    ///   an approximation of one.
    ///
    /// A closed segment is flushed first. Its pages are copy-on-write or
    /// append-only and its records are in the WAL either way, but
    /// dropping dirty pages would silently undo work the caller already
    /// believes is in the buffer pool, and the next `sync` would have
    /// nothing to write.
    fn evict_down_to(&mut self, capacity: usize, active: u32) -> Result<()> {
        while self.pagers.len() > capacity {
            let victim = self
                .order
                .iter()
                .map(|(tick, id)| (*tick, *id))
                .find(|(_, id)| {
                    *id != active
                        && self
                            .pagers
                            .get(id)
                            .is_some_and(|(pager, _)| Arc::strong_count(pager) == 1)
                });

            // Everything resident is pinned or active. The table is over
            // capacity for as long as that lasts, which is bounded by the
            // call in progress; forcing an eviction here would break the
            // caller holding the pager.
            let Some((tick, id)) = victim else {
                break;
            };

            self.order.remove(&tick);

            if let Some((pager, _)) = self.pagers.remove(&id) {
                pager.flush()?;
            }
        }

        Ok(())
    }

    /// Forget a segment entirely — it is being retired.
    fn forget(&mut self, id: u32) {
        if let Some((_, tick)) = self.pagers.remove(&id) {
            self.order.remove(&tick);
        }
    }

    /// Every resident pager, for a flush of the whole store.
    fn resident(&self) -> Vec<Arc<Pager>> {
        self.pagers.values().map(|(pager, _)| Arc::clone(pager)).collect()
    }
}

impl RecordStore {
    pub fn open(catalog: Arc<Catalog>) -> RecordStore {
        RecordStore {
            catalog,
            segments: Mutex::new(OpenSegments {
                pagers: BTreeMap::new(),
                order: BTreeMap::new(),
                tick: 0,
            }),
        }
    }

    pub fn segment_path(id: u32) -> std::path::PathBuf {
        config::data_file(&format!("facetql.heap.{id:06}.seg"))
    }

    /// The pager for a segment, opening the file if this is its first
    /// use and closing the least recently used one if that puts the
    /// table over capacity.
    fn pager(&self, id: u32) -> Result<Arc<Pager>> {
        let mut open = self.lock();

        if let Some((pager, previous)) = open.pagers.get(&id) {
            let pager = Arc::clone(pager);
            let previous = *previous;

            open.touch(id, previous);

            return Ok(pager);
        }

        let known = self
            .catalog
            .with(|data| data.segments.iter().find(|s| s.id == id).map(|s| s.pages));

        let Some(pages) = known else {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!(
                    "heap segment {id} is not in the catalog — a record \
                     location refers to a segment that has been retired"
                ),
            ));
        };

        let pager = Arc::new(Pager::open(&RecordStore::segment_path(id))?);

        // The catalog is the authority on how much of the segment is
        // real. A longer file is the residue of a crash between filling
        // pages and committing; those pages hold records no committed
        // index references, and appending over them is how they are
        // reclaimed.
        pager.set_page_count(pages);

        open.admit(id, Arc::clone(&pager));

        // Evicting after admitting, not before: the caller is about to
        // use this pager and still holds a reference to it, so the
        // strong-count test below excludes it from selection. Evicting
        // first would leave the table one over capacity instead.
        let active = self.catalog.with(|data| data.active_segment);

        open.evict_down_to(open_segment_capacity(), active)?;

        Ok(pager)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OpenSegments> {
        self.segments
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // -----------------------------------------------------------------
    // Append
    // -----------------------------------------------------------------

    /// Store `record` and return where it went.
    pub fn append(&self, record: &HeapRecord) -> Result<RecordLocation> {
        let plain = bincode::serialize(record).map_err(|e| {
            Error::new(ErrorKind::InvalidData, format!("failed to encode record: {e}"))
        })?;

        // The record frame from `storage::binary`, unchanged: magic,
        // format version, length and CRC. The payload is plaintext here
        // rather than separately encrypted, because the page it lands in
        // is encrypted as a whole (see `storage::pager`) — the frame's
        // job inside a page is per-record integrity and version
        // identification, which is exactly what it still does.
        let frame = binary::encode_frame(&plain)?;

        if frame.len() > MAX_CELL_LEN {
            return self.append_overflow(&frame);
        }

        let (segment, page, slot) = self.append_cell(&frame)?;

        Ok(RecordLocation {
            segment,
            page,
            slot,
            length: frame.len() as u32,
        })
    }

    /// Put one cell in the active segment, extending it by a page (and
    /// rolling to a new segment) as needed.
    fn append_cell(&self, cell: &[u8]) -> Result<(u32, u32, u16)> {
        let (segment, pages) = self.active_segment();

        self.append_cell_to(segment, pages, cell, true)
    }

    /// Put one cell in a named segment.
    ///
    /// `allow_roll` is what an overflow record needs: its stub has to
    /// land in the same segment as its chain, so that compacting that
    /// segment moves the whole record or none of it. Rolling to a fresh
    /// segment between the chain and the stub would split one record
    /// across two segment lifetimes, and retiring the chain's segment
    /// would leave a stub pointing at pages that no longer exist.
    fn append_cell_to(
        &self,
        segment: u32,
        pages: u32,
        cell: &[u8],
        allow_roll: bool,
    ) -> Result<(u32, u32, u16)> {
        if pages > 0 {
            let pager = self.pager(segment)?;
            let last = pages - 1;
            let mut page = (*pager.read(last)?).clone();

            if page.kind() == PageKind::Heap {
                if let Some(slot) = page.push_cell(cell) {
                    pager.write(last, page)?;
                    return Ok((segment, last, slot as u16));
                }
            }
        }

        let (segment, page_id, pager) = if allow_roll {
            self.grow(segment, pages)?
        } else {
            self.extend(segment, pages)?
        };

        let mut page = Page::new(PageKind::Heap);

        let slot = page.push_cell(cell).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "record of {} bytes does not fit in an empty page",
                    cell.len()
                ),
            )
        })?;

        pager.write(page_id, page)?;

        Ok((segment, page_id, slot as u16))
    }

    /// Retire the active segment and make a fresh, empty one active.
    ///
    /// Returns the new segment's id. It has no pages yet — the caller
    /// decides how many to claim, because the two callers want different
    /// things: the ordinary path takes exactly one page, and the
    /// overflow path takes as many as its chain needs.
    fn roll(&self) -> u32 {
        self.catalog.update(|data| {
            let id = data.next_segment;

            data.next_segment += 1;
            data.active_segment = id;
            data.segments.push(SegmentMeta { id, pages: 0, obsolete_bytes: 0 });

            id
        })
    }

    /// Add a page to the active segment, rolling to a fresh segment
    /// first if this one is full.
    fn grow(&self, segment: u32, pages: u32) -> Result<(u32, u32, Arc<Pager>)> {
        if pages >= MAX_PAGES_PER_SEGMENT {
            let fresh = self.roll();
            let pager = self.pager(fresh)?;

            self.set_pages(fresh, 1);

            return Ok((fresh, 0, pager));
        }

        self.extend(segment, pages)
    }

    /// Add a page to a segment, whatever its size. The segment cap is a
    /// target, and [`RecordStore::append_cell_to`] documents the one
    /// case that is allowed to exceed it.
    fn extend(&self, segment: u32, pages: u32) -> Result<(u32, u32, Arc<Pager>)> {
        let pager = self.pager(segment)?;
        self.set_pages(segment, pages + 1);

        Ok((segment, pages, pager))
    }

    /// Store a record too large for one page as a chain of overflow
    /// pages plus a stub cell that the index can address.
    ///
    /// The chain is written back to front so every page knows its
    /// successor at the moment it is written, and the whole record —
    /// chain and stub — stays inside one segment, so compacting that
    /// segment moves all of it or none of it.
    fn append_overflow(&self, frame: &[u8]) -> Result<RecordLocation> {
        let (active, active_pages) = self.active_segment();

        // The segment cap has to be applied *before* the first chunk is
        // written, because once the chain has begun the segment is
        // committed: a record is never split across segments, so there is
        // no legal place to roll after this point.
        //
        // Without this check the cap was enforced on exactly one path.
        // `grow` is the only caller that consults
        // `MAX_PAGES_PER_SEGMENT`, and nothing in this function reaches
        // it — the chain calls `extend` directly and the stub passes
        // `allow_roll = false`. So a workload made of oversized records
        // never rolled at all: one segment grew without bound, and
        // because `compaction_candidates` excludes whichever segment is
        // active, that same segment could never be compacted either. A
        // database of large records therefore reclaimed no space, ever.
        //
        // Oversized records are not exotic for the workloads this engine
        // is for — a post body, a JSON payload, a media manifest — so
        // this was the ordinary case on that path, not the edge one.
        let (segment, mut pages) = if active_pages >= MAX_PAGES_PER_SEGMENT {
            (self.roll(), 0)
        } else {
            (active, active_pages)
        };

        let pager = self.pager(segment)?;

        let mut next: u32 = 0;
        let mut chunks: Vec<&[u8]> = frame.chunks(MAX_CELL_LEN).collect();
        chunks.reverse();

        for chunk in chunks {
            let mut page = Page::new(PageKind::Overflow);
            page.set_extra(next);

            if page.push_cell(chunk).is_none() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "overflow chunk does not fit in an empty page",
                ));
            }

            // Deliberately grown without consulting the segment cap: a
            // single record is never split across segments, so one
            // oversized record is allowed to push its segment past the
            // target size rather than being torn in half by a roll.
            let page_id = pages;
            pages += 1;

            pager.write(page_id, page)?;
            self.set_pages(segment, pages);

            next = page_id;
        }

        let mut stub = Vec::with_capacity(OVERFLOW_STUB_LEN);
        stub.extend_from_slice(&OVERFLOW_MAGIC);
        stub.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        stub.extend_from_slice(&next.to_le_bytes());

        // Never rolls: the stub belongs with the chain it addresses.
        let (_, page, slot) = self.append_cell_to(segment, pages, &stub, false)?;

        Ok(RecordLocation {
            segment,
            page,
            slot,
            length: frame.len() as u32,
        })
    }

    fn active_segment(&self) -> (u32, u32) {
        self.catalog.with(|data| {
            let id = data.active_segment;

            let pages = data
                .segments
                .iter()
                .find(|s| s.id == id)
                .map(|s| s.pages)
                .unwrap_or(0);

            (id, pages)
        })
    }

    fn set_pages(&self, segment: u32, pages: u32) {
        self.catalog.update(|data| {
            if let Some(meta) = data.segments.iter_mut().find(|s| s.id == segment) {
                meta.pages = pages;
            }
        });
    }

    // -----------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------

    /// The record at `location`.
    pub fn read(&self, location: RecordLocation) -> Result<HeapRecord> {
        let bytes = self.read_frame(location)?;
        let payload = binary::decode_frame(&bytes)?;

        bincode::deserialize(payload).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "failed to decode the record at segment {} page {} slot {}: {e}",
                    location.segment, location.page, location.slot
                ),
            )
        })
    }

    /// The raw record frame at `location`, following an overflow chain
    /// when the cell is a stub.
    fn read_frame(&self, location: RecordLocation) -> Result<Vec<u8>> {
        let pager = self.pager(location.segment)?;
        let page = pager.read(location.page)?;

        let cell = page.cell(location.slot as usize).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "no record at segment {} page {} slot {}",
                    location.segment, location.page, location.slot
                ),
            )
        })?;

        if !is_overflow_stub(cell) {
            return Ok(cell.to_vec());
        }

        let total = u32::from_le_bytes(cell[4..8].try_into().expect("4 bytes")) as usize;
        let mut next = u32::from_le_bytes(cell[8..12].try_into().expect("4 bytes"));

        // The declared total is the one number here that has not been
        // verified by anything: it comes off a page as a bare u32 and it
        // is what sizes the reassembly buffer. Bound it against the
        // largest frame the writer will ever produce *before* allocating,
        // so a damaged stub is an integrity error rather than a 4 GiB
        // reservation.
        if total > binary::MAX_RECORD_FRAME_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "overflow stub at segment {} page {} slot {} declares a \
                     {total}-byte record, over the \
                     {}-byte maximum — the stub is corrupt",
                    location.segment,
                    location.page,
                    location.slot,
                    binary::MAX_RECORD_FRAME_LEN
                ),
            ));
        }

        // A chunk is at most MAX_CELL_LEN bytes, so a well-formed chain
        // for `total` bytes is at most this many links. Anything longer
        // is a cycle or a stub pointing into unrelated pages; either way
        // the walk stops instead of running until the machine gives up.
        let max_links = total.div_ceil(MAX_CELL_LEN).max(1);
        let mut links = 0usize;

        let mut frame = Vec::with_capacity(total);

        while frame.len() < total {
            links += 1;

            if links > max_links {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "overflow chain for segment {} page {} slot {} \
                         visited more than {max_links} pages without \
                         yielding {total} bytes — the chain is cyclic or \
                         corrupt",
                        location.segment, location.page, location.slot
                    ),
                ));
            }

            let chain = pager.read(next)?;

            if chain.kind() != PageKind::Overflow {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "overflow chain for segment {} page {} slot {} \
                         reached a non-overflow page",
                        location.segment, location.page, location.slot
                    ),
                ));
            }

            let chunk = chain.cell(0).ok_or_else(|| {
                Error::new(ErrorKind::InvalidData, "overflow page has no chunk")
            })?;

            frame.extend_from_slice(chunk);

            // `total` — not a sentinel page id — is what ends the walk.
            // The chain is written back to front, so its *last* link
            // carries a successor field of 0, and page 0 of a segment is
            // a perfectly ordinary page that the first overflow record
            // written to a fresh segment actually occupies. Treating 0 as
            // "end of chain" therefore stopped one link early for exactly
            // that record and made it permanently unreadable.
            next = chain.extra();
        }

        if frame.len() != total {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "overflow chain yielded {} bytes, expected {total}",
                    frame.len()
                ),
            ));
        }

        Ok(frame)
    }

    // -----------------------------------------------------------------
    // Space accounting and compaction
    // -----------------------------------------------------------------

    /// Record that the bytes at `location` are no longer referenced.
    ///
    /// A heuristic, not a liveness record: compaction decides what is
    /// live by asking the indexes, never by trusting this counter. It
    /// only decides *which segment is worth compacting*.
    pub fn mark_obsolete(&self, location: RecordLocation) {
        self.catalog.update(|data| {
            if let Some(meta) =
                data.segments.iter_mut().find(|s| s.id == location.segment)
            {
                meta.obsolete_bytes =
                    meta.obsolete_bytes.saturating_add(location.length as u64);
            }
        });
    }

    /// Segments worth draining: not the one being appended to, holding
    /// at least one page, and at least `ratio` dead by the counter
    /// above.
    pub fn compaction_candidates(&self, ratio: f64) -> Vec<u32> {
        self.catalog.with(|data| {
            data.segments
                .iter()
                .filter(|meta| meta.id != data.active_segment && meta.pages > 0)
                .filter(|meta| {
                    let capacity =
                        meta.pages as u64 * crate::storage::page::PAGE_BODY_LEN as u64;

                    capacity > 0
                        && meta.obsolete_bytes as f64 >= capacity as f64 * ratio
                })
                .map(|meta| meta.id)
                .collect()
        })
    }

    /// Visit every record stored in a segment, with the location that
    /// currently addresses it.
    ///
    /// Overflow pages are skipped: their chunks are not records, and the
    /// stub in an ordinary page is what addresses the record they hold.
    pub fn scan_segment<F>(&self, segment: u32, mut visit: F) -> Result<()>
    where
        F: FnMut(RecordLocation, HeapRecord) -> Result<()>,
    {
        let pager = self.pager(segment)?;
        let pages = pager.page_count();

        for page_id in 0..pages {
            let page = match pager.read(page_id) {
                Ok(page) => page,

                // A page that has never been written — a hole left by an
                // overflow chain an interrupted append never finished —
                // is not a record and not a reason to abandon the scan.
                // It shows up as the read running past the end of the
                // file, or past the page count the catalog vouches for.
                Err(e)
                    if matches!(
                        e.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::NotFound
                            | ErrorKind::InvalidInput
                    ) =>
                {
                    continue
                }

                // Anything else is a page that exists and did not
                // verify: a failed CRC, a failed AEAD tag, an impossible
                // slot directory. That must NOT be skipped.
                //
                // The only caller of this scan is compaction, and
                // compaction's contract is "everything live in this
                // segment has been copied elsewhere, so the segment may
                // now be deleted". Skipping an unreadable page silently
                // breaks that contract in the worst available direction:
                // the records in it are never migrated, the segment file
                // is removed anyway, and indexes are left pointing at
                // bytes that no longer exist. One damaged page becomes
                // permanent, unannounced loss of every record it held.
                //
                // Failing here instead aborts the compaction and the
                // checkpoint around it. Nothing is deleted, nothing is
                // lost, the WAL keeps the boundary where it was, and an
                // operator gets an error naming the page — which is a
                // recoverable position to be in.
                Err(e) => {
                    return Err(Error::new(
                        e.kind(),
                        format!(
                            "heap segment {segment} page {page_id} did not \
                             verify: {e}. Refusing to compact a segment that \
                             cannot be read in full — retiring it would \
                             discard whatever live records this page holds."
                        ),
                    ));
                }
            };

            if page.kind() != PageKind::Heap {
                continue;
            }

            for slot in 0..page.slot_count() {
                let Some(cell) = page.cell(slot) else { continue };

                let length = if is_overflow_stub(cell) {
                    u32::from_le_bytes(cell[4..8].try_into().expect("4 bytes"))
                } else {
                    cell.len() as u32
                };

                let location = RecordLocation {
                    segment,
                    page: page_id,
                    slot: slot as u16,
                    length,
                };

                let record = self.read(location)?;

                visit(location, record)?;
            }
        }

        Ok(())
    }

    /// Retire a drained segment: drop it from the catalog, make that
    /// durable, then delete the file.
    ///
    /// Called only after the index entries that used to point into it
    /// have been committed elsewhere. A crash before the file is deleted
    /// leaves an orphan on disk that nothing references and the next
    /// compaction pass ignores; a crash before the catalog is saved
    /// leaves the segment in place with no live records, which the next
    /// pass drains again for free.
    pub fn drop_segment(&self, segment: u32) -> Result<()> {
        self.catalog.update(|data| {
            data.segments.retain(|meta| meta.id != segment);
        });

        self.catalog.save()?;

        self.lock().forget(segment);

        match std::fs::remove_file(RecordStore::segment_path(segment)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Push every buffered page to stable storage, then the catalog that
    /// describes them.
    ///
    /// Order matters and is the reason this is one method: a catalog
    /// naming pages that are still only in memory would, after a crash,
    /// describe a segment longer than the records it actually holds.
    pub fn sync(&self) -> Result<()> {
        let pagers: Vec<Arc<Pager>> = self.lock().resident();

        for pager in pagers {
            pager.flush()?;
        }

        self.catalog.save()
    }
}

fn is_overflow_stub(cell: &[u8]) -> bool {
    cell.len() == OVERFLOW_STUB_LEN && cell[0..4] == OVERFLOW_MAGIC
}
