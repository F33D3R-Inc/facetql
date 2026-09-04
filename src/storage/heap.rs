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

pub struct RecordStore {
    catalog: Arc<Catalog>,
    /// Open segment files. Populated on demand — a database with many
    /// segments does not open them all to answer one point read.
    segments: Mutex<BTreeMap<u32, Arc<Pager>>>,
}

impl RecordStore {
    pub fn open(catalog: Arc<Catalog>) -> RecordStore {
        RecordStore {
            catalog,
            segments: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn segment_path(id: u32) -> std::path::PathBuf {
        config::data_file(&format!("facetql.heap.{id:06}.seg"))
    }

    /// The pager for a segment, opening the file if this is its first
    /// use.
    fn pager(&self, id: u32) -> Result<Arc<Pager>> {
        let mut open = self.lock();

        if let Some(pager) = open.get(&id) {
            return Ok(Arc::clone(pager));
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

        open.insert(id, Arc::clone(&pager));

        Ok(pager)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, Arc<Pager>>> {
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

    /// Add a page to the active segment, rolling to a fresh segment
    /// first if this one is full.
    fn grow(&self, segment: u32, pages: u32) -> Result<(u32, u32, Arc<Pager>)> {
        if pages >= MAX_PAGES_PER_SEGMENT {
            let fresh = self.catalog.update(|data| {
                let id = data.next_segment;

                data.next_segment += 1;
                data.active_segment = id;
                data.segments.push(SegmentMeta { id, pages: 0, obsolete_bytes: 0 });

                id
            });

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
        let (segment, mut pages) = self.active_segment();
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

        let mut frame = Vec::with_capacity(total);

        while frame.len() < total {
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
            next = chain.extra();

            if next == 0 {
                break;
            }
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
                // A page that has never been written (a hole left by an
                // overflow chain that was interrupted) is not a record
                // and not a reason to abandon the scan.
                Err(_) => continue,
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

        self.lock().remove(&segment);

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
        let pagers: Vec<Arc<Pager>> = self.lock().values().map(Arc::clone).collect();

        for pager in pagers {
            pager.flush()?;
        }

        self.catalog.save()
    }
}

fn is_overflow_stub(cell: &[u8]) -> bool {
    cell.len() == OVERFLOW_STUB_LEN && cell[0..4] == OVERFLOW_MAGIC
}
