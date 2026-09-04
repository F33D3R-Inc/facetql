//! The physical page: the unit every durable structure in FacetQL is
//! built out of.
//!
//! Before this existed, "physical storage" meant an append-only file of
//! variable-length record frames and nothing else — there was no unit a
//! reader could seek to without having walked everything before it, and
//! no place to put a structure (an index node, a free list) that has to
//! be updated rather than appended. A page is that unit: a fixed-size,
//! self-describing, individually-verifiable block that can be read,
//! rewritten and addressed on its own.
//!
//! # Physical vs logical size
//!
//! A page occupies exactly [`PAGE_SIZE`] = 16 KiB on disk, always, so
//! page `n` starts at byte `n * PAGE_SIZE` in its file and nothing has
//! to be scanned to find it. Pages are encrypted at rest (see
//! `storage::pager`), and AES-256-GCM adds a 12-byte nonce and a 16-byte
//! authentication tag, so the *body* a page may fill is
//! [`PAGE_BODY_LEN`] = `PAGE_SIZE - 28` bytes. That is the only reason
//! the two constants differ: the physical boundary stays a clean 16 KiB
//! while the envelope is paid for out of the body rather than by
//! spilling every page across a boundary.
//!
//! # Layout of a body
//!
//! ```text
//!   offset  size  field
//!   ------  ----  ------------------------------------------------
//!   0       4     PAGE_MAGIC ("FQPG")
//!   4       1     page kind (meta / leaf / branch / heap)
//!   5       1     page format version
//!   6       2     slot count (u16)
//!   8       2     free_end — start of the cell area (u16)
//!   10      2     reserved
//!   12      4     extra (u32; per-kind: branch leftmost child, ...)
//!   16      4     CRC-32 of the body with these 4 bytes excluded
//!   20      4     reserved
//!   24      ...   slot directory: slot_count × (offset u16, len u16)
//!   ...           free space
//!   free_end..    cell area, cells allocated downward from the end
//! ```
//!
//! This is the classic slotted page. Two properties matter and are
//! relied on elsewhere:
//!
//! * **A slot index is a stable address.** `remove_cell` shifts the
//!   directory and therefore renumbers slots, so the heap (whose
//!   [`crate::storage::location::RecordLocation`] names a slot) never
//!   calls it. `compact` moves cell *bytes* but never renumbers slots,
//!   so it is safe everywhere.
//!
//! * **A page verifies itself.** Magic, version, CRC and the structural
//!   bounds of every slot are checked on decode, before any caller sees
//!   a cell. A page that fails is an error, never a page with plausible
//!   garbage in it.

use std::io::{Error, ErrorKind, Result};

use crate::storage::binary::crc32_parts;

/// The physical page size. Every page occupies exactly this many bytes
/// in its file, so page `n` lives at `n * PAGE_SIZE`.
pub const PAGE_SIZE: usize = 16 * 1024;

/// Bytes the at-rest encryption envelope costs: a 12-byte AES-GCM nonce
/// plus the 16-byte authentication tag.
pub const PAGE_ENVELOPE_LEN: usize = 12 + 16;

/// Bytes of a page a caller may actually use.
pub const PAGE_BODY_LEN: usize = PAGE_SIZE - PAGE_ENVELOPE_LEN;

/// Fixed header in front of the slot directory.
pub const PAGE_HEADER_LEN: usize = 24;

/// One slot directory entry: cell offset (u16) + cell length (u16).
pub const SLOT_LEN: usize = 4;

/// Largest single cell a page can hold, i.e. the largest key+value a
/// B-tree entry or record may be. A cell larger than this cannot be
/// made to fit by splitting a page, so it is rejected at the API
/// boundary rather than discovered halfway through an insert.
pub const MAX_CELL_LEN: usize = PAGE_BODY_LEN - PAGE_HEADER_LEN - SLOT_LEN;

const PAGE_MAGIC: [u8; 4] = *b"FQPG";
const PAGE_FORMAT_VERSION: u8 = 1;

const OFF_MAGIC: usize = 0;
const OFF_KIND: usize = 4;
const OFF_VERSION: usize = 5;
const OFF_SLOT_COUNT: usize = 6;
const OFF_FREE_END: usize = 8;
const OFF_EXTRA: usize = 12;
const OFF_CRC: usize = 16;

/// What a page holds. Stored in the header so a structural bug (a heap
/// page reached through a B-tree child pointer, say) is caught by the
/// page itself rather than by whatever nonsense its cells decode to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageKind {
    /// A tree's meta/superblock page. Two of these live at page 0 and 1
    /// of every index file; see `storage::btree`.
    Meta,
    /// A B-tree leaf: cells are `key_len u16 | key | value`.
    Leaf,
    /// A B-tree branch: cells are `key_len u16 | child u32 | key`, and
    /// `extra` is the leftmost child.
    Branch,
    /// A heap page: cells are record frames (see `storage::binary`).
    Heap,
    /// One link of an overflow chain, holding a slice of a record whose
    /// frame is larger than a page. Kept a distinct kind so a segment
    /// scan can tell "this page holds records" from "this page holds
    /// part of one", which is what lets compaction walk a segment
    /// without mistaking a chunk for a record.
    Overflow,
}

impl PageKind {
    fn to_byte(self) -> u8 {
        match self {
            PageKind::Meta => 1,
            PageKind::Leaf => 2,
            PageKind::Branch => 3,
            PageKind::Heap => 4,
            PageKind::Overflow => 5,
        }
    }

    fn from_byte(byte: u8) -> Option<PageKind> {
        match byte {
            1 => Some(PageKind::Meta),
            2 => Some(PageKind::Leaf),
            3 => Some(PageKind::Branch),
            4 => Some(PageKind::Heap),
            5 => Some(PageKind::Overflow),
            _ => None,
        }
    }
}

/// One page's body, in the exact layout it has on disk.
///
/// Held as raw bytes rather than as a parsed struct with a `Vec<Cell>`:
/// a page *is* a byte layout, and keeping it that way means writing one
/// out is a memcpy instead of a re-serialization, and that there is only
/// one representation of a page that can ever be wrong.
#[derive(Clone)]
pub struct Page {
    body: Vec<u8>,
}

impl Page {
    /// A new, empty page of `kind`.
    pub fn new(kind: PageKind) -> Page {
        let mut page = Page { body: vec![0u8; PAGE_BODY_LEN] };

        page.body[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&PAGE_MAGIC);
        page.body[OFF_KIND] = kind.to_byte();
        page.body[OFF_VERSION] = PAGE_FORMAT_VERSION;
        page.set_slot_count(0);
        page.set_free_end(PAGE_BODY_LEN);
        page.set_extra(0);

        page
    }

    pub fn kind(&self) -> PageKind {
        PageKind::from_byte(self.body[OFF_KIND])
            .expect("page kind validated on decode and set on construction")
    }

    /// The per-kind auxiliary word: a branch's leftmost child pointer,
    /// unused elsewhere.
    pub fn extra(&self) -> u32 {
        u32::from_le_bytes(
            self.body[OFF_EXTRA..OFF_EXTRA + 4]
                .try_into()
                .expect("4 bytes"),
        )
    }

    pub fn set_extra(&mut self, value: u32) {
        self.body[OFF_EXTRA..OFF_EXTRA + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn slot_count(&self) -> usize {
        u16::from_le_bytes(
            self.body[OFF_SLOT_COUNT..OFF_SLOT_COUNT + 2]
                .try_into()
                .expect("2 bytes"),
        ) as usize
    }

    fn set_slot_count(&mut self, count: usize) {
        self.body[OFF_SLOT_COUNT..OFF_SLOT_COUNT + 2]
            .copy_from_slice(&(count as u16).to_le_bytes());
    }

    fn free_end(&self) -> usize {
        u16::from_le_bytes(
            self.body[OFF_FREE_END..OFF_FREE_END + 2]
                .try_into()
                .expect("2 bytes"),
        ) as usize
    }

    fn set_free_end(&mut self, value: usize) {
        self.body[OFF_FREE_END..OFF_FREE_END + 2]
            .copy_from_slice(&(value as u16).to_le_bytes());
    }

    fn slot(&self, index: usize) -> (usize, usize) {
        let at = PAGE_HEADER_LEN + index * SLOT_LEN;

        let offset = u16::from_le_bytes(
            self.body[at..at + 2].try_into().expect("2 bytes"),
        ) as usize;

        let length = u16::from_le_bytes(
            self.body[at + 2..at + 4].try_into().expect("2 bytes"),
        ) as usize;

        (offset, length)
    }

    fn set_slot(&mut self, index: usize, offset: usize, length: usize) {
        let at = PAGE_HEADER_LEN + index * SLOT_LEN;

        self.body[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        self.body[at + 2..at + 4].copy_from_slice(&(length as u16).to_le_bytes());
    }

    /// The bytes of cell `index`, or `None` when the slot doesn't exist.
    pub fn cell(&self, index: usize) -> Option<&[u8]> {
        if index >= self.slot_count() {
            return None;
        }

        let (offset, length) = self.slot(index);

        Some(&self.body[offset..offset + length])
    }

    /// Contiguous free bytes: what an insert can use without compacting.
    pub fn free_space(&self) -> usize {
        let directory_end = PAGE_HEADER_LEN + self.slot_count() * SLOT_LEN;

        self.free_end().saturating_sub(directory_end)
    }

    /// Free bytes including the holes left by removed or replaced cells,
    /// i.e. what an insert could use *after* a [`Page::compact`].
    pub fn reclaimable_space(&self) -> usize {
        let used: usize = (0..self.slot_count()).map(|i| self.slot(i).1).sum();

        PAGE_BODY_LEN - PAGE_HEADER_LEN - self.slot_count() * SLOT_LEN - used
    }

    /// Rewrite the cell area with no holes, preserving slot numbering.
    ///
    /// Slot *indices* are untouched — only the offsets they point at
    /// move — which is what makes this safe to run on a heap page whose
    /// slots are part of a durable record address.
    pub fn compact(&mut self) {
        let count = self.slot_count();

        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(count);

        for index in 0..count {
            let (offset, length) = self.slot(index);
            cells.push(self.body[offset..offset + length].to_vec());
        }

        let mut free_end = PAGE_BODY_LEN;

        for (index, cell) in cells.iter().enumerate() {
            free_end -= cell.len();
            self.body[free_end..free_end + cell.len()].copy_from_slice(cell);
            self.set_slot(index, free_end, cell.len());
        }

        self.set_free_end(free_end);
    }

    /// Insert `data` as a new cell at slot `index`, shifting the slots
    /// at and after it up by one. `false` means the page is full — the
    /// caller (a B-tree insert) responds by splitting.
    pub fn insert_cell(&mut self, index: usize, data: &[u8]) -> bool {
        let count = self.slot_count();

        if index > count || data.len() > MAX_CELL_LEN {
            return false;
        }

        let needed = data.len() + SLOT_LEN;

        if self.free_space() < needed {
            if self.reclaimable_space() < needed {
                return false;
            }

            self.compact();
        }

        let directory_end = PAGE_HEADER_LEN + count * SLOT_LEN;

        // Shift the directory tail right by one slot to open a hole at
        // `index`. Done on the raw bytes rather than slot-by-slot so a
        // partially-shifted directory can never be observed.
        self.body.copy_within(
            PAGE_HEADER_LEN + index * SLOT_LEN..directory_end,
            PAGE_HEADER_LEN + (index + 1) * SLOT_LEN,
        );

        let free_end = self.free_end() - data.len();
        self.body[free_end..free_end + data.len()].copy_from_slice(data);

        self.set_free_end(free_end);
        self.set_slot_count(count + 1);
        self.set_slot(index, free_end, data.len());

        true
    }

    /// Append a cell after the last one. Returns its slot index, or
    /// `None` when the page is full.
    pub fn push_cell(&mut self, data: &[u8]) -> Option<usize> {
        let index = self.slot_count();

        if self.insert_cell(index, data) {
            Some(index)
        } else {
            None
        }
    }

    /// Replace cell `index` in place (logically: remove + reinsert at
    /// the same position), keeping every other slot's index.
    pub fn replace_cell(&mut self, index: usize, data: &[u8]) -> bool {
        if index >= self.slot_count() {
            return false;
        }

        let (_, old_length) = self.slot(index);

        // Same size: overwrite the bytes where they already are. This is
        // the common case for a primary-index update (a RecordLocation
        // is fixed width), and it keeps a hot key from fragmenting its
        // page on every write.
        if old_length == data.len() {
            let (offset, _) = self.slot(index);
            self.body[offset..offset + data.len()].copy_from_slice(data);
            return true;
        }

        // Different size: drop the old cell's slot, then reinsert at the
        // same index. The old bytes become a hole that `compact`
        // reclaims when the page next runs short.
        self.remove_cell(index);

        if self.insert_cell(index, data) {
            return true;
        }

        // Undo is impossible here, but so is the situation: the caller
        // (btree) only replaces after checking the size delta fits.
        false
    }

    /// Remove cell `index`, shifting every later slot down by one.
    ///
    /// **Renumbers slots**, so this must never be called on a heap page:
    /// a `RecordLocation` names a slot, and renumbering would silently
    /// repoint every location after `index` at the wrong record.
    pub fn remove_cell(&mut self, index: usize) {
        let count = self.slot_count();

        if index >= count {
            return;
        }

        let directory_end = PAGE_HEADER_LEN + count * SLOT_LEN;

        self.body.copy_within(
            PAGE_HEADER_LEN + (index + 1) * SLOT_LEN..directory_end,
            PAGE_HEADER_LEN + index * SLOT_LEN,
        );

        self.set_slot_count(count - 1);
    }

    /// The page's on-disk body, with its CRC brought up to date.
    pub fn encode(&mut self) -> &[u8] {
        let crc = self.compute_crc();
        self.body[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());

        &self.body
    }

    fn compute_crc(&self) -> u32 {
        // Everything except the CRC field itself, which cannot cover
        // its own value.
        crc32_parts(&[&self.body[..OFF_CRC], &self.body[OFF_CRC + 4..]])
    }

    /// Parse and fully validate a page body read back from disk.
    ///
    /// Every check here is the difference between "this page is
    /// damaged" and "this page decodes into plausible garbage that the
    /// B-tree then walks off the end of".
    pub fn decode(bytes: &[u8]) -> Result<Page> {
        if bytes.len() != PAGE_BODY_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "page body is {} bytes, expected {PAGE_BODY_LEN}",
                    bytes.len()
                ),
            ));
        }

        let page = Page { body: bytes.to_vec() };

        if page.body[OFF_MAGIC..OFF_MAGIC + 4] != PAGE_MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "page magic mismatch — not a FacetQL page",
            ));
        }

        if page.body[OFF_VERSION] != PAGE_FORMAT_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "page format version {} is not supported by this build \
                     (expected {PAGE_FORMAT_VERSION})",
                    page.body[OFF_VERSION]
                ),
            ));
        }

        if PageKind::from_byte(page.body[OFF_KIND]).is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown page kind {}", page.body[OFF_KIND]),
            ));
        }

        let stored = u32::from_le_bytes(
            page.body[OFF_CRC..OFF_CRC + 4].try_into().expect("4 bytes"),
        );

        if stored != page.compute_crc() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "page checksum mismatch — the page is corrupt",
            ));
        }

        // Structural bounds. A CRC-clean page can still be structurally
        // impossible if it was written by a buggy build, and every one
        // of these would otherwise become an out-of-bounds slice at read
        // time.
        let count = page.slot_count();
        let free_end = page.free_end();
        let directory_end = PAGE_HEADER_LEN + count * SLOT_LEN;

        if directory_end > free_end || free_end > PAGE_BODY_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "page directory/cell areas overlap: {count} slots, \
                     free_end {free_end}"
                ),
            ));
        }

        for index in 0..count {
            let (offset, length) = page.slot(index);

            if offset < free_end || offset + length > PAGE_BODY_LEN {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "page slot {index} points outside the cell area \
                         (offset {offset}, length {length})"
                    ),
                ));
            }
        }

        Ok(page)
    }
}
