//! A durable, crash-safe, ordered index: the B+tree every FacetQL
//! access path is built on.
//!
//! # Why this and not a serialized map
//!
//! An index that is a `HashMap` written out at shutdown is not a
//! database index. It has to be read in full before the first query, it
//! has to be held in full while the database is open, and a crash loses
//! whatever had not been written. This is the other thing: a paged
//! structure on disk that is *searched* on disk, faulting in only the
//! pages a lookup actually touches, with an atomic commit of its own.
//!
//! # Copy-on-write, and the two meta pages
//!
//! Pages 0 and 1 of an index file are meta pages; data pages start at 2.
//! A commit alternates between them, so the previous committed state is
//! always intact in the other slot while a new one is being written.
//! Open reads both and takes the valid one with the higher generation.
//! A meta write that is torn by a crash simply fails validation and the
//! previous generation is used — there is no window in which the file
//! describes a tree that does not exist.
//!
//! Within a generation, modifying a page **copies it to a newly
//! allocated page id** the first time (`cow` below) and then modifies
//! that copy in place for the rest of the generation. Two things follow,
//! and both are load-bearing:
//!
//! * **Nothing a committed meta references is ever overwritten.** So a
//!   crash mid-generation cannot damage the committed tree, however many
//!   pages had been written, and the buffer pool is free to evict dirty
//!   pages to disk whenever it likes (see `storage::pager`).
//!
//! * **Write amplification stays proportional to the pages a generation
//!   touches**, not to the number of operations in it. Copying on
//!   *every* write would rewrite a root-to-leaf path per key; copying
//!   once per generation means a hot page is copied once no matter how
//!   many keys land in it.
//!
//! # Page reclamation
//!
//! A page superseded by a copy is recorded in the meta's free list with
//! the generation that freed it, and becomes allocatable two
//! generations later — at which point no meta that could still be
//! recovered names it. The list is bounded ([`MAX_FREE_ENTRIES`]); a
//! generation that frees more pages than fit simply leaks the excess as
//! unreachable pages, which costs file size and never costs
//! correctness.
//!
//! # What this deliberately does not have
//!
//! No sibling pointers between leaves. Range scans re-descend for each
//! next key (`seek`), which is `O(log n)` per entry instead of `O(1)`.
//! Leaf chains and copy-on-write are a bad combination — every leaf
//! split or removal has to rewrite a neighbour that is nowhere on the
//! descent path — and the cost is small next to the record read each
//! scanned key leads to.

use std::collections::HashSet;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::sync::Mutex;

use crate::storage::page::{Page, PageKind, MAX_CELL_LEN, SLOT_LEN};
use crate::storage::pager::Pager;

/// Longest key the tree accepts. Long enough for an `owner` + `address`
/// composite; short enough that a page always holds several entries, so
/// a split always makes progress.
pub const MAX_KEY_LEN: usize = 1024;

/// Longest value the tree accepts. Index values here are record
/// locations and short markers, not records.
pub const MAX_VALUE_LEN: usize = 4096;

/// Free-list entries a meta page carries.
const MAX_FREE_ENTRIES: usize = 1024;

/// Page ids of the two alternating meta slots.
const META_A: u32 = 0;
const META_B: u32 = 1;

/// First page id available for tree data.
const FIRST_DATA_PAGE: u32 = 2;

const META_MAGIC: [u8; 4] = *b"FQBT";

/// Which direction, and whether the search key itself may be the
/// answer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SeekMode {
    /// First entry with `key >= target`.
    Ge,
    /// First entry with `key > target`.
    Gt,
    /// Last entry with `key <= target`.
    Le,
    /// Last entry with `key < target`.
    Lt,
}

struct FreeEntry {
    /// Generation during which the page was superseded.
    generation: u64,
    page: u32,
}

struct TreeState {
    /// Generation of the last committed meta.
    generation: u64,
    root: u32,
    entries: u64,
    /// Pages superseded but not yet reusable, with the generation that
    /// freed them.
    free: Vec<FreeEntry>,
    /// Pages already copied during the generation being built, which may
    /// therefore be modified in place.
    dirty: HashSet<u32>,
}

impl TreeState {
    /// The generation currently being built — one past the last
    /// committed one.
    fn building(&self) -> u64 {
        self.generation + 1
    }
}

pub struct BTree {
    pager: Pager,
    state: Mutex<TreeState>,
}

/// What an insert did to the subtree it was applied to.
enum Insert {
    /// The subtree still has one root, now at this (possibly new) page.
    Kept(u32),
    /// The subtree split. The left half is at the first page, the right
    /// half at the second, and the separator key is the smallest key in
    /// the right half.
    Split(u32, Vec<u8>, u32),
}

/// What a removal did to the subtree it was applied to.
struct Removal {
    page: u32,
    removed: bool,
    /// The subtree holds nothing at all any more and its page has been
    /// freed by the caller's parent (or, at the root, kept as an empty
    /// leaf).
    empty: bool,
}

impl BTree {
    /// Open the index at `path`, creating an empty tree if it is new.
    pub fn open(path: &Path) -> Result<BTree> {
        let pager = Pager::open(path)?;

        if pager.page_count() < FIRST_DATA_PAGE {
            return BTree::initialize(pager);
        }

        let a = BTree::read_meta(&pager, META_A);
        let b = BTree::read_meta(&pager, META_B);

        let state = match (a, b) {
            (Some(a), Some(b)) => {
                if a.generation >= b.generation {
                    a
                } else {
                    b
                }
            }
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "index {} has no readable meta page — both \
                         generations failed validation. The index can be \
                         rebuilt from the record heap, which is the \
                         authoritative copy.",
                        path.display()
                    ),
                ));
            }
        };

        // Pages past the committed high-water mark belong to a
        // generation that never committed. Resetting the allocator to
        // the meta's high-water mark hands them back rather than leaving
        // them stranded past the end of a tree that never referenced
        // them.
        pager.set_page_count(state.high_water);

        Ok(BTree {
            pager,
            state: Mutex::new(TreeState {
                generation: state.generation,
                root: state.root,
                entries: state.entries,
                free: state.free,
                dirty: HashSet::new(),
            }),
        })
    }

    fn initialize(pager: Pager) -> Result<BTree> {
        let root = FIRST_DATA_PAGE;

        pager.set_page_count(0);

        // Reserve both meta slots and the root leaf before anything is
        // written, so page ids line up with the layout above.
        let _ = pager.allocate();
        let _ = pager.allocate();
        let allocated_root = pager.allocate();

        debug_assert_eq!(allocated_root, root);

        pager.write(root, Page::new(PageKind::Leaf))?;

        let tree = BTree {
            pager,
            state: Mutex::new(TreeState {
                generation: 0,
                root,
                entries: 0,
                free: Vec::new(),
                dirty: HashSet::new(),
            }),
        };

        // Generation 1 in slot 1; slot 0 stays unwritten and simply
        // fails validation on the next open, which the two-slot rule
        // already handles.
        tree.commit()?;

        Ok(tree)
    }

    // -----------------------------------------------------------------
    // Meta
    // -----------------------------------------------------------------

    fn read_meta(pager: &Pager, slot: u32) -> Option<MetaState> {
        let page = pager.read(slot).ok()?;

        if page.kind() != PageKind::Meta {
            return None;
        }

        MetaState::decode(page.cell(0)?)
    }

    /// Publish everything written since the last commit.
    ///
    /// Order is the whole point: every data page is on stable storage
    /// *before* the meta that names it. A crash between the two loses
    /// the generation and keeps the previous one, which is exactly the
    /// intended outcome — the WAL still holds the operations, and
    /// recovery replays them because the durability checkpoint has not
    /// moved either.
    pub fn commit(&self) -> Result<()> {
        let mut state = self.lock();

        // Step 1: data pages durable.
        self.pager.flush()?;

        let generation = state.building();

        let meta = MetaState {
            generation,
            root: state.root,
            entries: state.entries,
            high_water: self.pager.page_count(),
            free: std::mem::take(&mut state.free),
        };

        let mut page = Page::new(PageKind::Meta);

        if page.push_cell(&meta.encode()).is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                "index meta does not fit in one page",
            ));
        }

        let slot = if generation % 2 == 0 { META_A } else { META_B };

        // Step 2: the meta, then fsync. This is the atomic commit point.
        self.pager.write(slot, page)?;
        self.pager.flush()?;

        state.generation = generation;
        state.free = meta.free;
        state.dirty.clear();

        Ok(())
    }

    // -----------------------------------------------------------------
    // Page allocation
    // -----------------------------------------------------------------

    /// A page id safe to write.
    ///
    /// Prefers a page freed at least two generations ago — by then no
    /// meta that could still be recovered references it — and grows the
    /// file only when there is none.
    fn allocate(&self, state: &mut TreeState) -> u32 {
        let building = state.building();

        if let Some(index) = state
            .free
            .iter()
            .position(|entry| entry.generation + 2 <= building)
        {
            return state.free.swap_remove(index).page;
        }

        self.pager.allocate()
    }

    /// Record a superseded page for later reuse.
    fn release(&self, state: &mut TreeState, page: u32) {
        if state.free.len() >= MAX_FREE_ENTRIES {
            // Bounded on purpose: the alternative is an unbounded
            // structure in the one page that has to stay small. A leaked
            // page costs file size, never correctness.
            return;
        }

        let generation = state.building();

        state.free.push(FreeEntry { generation, page });
    }

    /// Get a writable copy of `page_id`, returning the id to use for it.
    ///
    /// The first touch in a generation copies to a fresh page and
    /// releases the original; later touches modify the copy in place.
    fn cow(&self, state: &mut TreeState, page_id: u32) -> Result<(u32, Page)> {
        let page = (*self.pager.read(page_id)?).clone();

        if state.dirty.contains(&page_id) {
            return Ok((page_id, page));
        }

        let fresh = self.allocate(state);

        state.dirty.insert(fresh);
        self.release(state, page_id);

        Ok((fresh, page))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TreeState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // -----------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------

    /// Number of entries in the tree.
    pub fn len(&self) -> u64 {
        self.lock().entries
    }

    /// The value stored under `key`, if any.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root = self.lock().root;

        let leaf = self.descend(root, key)?;
        let page = self.pager.read(leaf)?;

        match leaf_search(&page, key) {
            (index, true) => Ok(page
                .cell(index)
                .map(|cell| leaf_value(cell).to_vec())),
            (_, false) => Ok(None),
        }
    }

    /// The leaf page a key belongs in.
    fn descend(&self, mut page_id: u32, key: &[u8]) -> Result<u32> {
        loop {
            let page = self.pager.read(page_id)?;

            match page.kind() {
                PageKind::Leaf => return Ok(page_id),
                PageKind::Branch => {
                    page_id = branch_child_for(&page, key).1;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("tree descent reached a {other:?} page"),
                    ));
                }
            }
        }
    }

    /// The entry nearest `key` in the requested direction, or `None`
    /// when the tree has none.
    pub fn seek(
        &self,
        key: &[u8],
        mode: SeekMode,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.lock().root;

        // The descent path, as (page id, which child was taken), where
        // child 0 is the leftmost pointer and child k+1 is the k-th
        // cell's.
        let mut path: Vec<(u32, usize)> = Vec::new();
        let mut page_id = root;

        let leaf = loop {
            let page = self.pager.read(page_id)?;

            match page.kind() {
                PageKind::Leaf => break page,
                PageKind::Branch => {
                    let (slot, child) = branch_child_for(&page, key);
                    path.push((page_id, slot));
                    page_id = child;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("tree seek reached a {other:?} page"),
                    ));
                }
            }
        };

        let (index, found) = leaf_search(&leaf, key);

        let forward = matches!(mode, SeekMode::Ge | SeekMode::Gt);

        let candidate = if forward {
            // `index` is the first entry >= key; skip it for Gt when it
            // is the key itself.
            let start = if found && mode == SeekMode::Gt { index + 1 } else { index };

            (start < leaf.slot_count()).then_some(start)
        } else {
            // `index` is the first entry >= key, so the last entry
            // <= key is at `index` when it matched and `index - 1`
            // otherwise.
            let end = if found && mode == SeekMode::Le {
                Some(index)
            } else if index > 0 {
                Some(index - 1)
            } else {
                None
            };

            end
        };

        if let Some(slot) = candidate {
            if let Some(cell) = leaf.cell(slot) {
                return Ok(Some((
                    leaf_key(cell).to_vec(),
                    leaf_value(cell).to_vec(),
                )));
            }
        }

        // The answer is not in this leaf: walk back up the path and take
        // the neighbouring subtree in the direction of travel.
        while let Some((branch_id, slot)) = path.pop() {
            let branch = self.pager.read(branch_id)?;

            if forward {
                if slot < branch.slot_count() {
                    let child = branch_child_at(&branch, slot + 1);

                    if let Some(entry) = self.edge_entry(child, true)? {
                        return Ok(Some(entry));
                    }
                }
            } else if slot > 0 {
                let child = branch_child_at(&branch, slot - 1);

                if let Some(entry) = self.edge_entry(child, false)? {
                    return Ok(Some(entry));
                }
            }
        }

        Ok(None)
    }

    /// The first (or last) entry anywhere in the subtree rooted at
    /// `page_id`.
    ///
    /// Tolerates empty pages by moving on to the next child rather than
    /// concluding the subtree is empty, so a tree that has had entries
    /// removed still scans correctly.
    fn edge_entry(
        &self,
        page_id: u32,
        first: bool,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let page = self.pager.read(page_id)?;

        match page.kind() {
            PageKind::Leaf => {
                let count = page.slot_count();

                if count == 0 {
                    return Ok(None);
                }

                let slot = if first { 0 } else { count - 1 };

                Ok(page.cell(slot).map(|cell| {
                    (leaf_key(cell).to_vec(), leaf_value(cell).to_vec())
                }))
            }

            PageKind::Branch => {
                let children = page.slot_count() + 1;

                for step in 0..children {
                    let slot = if first { step } else { children - 1 - step };
                    let child = branch_child_at(&page, slot);

                    if let Some(entry) = self.edge_entry(child, first)? {
                        return Ok(Some(entry));
                    }
                }

                Ok(None)
            }

            other => Err(Error::new(
                ErrorKind::InvalidData,
                format!("tree walk reached a {other:?} page"),
            )),
        }
    }

    /// Visit every entry whose key starts with `prefix`, in key order
    /// (or reversed), optionally resuming strictly past `start_after`.
    ///
    /// `visit` returns `false` to stop early, which is what keeps a
    /// `LIMIT`ed query from reading a whole index.
    pub fn for_each_range<F>(
        &self,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        reverse: bool,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut cursor: Option<Vec<u8>> = start_after.map(<[u8]>::to_vec);

        loop {
            let entry = if reverse {
                match &cursor {
                    Some(key) => self.seek(key, SeekMode::Lt)?,
                    None => match prefix_upper_bound(prefix) {
                        Some(bound) => self.seek(&bound, SeekMode::Lt)?,
                        // No upper bound means the prefix is empty (or
                        // all 0xff): the last entry in the tree is the
                        // starting point.
                        None => {
                            let root = self.lock().root;
                            self.edge_entry(root, false)?
                        }
                    },
                }
            } else {
                match &cursor {
                    Some(key) => self.seek(key, SeekMode::Gt)?,
                    None => self.seek(prefix, SeekMode::Ge)?,
                }
            };

            let Some((key, value)) = entry else {
                return Ok(());
            };

            if !key.starts_with(prefix) {
                return Ok(());
            }

            if !visit(&key, &value)? {
                return Ok(());
            }

            cursor = Some(key);
        }
    }

    // -----------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------

    /// Insert or replace `key`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "index key is {} bytes; must be 1..={MAX_KEY_LEN}",
                    key.len()
                ),
            ));
        }

        if value.len() > MAX_VALUE_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "index value is {} bytes; must be at most {MAX_VALUE_LEN}",
                    value.len()
                ),
            ));
        }

        let existed = self.get(key)?.is_some();

        let mut state = self.lock();
        let root = state.root;

        match self.insert_into(&mut state, root, key, value)? {
            Insert::Kept(page) => state.root = page,

            Insert::Split(left, separator, right) => {
                let mut branch = Page::new(PageKind::Branch);
                branch.set_extra(left);

                if !branch.insert_cell(0, &branch_cell(&separator, right)) {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "separator key does not fit in a fresh root page",
                    ));
                }

                let id = self.allocate(&mut state);
                state.dirty.insert(id);
                self.pager.write(id, branch)?;

                state.root = id;
            }
        }

        if !existed {
            state.entries += 1;
        }

        Ok(())
    }

    fn insert_into(
        &self,
        state: &mut TreeState,
        page_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<Insert> {
        let kind = self.pager.read(page_id)?.kind();

        match kind {
            PageKind::Leaf => self.insert_into_leaf(state, page_id, key, value),
            PageKind::Branch => self.insert_into_branch(state, page_id, key, value),
            other => Err(Error::new(
                ErrorKind::InvalidData,
                format!("tree insert reached a {other:?} page"),
            )),
        }
    }

    fn insert_into_leaf(
        &self,
        state: &mut TreeState,
        page_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<Insert> {
        let (id, mut page) = self.cow(state, page_id)?;
        let cell = leaf_cell(key, value);
        let (index, found) = leaf_search(&page, key);

        // The in-page fast path. Both arms check capacity *before*
        // mutating, so a page that cannot take the entry is left exactly
        // as it was for the split path to rebuild from.
        if found {
            let old_len = page.cell(index).map(<[u8]>::len).unwrap_or(0);

            if page.reclaimable_space() + old_len >= cell.len() {
                page.replace_cell(index, &cell);
                self.pager.write(id, page)?;
                return Ok(Insert::Kept(id));
            }
        } else if page.reclaimable_space() >= cell.len() + SLOT_LEN {
            page.insert_cell(index, &cell);
            self.pager.write(id, page)?;
            return Ok(Insert::Kept(id));
        }

        // Split. Rebuilding from the cell list rather than surgically
        // moving cells between two live pages keeps the failure mode
        // simple: either both halves are built and written, or nothing
        // changed.
        let mut cells = page_cells(&page);

        if found {
            cells[index] = cell;
        } else {
            cells.insert(index, cell);
        }

        let point = split_point(&cells).ok_or_else(|| {
            Error::new(ErrorKind::Other, "leaf page cannot be split")
        })?;

        let separator = leaf_key(&cells[point]).to_vec();

        let left = build_page(PageKind::Leaf, 0, &cells[..point])?;
        let right = build_page(PageKind::Leaf, 0, &cells[point..])?;

        let right_id = self.allocate(state);
        state.dirty.insert(right_id);

        self.pager.write(id, left)?;
        self.pager.write(right_id, right)?;

        Ok(Insert::Split(id, separator, right_id))
    }

    fn insert_into_branch(
        &self,
        state: &mut TreeState,
        page_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<Insert> {
        let page = self.pager.read(page_id)?;
        let (slot, child) = branch_child_for(&page, key);
        drop(page);

        match self.insert_into(state, child, key, value)? {
            Insert::Kept(new_child) => {
                if new_child == child {
                    // The child was already dirty in this generation, so
                    // it kept its page id and this branch does not need
                    // rewriting at all.
                    return Ok(Insert::Kept(page_id));
                }

                let (id, mut page) = self.cow(state, page_id)?;
                set_branch_child(&mut page, slot, new_child);
                self.pager.write(id, page)?;

                Ok(Insert::Kept(id))
            }

            Insert::Split(left, separator, right) => {
                let (id, mut page) = self.cow(state, page_id)?;

                set_branch_child(&mut page, slot, left);

                let cell = branch_cell(&separator, right);

                if page.reclaimable_space() >= cell.len() + SLOT_LEN {
                    page.insert_cell(slot, &cell);
                    self.pager.write(id, page)?;
                    return Ok(Insert::Kept(id));
                }

                // The branch itself overflows: rebuild it as two
                // branches and promote a separator one level further up.
                let mut cells = page_cells(&page);
                cells.insert(slot, cell);

                let leftmost = page.extra();

                let point = split_point(&cells).ok_or_else(|| {
                    Error::new(ErrorKind::Other, "branch page cannot be split")
                })?;

                // The separator at the split point moves up rather than
                // staying in either half — its child becomes the right
                // half's leftmost pointer.
                let promoted = branch_key(&cells[point]).to_vec();
                let right_leftmost = branch_child(&cells[point]);

                let left_page =
                    build_page(PageKind::Branch, leftmost, &cells[..point])?;
                let right_page = build_page(
                    PageKind::Branch,
                    right_leftmost,
                    &cells[point + 1..],
                )?;

                let right_id = self.allocate(state);
                state.dirty.insert(right_id);

                self.pager.write(id, left_page)?;
                self.pager.write(right_id, right_page)?;

                Ok(Insert::Split(id, promoted, right_id))
            }
        }
    }

    /// Remove `key`. Returns whether it was there.
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let mut state = self.lock();
        let root = state.root;

        let outcome = self.remove_from(&mut state, root, key)?;

        if !outcome.removed {
            return Ok(false);
        }

        state.root = outcome.page;
        state.entries = state.entries.saturating_sub(1);

        // A root branch that has lost every separator is one indirection
        // with no information in it; drop it so the tree does not grow a
        // permanent spine of single-child branches.
        loop {
            let root_id = state.root;
            let page = self.pager.read(root_id)?;

            if page.kind() != PageKind::Branch || page.slot_count() > 0 {
                break;
            }

            let child = page.extra();
            drop(page);

            self.release(&mut state, root_id);
            state.root = child;
        }

        Ok(true)
    }

    fn remove_from(
        &self,
        state: &mut TreeState,
        page_id: u32,
        key: &[u8],
    ) -> Result<Removal> {
        let page = self.pager.read(page_id)?;

        match page.kind() {
            PageKind::Leaf => {
                let (index, found) = leaf_search(&page, key);

                if !found {
                    return Ok(Removal { page: page_id, removed: false, empty: false });
                }

                drop(page);

                let (id, mut page) = self.cow(state, page_id)?;
                page.remove_cell(index);

                let empty = page.slot_count() == 0;

                self.pager.write(id, page)?;

                Ok(Removal { page: id, removed: true, empty })
            }

            PageKind::Branch => {
                let (slot, child) = branch_child_for(&page, key);
                drop(page);

                let outcome = self.remove_from(state, child, key)?;

                if !outcome.removed {
                    return Ok(Removal { page: page_id, removed: false, empty: false });
                }

                let (id, mut page) = self.cow(state, page_id)?;

                if outcome.empty {
                    // The child holds nothing: drop the pointer to it
                    // instead of keeping an empty page reachable
                    // forever. Removing the *leftmost* pointer promotes
                    // the next child into its place, which is safe
                    // because routing only ever compares against the
                    // separators that remain.
                    self.release(state, outcome.page);

                    if slot == 0 {
                        if page.slot_count() == 0 {
                            self.pager.write(id, page)?;
                            return Ok(Removal { page: id, removed: true, empty: true });
                        }

                        let promoted = branch_child_at(&page, 1);
                        page.set_extra(promoted);
                        page.remove_cell(0);
                    } else {
                        page.remove_cell(slot - 1);
                    }
                } else {
                    set_branch_child(&mut page, slot, outcome.page);
                }

                self.pager.write(id, page)?;

                Ok(Removal { page: id, removed: true, empty: false })
            }

            other => Err(Error::new(
                ErrorKind::InvalidData,
                format!("tree remove reached a {other:?} page"),
            )),
        }
    }
}

// ---------------------------------------------------------------------
// Meta encoding
// ---------------------------------------------------------------------

struct MetaState {
    generation: u64,
    root: u32,
    entries: u64,
    high_water: u32,
    free: Vec<FreeEntry>,
}

impl MetaState {
    fn encode(&self) -> Vec<u8> {
        let count = self.free.len().min(MAX_FREE_ENTRIES);

        let mut out = Vec::with_capacity(4 + 8 + 4 + 8 + 4 + 2 + count * 12);

        out.extend_from_slice(&META_MAGIC);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.root.to_le_bytes());
        out.extend_from_slice(&self.entries.to_le_bytes());
        out.extend_from_slice(&self.high_water.to_le_bytes());
        out.extend_from_slice(&(count as u16).to_le_bytes());

        for entry in self.free.iter().take(count) {
            out.extend_from_slice(&entry.generation.to_le_bytes());
            out.extend_from_slice(&entry.page.to_le_bytes());
        }

        out
    }

    fn decode(bytes: &[u8]) -> Option<MetaState> {
        const FIXED: usize = 4 + 8 + 4 + 8 + 4 + 2;

        if bytes.len() < FIXED || bytes[0..4] != META_MAGIC {
            return None;
        }

        let generation = u64::from_le_bytes(bytes[4..12].try_into().ok()?);
        let root = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
        let entries = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let high_water = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        let count = u16::from_le_bytes(bytes[28..30].try_into().ok()?) as usize;

        if bytes.len() < FIXED + count * 12 {
            return None;
        }

        let mut free = Vec::with_capacity(count);

        for index in 0..count {
            let at = FIXED + index * 12;

            free.push(FreeEntry {
                generation: u64::from_le_bytes(bytes[at..at + 8].try_into().ok()?),
                page: u32::from_le_bytes(bytes[at + 8..at + 12].try_into().ok()?),
            });
        }

        Some(MetaState { generation, root, entries, high_water, free })
    }
}

// ---------------------------------------------------------------------
// Cell encoding
//
// Leaf:   key_len u16 | key | value
// Branch: key_len u16 | child u32 | key
//
// The accessors below are lenient about a malformed cell (they return
// an empty key rather than an error) because a cell can only be
// malformed if this module wrote it wrong: every page is checked for
// magic, version, CRC and slot bounds on decode, and every page is
// additionally authenticated by AES-GCM. Corruption is caught a layer
// down; a bad cell here would be a bug, and returning an ordering-
// neutral key keeps that bug from becoming a panic in a query path.
// ---------------------------------------------------------------------

fn leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(2 + key.len() + value.len());

    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(value);

    cell
}

fn leaf_key(cell: &[u8]) -> &[u8] {
    if cell.len() < 2 {
        return &[];
    }

    let len = u16::from_le_bytes([cell[0], cell[1]]) as usize;

    if cell.len() < 2 + len {
        return &[];
    }

    &cell[2..2 + len]
}

fn leaf_value(cell: &[u8]) -> &[u8] {
    if cell.len() < 2 {
        return &[];
    }

    let len = u16::from_le_bytes([cell[0], cell[1]]) as usize;

    if cell.len() < 2 + len {
        return &[];
    }

    &cell[2 + len..]
}

fn branch_cell(key: &[u8], child: u32) -> Vec<u8> {
    let mut cell = Vec::with_capacity(6 + key.len());

    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(&child.to_le_bytes());
    cell.extend_from_slice(key);

    cell
}

fn branch_key(cell: &[u8]) -> &[u8] {
    if cell.len() < 6 {
        return &[];
    }

    let len = u16::from_le_bytes([cell[0], cell[1]]) as usize;

    if cell.len() < 6 + len {
        return &[];
    }

    &cell[6..6 + len]
}

fn branch_child(cell: &[u8]) -> u32 {
    if cell.len() < 6 {
        return 0;
    }

    u32::from_le_bytes([cell[2], cell[3], cell[4], cell[5]])
}

/// Which child of a branch a key routes to, as `(slot, child)` where
/// slot 0 is the leftmost pointer and slot k+1 is cell k's.
fn branch_child_for(page: &Page, key: &[u8]) -> (usize, u32) {
    let mut slot = 0;

    for index in 0..page.slot_count() {
        let Some(cell) = page.cell(index) else { break };

        if branch_key(cell) <= key {
            slot = index + 1;
        } else {
            break;
        }
    }

    (slot, branch_child_at(page, slot))
}

fn branch_child_at(page: &Page, slot: usize) -> u32 {
    if slot == 0 {
        return page.extra();
    }

    page.cell(slot - 1).map(branch_child).unwrap_or(0)
}

fn set_branch_child(page: &mut Page, slot: usize, child: u32) {
    if slot == 0 {
        page.set_extra(child);
        return;
    }

    let Some(cell) = page.cell(slot - 1) else { return };

    let updated = branch_cell(branch_key(cell), child);

    page.replace_cell(slot - 1, &updated);
}

/// Position of `key` among a leaf's cells, and whether it is an exact
/// match. On a miss, the position is where the key would be inserted.
fn leaf_search(page: &Page, key: &[u8]) -> (usize, bool) {
    let mut low = 0usize;
    let mut high = page.slot_count();

    while low < high {
        let mid = (low + high) / 2;

        let Some(cell) = page.cell(mid) else { break };

        match leaf_key(cell).cmp(key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return (mid, true),
        }
    }

    (low, false)
}

fn page_cells(page: &Page) -> Vec<Vec<u8>> {
    (0..page.slot_count())
        .filter_map(|index| page.cell(index).map(<[u8]>::to_vec))
        .collect()
}

/// Where to cut an overfull cell list so both halves fit in a page.
///
/// Starts from the balanced midpoint and walks outward, so the common
/// case is an even split and the pathological case (a few very large
/// cells) still finds a cut rather than failing.
fn split_point(cells: &[Vec<u8>]) -> Option<usize> {
    if cells.len() < 2 {
        return None;
    }

    let total: usize = cells.iter().map(|cell| cell.len() + SLOT_LEN).sum();

    let mut midpoint = 1;
    let mut running = 0;

    for (index, cell) in cells.iter().enumerate() {
        running += cell.len() + SLOT_LEN;

        if running * 2 >= total {
            midpoint = index.max(1);
            break;
        }
    }

    for offset in 0..cells.len() {
        for candidate in [midpoint + offset, midpoint.saturating_sub(offset)] {
            if candidate == 0 || candidate >= cells.len() {
                continue;
            }

            if fits(&cells[..candidate]) && fits(&cells[candidate..]) {
                return Some(candidate);
            }
        }
    }

    None
}

fn fits(cells: &[Vec<u8>]) -> bool {
    let used: usize = cells.iter().map(|cell| cell.len() + SLOT_LEN).sum();

    used <= MAX_CELL_LEN + SLOT_LEN
}

fn build_page(kind: PageKind, extra: u32, cells: &[Vec<u8>]) -> Result<Page> {
    let mut page = Page::new(kind);
    page.set_extra(extra);

    for cell in cells {
        if page.push_cell(cell).is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                "rebuilt page overflowed while being filled",
            ));
        }
    }

    Ok(page)
}

/// The first key that is *past* every key starting with `prefix`, used
/// to start a reverse scan. `None` when there is no such key — an empty
/// prefix, or one that is all `0xff`.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();

    while let Some(last) = bound.pop() {
        if last != 0xff {
            bound.push(last + 1);
            return Some(bound);
        }
    }

    None
}
