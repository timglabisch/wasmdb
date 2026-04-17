use crate::dirty_slot_id::DirtySlotId;
use crate::iter::Iter;

/// Single-threaded dirty-set: writers `mark_dirty(id)`, reader walks
/// [`DirtySet::iter`] and then calls [`DirtySet::clear`] to drop the batch.
///
/// # Storage model
///
/// Two tiers:
///
/// - A fast-path **ring list** of size `LIST_CAP` holding the first
///   `LIST_CAP` marks in insertion order. Duplicates in the list path are
///   NOT deduplicated — if the integration cares, it must dedupe on read.
/// - An **overflow bitmap** of `ceil(n_subs / 32)` words. Once the list
///   fills, further marks flip the `overflowed` flag and set the bit in the
///   bitmap. Bitmap writes are idempotent: marking the same id twice in the
///   bitmap path is a no-op.
///
/// # Read / clear separation
///
/// `iter()` only borrows `&self` and does not touch internal state. The
/// batch persists until `clear()` is called, so iteration is safe to drop
/// mid-way and `iter()` can be called multiple times on the same batch.
///
/// # Single-threaded only
///
/// All mutating methods take `&mut self`. There is no atomic backing, no
/// shared memory, no concurrent writer or reader.
pub struct DirtySet<const LIST_CAP: usize> {
    /// Number of valid entries in `list`. When `head == LIST_CAP` the list
    /// is full; any further `mark_dirty` goes to the bitmap.
    head: u32,
    /// Ring list of dirty DirtySlotIds in insertion order. Only
    /// `list[..head as usize]` is valid.
    list: [u32; LIST_CAP],
    /// Overflow bitmap. Length is `ceil(n_subs / 32)`; bit `id` lives in
    /// `bitmap[id / 32] & (1 << (id % 32))`.
    bitmap: Box<[u32]>,
    /// Set the first time a mark in the current batch falls through to the
    /// bitmap. Reader consults this to decide whether to scan the bitmap.
    overflowed: bool,
    /// Maximum valid DirtySlotId (exclusive). Used only for debug_asserts.
    n_subs: u32,
}

impl<const LIST_CAP: usize> DirtySet<LIST_CAP> {
    /// Build an empty set sized for `n_subs` possible DirtySlotIds.
    ///
    /// Allocates a bitmap of `ceil(n_subs / 32)` `u32` words on the heap.
    /// The list lives inline in the struct.
    pub fn new(n_subs: u32) -> Self {
        let word_count = ((n_subs as usize) + 31) / 32;
        Self {
            head: 0,
            list: [0u32; LIST_CAP],
            bitmap: vec![0u32; word_count].into_boxed_slice(),
            overflowed: false,
            n_subs,
        }
    }

    /// Record that `id` is dirty. O(1).
    ///
    /// - If the list still has room, appends `id` without any dedup check.
    /// - Once the list is full, sets `overflowed` and ORs the bit into the
    ///   bitmap; that path is idempotent.
    ///
    /// Panics in debug builds if `id.0 >= n_subs`.
    pub fn mark_dirty(&mut self, id: DirtySlotId) {
        debug_assert!(
            id.0 < self.n_subs,
            "DirtySlotId({}) >= n_subs ({})",
            id.0,
            self.n_subs
        );

        if !self.overflowed && (self.head as usize) < LIST_CAP {
            self.list[self.head as usize] = id.0;
            self.head += 1;
            return;
        }

        self.overflowed = true;
        let word = (id.0 / 32) as usize;
        let bit = id.0 % 32;
        self.bitmap[word] |= 1u32 << bit;
    }

    /// Walk every currently-dirty id. Does not mutate the set — call
    /// [`DirtySet::clear`] when the batch has been processed.
    ///
    /// List entries are yielded first in insertion order (with duplicates),
    /// then bitmap entries in ascending id order (deduplicated).
    pub fn iter(&self) -> Iter<'_, LIST_CAP> {
        Iter::new(self)
    }

    /// Drop the current batch. After `clear()`, `is_empty()` is true and a
    /// fresh batch can be marked.
    ///
    /// Zeroing the bitmap is skipped when the batch has not overflowed.
    pub fn clear(&mut self) {
        self.head = 0;
        if self.overflowed {
            self.bitmap.fill(0);
            self.overflowed = false;
        }
    }

    /// True if no entries are currently dirty.
    pub fn is_empty(&self) -> bool {
        self.head == 0 && !self.overflowed
    }

    /// Current list length. Caps at `LIST_CAP`.
    pub fn list_len(&self) -> usize {
        self.head as usize
    }

    /// True if the current batch has spilled into the bitmap.
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// Configured DirtySlotId range (exclusive upper bound).
    pub fn n_subs(&self) -> u32 {
        self.n_subs
    }

    // Internal read-only accessors for Iter.

    pub(crate) fn list_slot(&self, pos: usize) -> u32 {
        self.list[pos]
    }

    pub(crate) fn bitmap_word_count(&self) -> usize {
        self.bitmap.len()
    }

    pub(crate) fn bitmap_word(&self, word: usize) -> u32 {
        self.bitmap[word]
    }
}
