use crate::dirty_slot_id::DirtySlotId;
use crate::set::DirtySet;

/// Read-only iterator returned from [`DirtySet::iter`].
///
/// Yields every [`DirtySlotId`] currently dirty. List entries come first, in
/// insertion order; bitmap entries follow if the batch overflowed, in
/// ascending id order.
///
/// Does not mutate the set. Call [`DirtySet::clear`] explicitly when you
/// want to drop the batch. Partial iteration is safe — the set's state is
/// unchanged until `clear` runs.
pub struct Iter<'a, const LIST_CAP: usize> {
    set: &'a DirtySet<LIST_CAP>,
    list_len: usize,
    list_pos: usize,
    overflowed: bool,
    /// Index of the bitmap word we're currently consuming bits from.
    /// Invariant: `word_bits` is a local copy of `set.bitmap[bitmap_word]`
    /// with already-yielded bits cleared.
    bitmap_word: usize,
    word_bits: u32,
}

impl<'a, const LIST_CAP: usize> Iter<'a, LIST_CAP> {
    pub(crate) fn new(set: &'a DirtySet<LIST_CAP>) -> Self {
        let list_len = set.list_len();
        let overflowed = set.overflowed();
        // Pre-load word 0 so `bitmap_word` always points at the word
        // `word_bits` came from.
        let word_bits = if overflowed && set.bitmap_word_count() > 0 {
            set.bitmap_word(0)
        } else {
            0
        };
        Self {
            set,
            list_len,
            list_pos: 0,
            overflowed,
            bitmap_word: 0,
            word_bits,
        }
    }

    /// Take the lowest set bit of the current word and return its id.
    /// Caller must ensure `word_bits != 0`.
    ///
    /// The whole bitmap phase is built on this primitive: repeatedly
    /// take the *lowest* 1-bit, which gives ids in ascending order.
    fn take_lowest_bit(&mut self) -> DirtySlotId {
        debug_assert!(self.word_bits != 0);

        // Position of the lowest 1-bit in the word (0..32).
        // Example: 0b1010100 -> 2.
        // One instruction: `tzcnt` (x86_64-BMI) / `rbit+clz` (arm64).
        let bit = self.word_bits.trailing_zeros();

        // Turn off that lowest 1-bit so the next call finds the next
        // one up. `x & (x - 1)` does it in one instruction (`blsr`):
        // subtracting 1 flips the lowest 1 to 0 and the zeros below it
        // to 1; AND-ing keeps only what was above, dropping that one
        // bit. No shift needed.
        //   0b1010100 - 1   =  0b1010011
        //   0b1010100 & ..  =  0b1010000
        self.word_bits &= self.word_bits - 1;

        // Each word holds 32 contiguous ids, plus the bit offset inside.
        DirtySlotId((self.bitmap_word as u32) * 32 + bit)
    }

    /// Advance past empty words until the current word has at least one
    /// set bit. Returns `false` when the bitmap is exhausted.
    fn advance_until_nonempty(&mut self) -> bool {
        while self.word_bits == 0 {
            self.bitmap_word += 1;
            if self.bitmap_word >= self.set.bitmap_word_count() {
                return false;
            }
            self.word_bits = self.set.bitmap_word(self.bitmap_word);
        }
        true
    }
}

impl<const LIST_CAP: usize> Iterator for Iter<'_, LIST_CAP> {
    type Item = DirtySlotId;

    fn next(&mut self) -> Option<DirtySlotId> {
        // Phase 1: ring list, one entry per call, in insertion order.
        if self.list_pos < self.list_len {
            let id = self.set.list_slot(self.list_pos);
            self.list_pos += 1;
            return Some(DirtySlotId(id));
        }

        // Phase 2: bitmap. The algorithm is just two steps:
        //   1. Skip forward to a word that has a 1-bit.
        //   2. Take its lowest 1-bit and return the corresponding id.
        // Repeated across calls, this yields every dirty id in
        // ascending order and ignores zero bits for free.
        if !self.overflowed {
            return None;
        }
        if !self.advance_until_nonempty() {
            return None;
        }
        Some(self.take_lowest_bit())
    }
}
