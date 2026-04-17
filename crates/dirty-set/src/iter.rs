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
}

impl<const LIST_CAP: usize> Iterator for Iter<'_, LIST_CAP> {
    type Item = DirtySlotId;

    fn next(&mut self) -> Option<DirtySlotId> {
        // Phase 1: ring list, in insertion order.
        if self.list_pos < self.list_len {
            let id = self.set.list_slot(self.list_pos);
            self.list_pos += 1;
            return Some(DirtySlotId(id));
        }

        // Phase 2: bitmap, only if the batch overflowed.
        if !self.overflowed {
            return None;
        }

        // Yield one set bit per call, in ascending id order. `word_bits`
        // is a local copy of the current word — the bitmap itself is
        // never mutated.
        loop {
            if self.word_bits != 0 {
                // Position of the lowest set bit in the word (0..32).
                // Example: 0b1010100 -> 2.
                // Compiles to `tzcnt` (x86_64-BMI) / `rbit+clz` (arm64).
                let bit = self.word_bits.trailing_zeros();

                // Clear that lowest set bit. The subtraction rolls a
                // borrow through the trailing zeros, so `x - 1` flips
                // the lowest 1-bit to 0 (and the zeros below it to 1);
                // the AND then keeps only the higher bits.
                //   0b1010100 & 0b1010011  ==  0b1010000
                // Compiles to `blsr` (x86_64-BMI).
                self.word_bits &= self.word_bits - 1;

                // Each word holds 32 ids, so the id is simply
                // `word_index * 32 + bit_position`.
                let id = (self.bitmap_word as u32) * 32 + bit;
                return Some(DirtySlotId(id));
            }

            // Current word exhausted — advance. Loop allows skipping
            // runs of zero-words inside a single `next()` call.
            self.bitmap_word += 1;
            if self.bitmap_word >= self.set.bitmap_word_count() {
                return None;
            }
            self.word_bits = self.set.bitmap_word(self.bitmap_word);
        }
    }
}
