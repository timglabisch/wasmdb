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
    bitmap_word: usize,
    word_bits: u32,
}

impl<'a, const LIST_CAP: usize> Iter<'a, LIST_CAP> {
    pub(crate) fn new(set: &'a DirtySet<LIST_CAP>) -> Self {
        let list_len = set.list_len();
        let overflowed = set.overflowed();
        Self {
            set,
            list_len,
            list_pos: 0,
            overflowed,
            bitmap_word: 0,
            word_bits: 0,
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

        // Phase 2: bitmap, only if the batch overflowed. `word_bits` is a
        // local copy; the set's bitmap is never mutated by iteration.
        if !self.overflowed {
            return None;
        }

        loop {
            if self.word_bits == 0 {
                if self.bitmap_word >= self.set.bitmap_word_count() {
                    return None;
                }
                self.word_bits = self.set.bitmap_word(self.bitmap_word);
                self.bitmap_word += 1;
            }
            if self.word_bits != 0 {
                let bit = self.word_bits.trailing_zeros();
                self.word_bits &= self.word_bits - 1;
                let id = ((self.bitmap_word - 1) as u32) * 32 + bit;
                return Some(DirtySlotId(id));
            }
        }
    }
}
