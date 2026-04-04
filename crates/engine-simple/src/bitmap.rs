/// A compact bitmap backed by `Vec<u64>`.
/// Bit `i` is stored in `words[i / 64]` at position `(i % 64)`.
#[derive(Debug, Clone)]
pub struct Bitmap {
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    /// Create a bitmap with `len` bits, all set to `fill`.
    pub fn new(len: usize, fill: bool) -> Self {
        let n_words = (len + 63) / 64;
        let word = if fill { !0u64 } else { 0u64 };
        let mut bm = Bitmap {
            words: vec![word; n_words],
            len,
        };
        // Mask trailing bits in the last word so count_ones stays correct.
        if fill && len > 0 {
            let tail = len % 64;
            if tail > 0 {
                if let Some(last) = bm.words.last_mut() {
                    *last = (1u64 << tail) - 1;
                }
            }
        }
        bm
    }

    /// Create an empty bitmap with reserved capacity for `cap` bits.
    pub fn with_capacity(cap: usize) -> Self {
        let n_words = (cap + 63) / 64;
        Bitmap {
            words: Vec::with_capacity(n_words),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        debug_assert!(idx < self.len);
        let word = self.words[idx / 64];
        (word >> (idx % 64)) & 1 == 1
    }

    #[inline]
    pub fn set(&mut self, idx: usize, val: bool) {
        debug_assert!(idx < self.len);
        let w = idx / 64;
        let b = idx % 64;
        if val {
            self.words[w] |= 1u64 << b;
        } else {
            self.words[w] &= !(1u64 << b);
        }
    }

    /// Append one bit.
    pub fn push(&mut self, val: bool) {
        let b = self.len % 64;
        if b == 0 {
            // Need a new word.
            self.words.push(0);
        }
        self.len += 1;
        if val {
            let w = self.words.len() - 1;
            self.words[w] |= 1u64 << b;
        }
    }

    /// Number of set bits.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Number of unset bits.
    pub fn count_zeros(&self) -> usize {
        self.len - self.count_ones()
    }

    /// Iterate over indices where bit is 1.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(move |(wi, &word)| {
            let base = wi * 64;
            BitIter { word, base, limit: self.len }
        })
    }

    /// Iterate over indices where bit is 0.
    pub fn iter_zeros(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(move |(wi, &word)| {
            let base = wi * 64;
            BitIter { word: !word, base, limit: self.len }
        })
    }

    /// Bitwise AND (panics if lengths differ).
    pub fn and(&self, other: &Bitmap) -> Bitmap {
        assert_eq!(self.len, other.len);
        Bitmap {
            words: self.words.iter().zip(&other.words).map(|(a, b)| a & b).collect(),
            len: self.len,
        }
    }

    /// Bitwise OR (panics if lengths differ).
    pub fn or(&self, other: &Bitmap) -> Bitmap {
        assert_eq!(self.len, other.len);
        Bitmap {
            words: self.words.iter().zip(&other.words).map(|(a, b)| a | b).collect(),
            len: self.len,
        }
    }

    /// Bitwise NOT.
    pub fn not(&self) -> Bitmap {
        let mut words: Vec<u64> = self.words.iter().map(|w| !w).collect();
        // Mask trailing bits in the last word so count_ones stays correct.
        let tail = self.len % 64;
        if tail > 0 {
            if let Some(last) = words.last_mut() {
                *last &= (1u64 << tail) - 1;
            }
        }
        Bitmap { words, len: self.len }
    }

    /// Create a bitmap from a closure: bit `i` = `f(i)`.
    pub fn from_fn(len: usize, f: impl Fn(usize) -> bool) -> Self {
        let mut bm = Bitmap::with_capacity(len);
        for i in 0..len {
            bm.push(f(i));
        }
        bm
    }
}

/// Iterator that yields set-bit positions within a single u64 word.
struct BitIter {
    word: u64,
    base: usize,
    limit: usize,
}

impl Iterator for BitIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        while self.word != 0 {
            let tz = self.word.trailing_zeros() as usize;
            self.word &= self.word - 1; // clear lowest set bit
            let idx = self.base + tz;
            if idx < self.limit {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_false() {
        let bm = Bitmap::new(10, false);
        assert_eq!(bm.len(), 10);
        assert_eq!(bm.count_ones(), 0);
        for i in 0..10 {
            assert!(!bm.get(i));
        }
    }

    #[test]
    fn test_new_true() {
        let bm = Bitmap::new(10, true);
        assert_eq!(bm.len(), 10);
        assert_eq!(bm.count_ones(), 10);
        for i in 0..10 {
            assert!(bm.get(i));
        }
    }

    #[test]
    fn test_push_and_get() {
        let mut bm = Bitmap::with_capacity(0);
        bm.push(true);
        bm.push(false);
        bm.push(true);
        assert_eq!(bm.len(), 3);
        assert!(bm.get(0));
        assert!(!bm.get(1));
        assert!(bm.get(2));
    }

    #[test]
    fn test_set() {
        let mut bm = Bitmap::new(5, false);
        bm.set(2, true);
        bm.set(4, true);
        assert!(!bm.get(0));
        assert!(bm.get(2));
        assert!(bm.get(4));
        bm.set(2, false);
        assert!(!bm.get(2));
    }

    #[test]
    fn test_count() {
        let mut bm = Bitmap::new(100, false);
        for i in (0..100).step_by(3) {
            bm.set(i, true);
        }
        assert_eq!(bm.count_ones(), 34);
        assert_eq!(bm.count_zeros(), 66);
    }

    #[test]
    fn test_iter_ones() {
        let mut bm = Bitmap::new(10, false);
        bm.set(1, true);
        bm.set(5, true);
        bm.set(9, true);
        let ones: Vec<usize> = bm.iter_ones().collect();
        assert_eq!(ones, vec![1, 5, 9]);
    }

    #[test]
    fn test_iter_zeros() {
        let mut bm = Bitmap::new(5, true);
        bm.set(2, false);
        let zeros: Vec<usize> = bm.iter_zeros().collect();
        assert_eq!(zeros, vec![2]);
    }

    #[test]
    fn test_and_or_not() {
        let mut a = Bitmap::new(8, false);
        a.set(0, true);
        a.set(2, true);
        a.set(4, true);

        let mut b = Bitmap::new(8, false);
        b.set(2, true);
        b.set(3, true);
        b.set(4, true);

        let and = a.and(&b);
        assert_eq!(and.iter_ones().collect::<Vec<_>>(), vec![2, 4]);

        let or = a.or(&b);
        assert_eq!(or.iter_ones().collect::<Vec<_>>(), vec![0, 2, 3, 4]);

        let not_a = a.not();
        assert_eq!(not_a.iter_ones().collect::<Vec<_>>(), vec![1, 3, 5, 6, 7]);
    }

    #[test]
    fn test_from_fn() {
        let bm = Bitmap::from_fn(10, |i| i % 2 == 0);
        assert_eq!(bm.iter_ones().collect::<Vec<_>>(), vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_across_word_boundary() {
        // Test with >64 bits to verify multi-word behavior
        let mut bm = Bitmap::new(130, false);
        bm.set(0, true);
        bm.set(63, true);
        bm.set(64, true);
        bm.set(127, true);
        bm.set(129, true);
        let ones: Vec<usize> = bm.iter_ones().collect();
        assert_eq!(ones, vec![0, 63, 64, 127, 129]);
        assert_eq!(bm.count_ones(), 5);
    }

    #[test]
    fn test_empty() {
        let bm = Bitmap::new(0, false);
        assert!(bm.is_empty());
        assert_eq!(bm.count_ones(), 0);
        assert_eq!(bm.iter_ones().collect::<Vec<_>>(), vec![]);
    }

    #[test]
    fn test_not_trailing_bits_masked() {
        // 3 bits in a 64-bit word: NOT should only flip 3 bits, not 64
        let bm = Bitmap::new(3, false);
        let not = bm.not();
        assert_eq!(not.count_ones(), 3);
        assert_eq!(not.len(), 3);
    }

    #[test]
    fn test_push_across_word_boundary() {
        let mut bm = Bitmap::with_capacity(0);
        for i in 0..70 {
            bm.push(i % 3 == 0);
        }
        assert_eq!(bm.len(), 70);
        assert!(bm.get(0));
        assert!(!bm.get(1));
        assert!(!bm.get(2));
        assert!(bm.get(3));
        assert!(bm.get(63));
        assert!(!bm.get(64));
        assert!(!bm.get(65));
        assert!(bm.get(66));
    }
}
