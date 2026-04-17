//! Proptest-driven roundtrip: for any sequence of marks on any index size,
//! the set of iterated ids equals the set of marked ids.
//!
//! The list path preserves insertion order (with duplicates), the bitmap
//! path deduplicates. Comparing as sets is the most general invariant.

use proptest::prelude::*;
use dirty_set::{DirtySlotId, DirtySet};
use std::collections::HashSet;

proptest! {
    /// For every mark sequence, iter-as-set == marked-as-set.
    #[test]
    fn iter_set_equals_marked_set(
        n_subs in 1u32..256,
        marks in proptest::collection::vec(0u32..256, 0..500),
    ) {
        // Clamp marks to [0, n_subs).
        let clamped: Vec<u32> = marks.iter().map(|m| m % n_subs).collect();

        let mut set = DirtySet::<32>::new(n_subs);
        for m in &clamped {
            set.mark_dirty(DirtySlotId(*m));
        }

        let batch: HashSet<u32> = set.iter().map(|d| d.0).collect();
        let expected: HashSet<u32> = clamped.into_iter().collect();
        prop_assert_eq!(batch, expected);

        set.clear();
        prop_assert!(set.is_empty());
    }

    /// After clear, iteration yields nothing regardless of what the first
    /// mark sequence looked like.
    #[test]
    fn clear_yields_empty_iter(
        n_subs in 1u32..128,
        marks in proptest::collection::vec(0u32..128, 0..200),
    ) {
        let mut set = DirtySet::<16>::new(n_subs);
        for m in &marks {
            set.mark_dirty(DirtySlotId(m % n_subs));
        }
        let _first: Vec<_> = set.iter().collect();
        set.clear();
        let second: Vec<_> = set.iter().collect();
        prop_assert!(second.is_empty());
    }

    /// Multi-tick invariant: marks → iter → clear → marks → iter yields
    /// each tick's marked set independently.
    #[test]
    fn marks_across_ticks_isolate(
        n_subs in 1u32..64,
        tick_a in proptest::collection::vec(0u32..64, 0..100),
        tick_b in proptest::collection::vec(0u32..64, 0..100),
    ) {
        let mut set = DirtySet::<16>::new(n_subs);

        for m in &tick_a {
            set.mark_dirty(DirtySlotId(m % n_subs));
        }
        let a: HashSet<u32> = set.iter().map(|d| d.0).collect();
        set.clear();

        for m in &tick_b {
            set.mark_dirty(DirtySlotId(m % n_subs));
        }
        let b: HashSet<u32> = set.iter().map(|d| d.0).collect();
        set.clear();

        let expected_a: HashSet<u32> =
            tick_a.into_iter().map(|m| m % n_subs).collect();
        let expected_b: HashSet<u32> =
            tick_b.into_iter().map(|m| m % n_subs).collect();
        prop_assert_eq!(a, expected_a);
        prop_assert_eq!(b, expected_b);
    }
}
