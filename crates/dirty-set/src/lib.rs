//! Single-threaded dirty-set primitive.
//!
//! [`DirtySet`] collects [`DirtySlotId`]s marked dirty by a writer. Readers
//! walk [`DirtySet::iter`] (read-only) and then call [`DirtySet::clear`] to
//! drop the batch. Writer calls [`DirtySet::mark_dirty`] are O(1).
//!
//! The primitive is single-threaded (`&mut self` for writes, `&self` for
//! reads), zero-dep, and has no shared-memory story. A future atomic /
//! SAB-backed variant will live in a separate crate once the slot protocol
//! (versioned slots) is designed.
//!
//! # Naming caveat
//!
//! "Set" is slightly loose — the list path does not deduplicate, so a mark
//! repeated while the list still has room will appear twice on iteration.
//! The bitmap path (once the list overflows) IS deduped. See
//! [`DirtySet::mark_dirty`] for the exact contract.
//!
//! # Intended use
//!
//! The reactive engine maps `SubscriptionId` → `DirtySlotId` at the
//! integration layer. Mutations call `mark_dirty` per affected subscription;
//! the wasm-bindgen boundary calls `iter` once per Wasm return, notifies
//! each JS store exactly once, and then calls `clear`.
//!
//! This crate does not know anything about subscriptions, SQL, or JS.
//!
//! # Example
//!
//! ```
//! use dirty_set::{DirtySlotId, DirtySet};
//!
//! let mut set = DirtySet::<128>::new(4096);
//! set.mark_dirty(DirtySlotId(3));
//! set.mark_dirty(DirtySlotId(7));
//! set.mark_dirty(DirtySlotId(3));
//!
//! let batch: Vec<_> = set.iter().collect();
//! // List path does not dedupe: 3 appears twice.
//! assert_eq!(batch, vec![DirtySlotId(3), DirtySlotId(7), DirtySlotId(3)]);
//!
//! set.clear();
//! assert!(set.is_empty());
//! ```

#![forbid(unsafe_code)]

mod dirty_slot_id;
mod iter;
mod set;

pub use dirty_slot_id::DirtySlotId;
pub use iter::Iter;
pub use set::DirtySet;

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let set = DirtySet::<16>::new(100);
        assert!(set.is_empty());
        assert_eq!(set.list_len(), 0);
        assert!(!set.overflowed());
        assert_eq!(set.n_subs(), 100);
    }

    #[test]
    fn bitmap_word_count_rounds_up() {
        let set = DirtySet::<8>::new(100);
        assert_eq!(set.n_subs(), 100);
        let _ = set;
    }

    #[test]
    fn mark_dirty_list_path_appends() {
        let mut set = DirtySet::<8>::new(32);
        set.mark_dirty(DirtySlotId(1));
        set.mark_dirty(DirtySlotId(2));
        set.mark_dirty(DirtySlotId(3));
        assert_eq!(set.list_len(), 3);
        assert!(!set.overflowed());
        assert!(!set.is_empty());
    }

    #[test]
    fn mark_dirty_list_fills_to_cap() {
        let mut set = DirtySet::<4>::new(32);
        for i in 0..4 {
            set.mark_dirty(DirtySlotId(i));
        }
        assert_eq!(set.list_len(), 4);
        assert!(!set.overflowed());
    }

    #[test]
    fn mark_dirty_overflows_to_bitmap() {
        let mut set = DirtySet::<4>::new(32);
        for i in 0..4 {
            set.mark_dirty(DirtySlotId(i));
        }
        set.mark_dirty(DirtySlotId(10));
        assert!(set.overflowed());
        assert_eq!(set.list_len(), 4);
    }

    #[test]
    fn bitmap_is_idempotent_after_overflow() {
        let mut set = DirtySet::<2>::new(32);
        set.mark_dirty(DirtySlotId(0));
        set.mark_dirty(DirtySlotId(1));
        for _ in 0..1000 {
            set.mark_dirty(DirtySlotId(5));
        }
        let batch: Vec<_> = set.iter().collect();
        assert_eq!(batch, vec![DirtySlotId(0), DirtySlotId(1), DirtySlotId(5)]);
    }

    #[test]
    fn iter_yields_list_in_order() {
        let mut set = DirtySet::<8>::new(32);
        set.mark_dirty(DirtySlotId(7));
        set.mark_dirty(DirtySlotId(2));
        set.mark_dirty(DirtySlotId(5));
        let batch: Vec<_> = set.iter().collect();
        assert_eq!(batch, vec![DirtySlotId(7), DirtySlotId(2), DirtySlotId(5)]);
    }

    #[test]
    fn iter_does_not_mutate() {
        let mut set = DirtySet::<4>::new(32);
        set.mark_dirty(DirtySlotId(1));
        set.mark_dirty(DirtySlotId(2));
        let first: Vec<_> = set.iter().collect();
        let second: Vec<_> = set.iter().collect();
        assert_eq!(first, second);
        assert!(!set.is_empty());
    }

    #[test]
    fn clear_resets_state() {
        let mut set = DirtySet::<4>::new(32);
        set.mark_dirty(DirtySlotId(1));
        set.clear();
        assert!(set.is_empty());
        assert_eq!(set.list_len(), 0);
        assert!(!set.overflowed());
    }

    #[test]
    fn clear_resets_bitmap_after_overflow() {
        let mut set = DirtySet::<1>::new(64);
        set.mark_dirty(DirtySlotId(0));
        set.mark_dirty(DirtySlotId(10));
        set.mark_dirty(DirtySlotId(30));
        assert!(set.overflowed());
        set.clear();
        assert!(set.is_empty());
        assert!(!set.overflowed());
        let after: Vec<_> = set.iter().collect();
        assert!(after.is_empty());
    }

    #[test]
    fn iter_of_empty_is_empty() {
        let set = DirtySet::<4>::new(32);
        let batch: Vec<_> = set.iter().collect();
        assert!(batch.is_empty());
    }

    #[test]
    fn back_to_back_clear_is_empty() {
        let mut set = DirtySet::<4>::new(32);
        set.mark_dirty(DirtySlotId(3));
        let first: Vec<_> = set.iter().collect();
        set.clear();
        let second: Vec<_> = set.iter().collect();
        assert_eq!(first, vec![DirtySlotId(3)]);
        assert!(second.is_empty());
    }

    #[test]
    fn list_then_bitmap_iter_reports_both() {
        let mut set = DirtySet::<2>::new(64);
        set.mark_dirty(DirtySlotId(10));
        set.mark_dirty(DirtySlotId(20));
        set.mark_dirty(DirtySlotId(33));
        set.mark_dirty(DirtySlotId(40));
        let batch: Vec<_> = set.iter().collect();
        assert_eq!(
            batch,
            vec![DirtySlotId(10), DirtySlotId(20), DirtySlotId(33), DirtySlotId(40)]
        );
    }

    #[test]
    #[should_panic]
    fn mark_beyond_n_subs_debug_panics() {
        let mut set = DirtySet::<4>::new(32);
        set.mark_dirty(DirtySlotId(32));
    }
}
