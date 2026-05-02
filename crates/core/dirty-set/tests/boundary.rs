//! Edge cases: min/max DirtySlotIds, empty iter, awkward sizes.

use dirty_set::{DirtySlotId, DirtySet};

#[test]
fn mark_dense_id_zero() {
    let mut idx = DirtySet::<4>::new(16);
    idx.mark_dirty(DirtySlotId(0));
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(0)]);
}

#[test]
fn mark_max_dense_id() {
    let mut idx = DirtySet::<4>::new(64);
    idx.mark_dirty(DirtySlotId(63));
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(63)]);
}

#[test]
fn mark_max_dense_id_via_bitmap() {
    // Force overflow so DirtySlotId(63) lands in the last bitmap bit.
    let mut idx = DirtySet::<1>::new(64);
    idx.mark_dirty(DirtySlotId(0));
    idx.mark_dirty(DirtySlotId(63));
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(0), DirtySlotId(63)]);
}

#[test]
fn n_subs_not_multiple_of_32() {
    // 33 subs -> ceil(33/32) = 2 words. DirtySlotId(32) lives in the second word.
    let mut idx = DirtySet::<1>::new(33);
    idx.mark_dirty(DirtySlotId(0));
    idx.mark_dirty(DirtySlotId(32));
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(0), DirtySlotId(32)]);
}

#[test]
fn iter_on_fresh_index_yields_nothing() {
    let idx = DirtySet::<4>::new(16);
    let batch: Vec<_> = idx.iter().collect();
    assert!(batch.is_empty());
    assert!(idx.is_empty());
}

#[test]
fn many_back_to_back_iters_stay_consistent() {
    let mut idx = DirtySet::<4>::new(32);
    for _ in 0..100 {
        let batch: Vec<_> = idx.iter().collect();
        assert!(batch.is_empty());
        idx.clear();
    }
    idx.mark_dirty(DirtySlotId(5));
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(5)]);
}

#[test]
fn list_cap_one_every_mark_after_first_overflows() {
    let mut idx = DirtySet::<1>::new(16);
    idx.mark_dirty(DirtySlotId(0));
    assert!(!idx.overflowed());
    idx.mark_dirty(DirtySlotId(1));
    assert!(idx.overflowed());

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(0), DirtySlotId(1)]);
}

#[test]
fn iter_does_not_mutate_preserves_batch_across_reads() {
    // Replaces the old partial-drain test. `iter()` does not mutate, so the
    // set keeps its batch across as many reads as the caller wants. `clear`
    // is the explicit drop point.
    let mut idx = DirtySet::<1>::new(64);
    idx.mark_dirty(DirtySlotId(0));    // list
    idx.mark_dirty(DirtySlotId(10));   // bitmap
    idx.mark_dirty(DirtySlotId(20));   // bitmap
    idx.mark_dirty(DirtySlotId(30));   // bitmap

    let full: Vec<_> = idx.iter().collect();
    assert_eq!(full.len(), 4);

    // Read again — state is unchanged.
    let again: Vec<_> = idx.iter().collect();
    assert_eq!(full, again);
    assert!(!idx.is_empty());

    // Explicit clear.
    idx.clear();
    assert!(idx.is_empty());
    let after: Vec<_> = idx.iter().collect();
    assert!(after.is_empty());
}
