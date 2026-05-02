//! Deterministic overflow scenarios.

use dirty_set::{DirtySlotId, DirtySet};
use std::collections::HashSet;

#[test]
fn exactly_list_cap_marks_stays_in_list() {
    let mut idx = DirtySet::<8>::new(64);
    for i in 0..8u32 {
        idx.mark_dirty(DirtySlotId(i));
    }
    assert_eq!(idx.list_len(), 8);
    assert!(!idx.overflowed());

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    let expected: Vec<DirtySlotId> = (0..8).map(DirtySlotId).collect();
    assert_eq!(batch, expected);
}

#[test]
fn one_more_than_cap_triggers_overflow() {
    let mut idx = DirtySet::<4>::new(64);
    for i in 0..4u32 {
        idx.mark_dirty(DirtySlotId(i));
    }
    assert!(!idx.overflowed());

    idx.mark_dirty(DirtySlotId(50));
    assert!(idx.overflowed());

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    // List first (insertion order), then bitmap.
    assert_eq!(
        batch,
        vec![
            DirtySlotId(0),
            DirtySlotId(1),
            DirtySlotId(2),
            DirtySlotId(3),
            DirtySlotId(50),
        ]
    );
}

#[test]
fn mass_overflow_dedupes_in_bitmap() {
    let mut idx = DirtySet::<2>::new(128);
    idx.mark_dirty(DirtySlotId(0));
    idx.mark_dirty(DirtySlotId(1));
    // Overflow. Repeat a handful of ids many times each.
    for _ in 0..500 {
        idx.mark_dirty(DirtySlotId(10));
        idx.mark_dirty(DirtySlotId(20));
        idx.mark_dirty(DirtySlotId(100));
    }

    let batch: HashSet<u32> = idx.iter().map(|d| d.0).collect();
    idx.clear();
    assert_eq!(
        batch,
        HashSet::from([0, 1, 10, 20, 100])
    );
}

#[test]
fn bitmap_drains_in_ascending_order() {
    let mut idx = DirtySet::<1>::new(256);
    // Fill list so the very next mark overflows.
    idx.mark_dirty(DirtySlotId(200));
    // Now bitmap path only.
    for &id in &[100u32, 50, 250, 10, 127, 128] {
        idx.mark_dirty(DirtySlotId(id));
    }

    let batch: Vec<u32> = idx.iter().map(|d| d.0).collect();
    idx.clear();
    // First the list entry, then bitmap entries ascending.
    assert_eq!(batch, vec![200, 10, 50, 100, 127, 128, 250]);
}

#[test]
fn overflow_resets_on_clear() {
    let mut idx = DirtySet::<1>::new(64);
    idx.mark_dirty(DirtySlotId(0));
    idx.mark_dirty(DirtySlotId(5)); // overflow

    idx.clear();
    assert!(!idx.overflowed());
    assert_eq!(idx.list_len(), 0);

    // Second batch stays in list.
    idx.mark_dirty(DirtySlotId(7));
    assert!(!idx.overflowed());
    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(7)]);
}
