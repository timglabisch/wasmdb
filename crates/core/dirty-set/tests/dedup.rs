//! Dedup semantics per crate-plan §6.1:
//! - list path: no dedup (duplicates appear)
//! - bitmap path: idempotent (duplicates collapse)
//! - overlap: id marked once in list, then in bitmap after overflow,
//!   is reported twice (integration is responsible for dedupe on read)

use dirty_set::{DirtySlotId, DirtySet};

#[test]
fn list_path_duplicates_appear() {
    let mut idx = DirtySet::<8>::new(16);
    idx.mark_dirty(DirtySlotId(3));
    idx.mark_dirty(DirtySlotId(3));
    idx.mark_dirty(DirtySlotId(3));

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(3), DirtySlotId(3), DirtySlotId(3)]);
}

#[test]
fn bitmap_path_dedupes() {
    let mut idx = DirtySet::<1>::new(16);
    idx.mark_dirty(DirtySlotId(0)); // list
    // All further marks of DirtySlotId(5) go to the bitmap.
    for _ in 0..50 {
        idx.mark_dirty(DirtySlotId(5));
    }

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    assert_eq!(batch, vec![DirtySlotId(0), DirtySlotId(5)]);
}

#[test]
fn list_then_bitmap_same_id_reports_twice() {
    // Documented overlap: integration-layer concern, but pin the contract.
    let mut idx = DirtySet::<1>::new(16);
    idx.mark_dirty(DirtySlotId(7));    // list
    idx.mark_dirty(DirtySlotId(9));    // overflow -> bitmap
    idx.mark_dirty(DirtySlotId(7));    // bitmap (same id as list)

    let batch: Vec<_> = idx.iter().collect();
    idx.clear();
    // List reports 7 in insertion order; bitmap path reports 7 again.
    assert_eq!(batch, vec![DirtySlotId(7), DirtySlotId(7), DirtySlotId(9)]);
}
