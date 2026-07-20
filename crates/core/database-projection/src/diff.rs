//! Multiset diff between two renders of the same key.
//!
//! Duplicates count (an identical row twice is twice); unchanged rows
//! cancel out and never appear in the delta. Output is deterministically
//! sorted by (table, row) and emitted as unit-weight entries (±1) so
//! `apply_zset`'s row-at-a-time semantics (PK-upsert for +, tolerant
//! full-row delete for −) apply each change individually and replacement
//! pairs stay order-robust.

use std::collections::HashMap;

use sql_engine::storage::{ZSet, ZSetEntry};

use crate::spec::OutputRow;

/// `diff(new, old)` — the delta that turns the materialization of `old`
/// into the materialization of `new`.
pub fn multiset_diff(new: &[OutputRow], old: &[OutputRow]) -> ZSet {
    let mut counts: HashMap<&OutputRow, i64> = HashMap::new();
    for row in new {
        *counts.entry(row).or_default() += 1;
    }
    for row in old {
        *counts.entry(row).or_default() -= 1;
    }

    let mut changed: Vec<(&OutputRow, i64)> =
        counts.into_iter().filter(|(_, w)| *w != 0).collect();
    changed.sort_by_key(|&(row, _)| row);

    let mut zset = ZSet::new();
    for ((table, row), weight) in changed {
        let unit = if weight > 0 { 1 } else { -1 };
        for _ in 0..weight.abs() {
            zset.entries.push(ZSetEntry {
                table: table.clone(),
                row: row.clone(),
                weight: unit,
            });
        }
    }
    zset
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_engine::storage::CellValue;

    fn row(table: &str, cells: &[i64]) -> OutputRow {
        (
            table.to_string(),
            cells.iter().map(|&c| CellValue::I64(c)).collect(),
        )
    }

    #[test]
    fn unchanged_rows_cancel_out() {
        let old = vec![row("t", &[1, 10]), row("t", &[2, 20])];
        let new = vec![row("t", &[1, 10]), row("t", &[2, 20])];
        assert!(multiset_diff(&new, &old).is_empty());
    }

    #[test]
    fn replacement_pair() {
        let old = vec![row("t", &[1, 10])];
        let new = vec![row("t", &[1, 11])];
        let d = multiset_diff(&new, &old);
        assert_eq!(d.entries.len(), 2);
        // Sorted by (table, row): [1,10] before [1,11].
        assert_eq!(d.entries[0].row, vec![CellValue::I64(1), CellValue::I64(10)]);
        assert_eq!(d.entries[0].weight, -1);
        assert_eq!(d.entries[1].row, vec![CellValue::I64(1), CellValue::I64(11)]);
        assert_eq!(d.entries[1].weight, 1);
    }

    #[test]
    fn duplicates_count() {
        // Same row twice in new, once in old → one +1 entry.
        let old = vec![row("t", &[1, 10])];
        let new = vec![row("t", &[1, 10]), row("t", &[1, 10])];
        let d = multiset_diff(&new, &old);
        assert_eq!(d.entries.len(), 1);
        assert_eq!(d.entries[0].weight, 1);

        // Three in old, one in new → two -1 entries.
        let old = vec![row("t", &[1, 10]), row("t", &[1, 10]), row("t", &[1, 10])];
        let new = vec![row("t", &[1, 10])];
        let d = multiset_diff(&new, &old);
        assert_eq!(d.entries.len(), 2);
        assert!(d.entries.iter().all(|e| e.weight == -1));
    }

    #[test]
    fn deterministic_order_across_tables() {
        let old = vec![];
        let new = vec![row("b", &[2]), row("a", &[1]), row("b", &[1])];
        let d = multiset_diff(&new, &old);
        let seq: Vec<(&str, &Vec<CellValue>)> =
            d.entries.iter().map(|e| (e.table.as_str(), &e.row)).collect();
        assert_eq!(
            seq,
            vec![
                ("a", &vec![CellValue::I64(1)]),
                ("b", &vec![CellValue::I64(1)]),
                ("b", &vec![CellValue::I64(2)]),
            ]
        );
    }

    #[test]
    fn full_teardown_is_negated_render() {
        let old = vec![row("t", &[1]), row("u", &[2])];
        let d = multiset_diff(&[], &old);
        assert_eq!(d.entries.len(), 2);
        assert!(d.entries.iter().all(|e| e.weight == -1));
    }
}
