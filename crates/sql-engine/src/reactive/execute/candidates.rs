//! Phase 1: Candidate collection.
//!
//! Collects candidate subscription IDs that *might* be affected by a mutation.
//! Uses O(1) composite reverse-index lookups + table-level subscriptions.
//! Candidates are then verified by `verify::check()`.

use fnv::FnvHashSet;

use crate::reactive::execute::{ReactiveContext, ReactiveSpanOperation};
use crate::reactive::registry::{CompositeKey, SubId, SubscriptionRegistry};
use crate::storage::CellValue;

/// Collect all candidate subscriptions for a mutation on `table` with `row`.
///
/// Two sources of candidates:
/// 1. **Table-level**: subscriptions watching the entire table (always candidates).
/// 2. **Composite reverse-index**: For each registered column-set on the table,
///    build a composite key from the row values and do an O(1) lookup.
///
/// Emits `HashLookup` and `ScanLookup` child spans for tracing.
pub(crate) fn collect(
    ctx: &mut ReactiveContext,
    registry: &SubscriptionRegistry,
    table: &str,
    row: &[CellValue],
) -> FnvHashSet<SubId> {
    let mut candidates = FnvHashSet::default();

    // 1. Composite reverse-index: iterate registered column-sets for this table.
    if let Some(column_sets) = registry.column_sets_for_table(table) {
        for col_indices in column_sets {
            let cols: Vec<(usize, CellValue)> = col_indices
                .iter()
                .filter_map(|&col| row.get(col).map(|v| (col, v.clone())))
                .collect();
            // Only look up if we could extract all columns.
            if cols.len() == col_indices.len() {
                let key = CompositeKey {
                    table: table.to_string(),
                    cols: cols.clone(),
                };
                let key_values: Vec<CellValue> = cols.iter().map(|(_, v)| v.clone()).collect();
                let hit_subs: Vec<SubId> = registry.composite_lookup(&key)
                    .map(|s| s.iter().copied().collect())
                    .unwrap_or_default();
                candidates.extend(hit_subs.iter().copied());
                ctx.span(ReactiveSpanOperation::HashLookup { key_values, hit_subs }, |_| {});
            }
        }
    }

    // 2. Table-level subscriptions — any mutation on this table is a candidate.
    if let Some(subs) = registry.table_level_subs(table) {
        let hit_subs: Vec<SubId> = subs.iter().copied().collect();
        candidates.extend(&hit_subs);
        ctx.span(ReactiveSpanOperation::ScanLookup { hit_subs }, |_| {});
    }

    candidates
}
