//! Phase 1: Candidate collection.
//!
//! Collects candidate subscription IDs that *might* be affected by a mutation.
//! Uses O(1) reverse-index lookups + table-level subscriptions.
//! Candidates are then verified by `verify::check()`.

use std::collections::HashSet;

use crate::reactive::registry::{MaterializedLookupKey, SubId, SubscriptionRegistry};
use crate::storage::CellValue;

/// Collect all candidate subscriptions for a mutation on `table` with `row`.
///
/// Two sources of candidates:
/// 1. **Table-level**: subscriptions watching the entire table (always candidates).
/// 2. **Reverse-index**: O(1) lookup per column value — finds subscriptions whose
///    lookup keys match a column in the mutated row.
pub(crate) fn collect(
    registry: &SubscriptionRegistry,
    table: &str,
    row: &[CellValue],
) -> HashSet<SubId> {
    let mut candidates = HashSet::new();

    // 1. Table-level subscriptions — any mutation on this table is a candidate.
    if let Some(subs) = registry.table_level_subs(table) {
        candidates.extend(subs);
    }

    // 2. Reverse-index lookup per column.
    for (col_idx, cell) in row.iter().enumerate() {
        let key = MaterializedLookupKey {
            table: table.to_string(),
            col: col_idx,
            value: cell.clone(),
        };
        if let Some(subs) = registry.index_lookup(&key) {
            candidates.extend(subs);
        }
    }

    candidates
}
