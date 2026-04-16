//! Reactive execution — the hot path.
//!
//! When a mutation (INSERT/UPDATE/DELETE) happens, the execute module determines
//! which subscriptions are affected. The pipeline has two phases:
//!
//! 1. **Candidates** (`candidates::collect`): O(1) reverse-index lookup to narrow
//!    down which subscriptions *might* be affected.
//! 2. **Verify** (`verify::check`): evaluate the full verify_filter predicate on
//!    each candidate to confirm it is actually affected.

pub mod candidates;
pub mod verify;

use std::collections::{HashMap, HashSet};

use crate::reactive::registry::{SubId, SubscriptionRegistry};
use crate::storage::{CellValue, ZSet};

/// Process a ZSet against the registry — the primary integration point.
///
/// Iterates all entries in the ZSet and determines which subscriptions are
/// affected. Returns a map of SubId → set of triggered condition indices.
pub fn on_zset(
    registry: &SubscriptionRegistry,
    zset: &ZSet,
) -> HashMap<SubId, HashSet<usize>> {
    let mut affected: HashMap<SubId, HashSet<usize>> = HashMap::new();
    for entry in &zset.entries {
        for (sub_id, indices) in check_mutation(registry, &entry.table, &entry.row) {
            affected.entry(sub_id).or_default().extend(indices);
        }
    }
    affected
}

/// Core pipeline: collect candidates, then verify.
fn check_mutation(
    registry: &SubscriptionRegistry,
    table: &str,
    row: &[CellValue],
) -> HashMap<SubId, HashSet<usize>> {
    let candidates = candidates::collect(registry, table, row);
    verify::check(registry, candidates, table, row)
}

/// Check which subscriptions are affected by an INSERT.
pub fn on_insert(registry: &SubscriptionRegistry, table: &str, new_row: &[CellValue]) -> Vec<SubId> {
    check_mutation(registry, table, new_row).into_keys().collect()
}

/// Check which subscriptions are affected by a DELETE.
pub fn on_delete(registry: &SubscriptionRegistry, table: &str, old_row: &[CellValue]) -> Vec<SubId> {
    check_mutation(registry, table, old_row).into_keys().collect()
}

/// Like `on_insert` but also returns which condition indices triggered per subscription.
pub fn on_insert_detailed(
    registry: &SubscriptionRegistry,
    table: &str,
    new_row: &[CellValue],
) -> HashMap<SubId, HashSet<usize>> {
    check_mutation(registry, table, new_row)
}

/// Like `on_delete` but also returns which condition indices triggered per subscription.
pub fn on_delete_detailed(
    registry: &SubscriptionRegistry,
    table: &str,
    old_row: &[CellValue],
) -> HashMap<SubId, HashSet<usize>> {
    check_mutation(registry, table, old_row)
}
