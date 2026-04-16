//! Phase 2: Verify filter evaluation.
//!
//! Evaluates the full verify_filter predicate on each candidate subscription.
//! Only candidates that pass the predicate check are returned as affected.

use std::collections::{HashMap, HashSet};

use crate::execute::filter_row::eval_predicate;
use crate::planner::plan::ColumnRef;
use crate::reactive::registry::{SubId, SubscriptionRegistry};
use crate::storage::CellValue;

/// Verify which candidates are actually affected by evaluating verify_filter.
///
/// For each candidate, iterates over its conditions for the given table and
/// evaluates the full predicate against the row. Returns a map of SubId →
/// set of triggered condition indices.
pub(crate) fn check(
    registry: &SubscriptionRegistry,
    candidates: HashSet<SubId>,
    table: &str,
    row: &[CellValue],
) -> HashMap<SubId, HashSet<usize>> {
    let mut result: HashMap<SubId, HashSet<usize>> = HashMap::new();

    for sub_id in candidates {
        let conditions = registry.conditions(sub_id);
        if conditions.is_empty() {
            result.insert(sub_id, HashSet::new());
            continue;
        }

        let mut triggered = HashSet::new();
        for (idx, cond) in conditions.iter().enumerate() {
            if cond.table != table {
                continue;
            }
            let matches = eval_predicate(&cond.verify_filter, &|col: ColumnRef| {
                row.get(col.col).cloned().unwrap_or(CellValue::Null)
            });
            if matches {
                triggered.insert(idx);
            }
        }

        if !triggered.is_empty() {
            result.insert(sub_id, triggered);
        }
    }

    result
}
