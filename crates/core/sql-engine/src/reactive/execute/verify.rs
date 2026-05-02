//! Phase 2: Verify filter evaluation.
//!
//! Evaluates the full verify_filter predicate on each candidate subscription.
//! Only candidates that pass the predicate check are returned as affected.

use fnv::{FnvHashMap, FnvHashSet};

use crate::execute::filter_row::eval_predicate;
use crate::planner::shared::plan::{ColumnRef, PlanFilterPredicate};
use crate::reactive::execute::{ReactiveContext, ReactiveSpanOperation};
use crate::reactive::identity::SubscriptionId;
use crate::reactive::registry::SubscriptionRegistry;
use crate::storage::CellValue;

/// Verify which candidates are actually affected by evaluating verify_filter.
///
/// For each candidate, iterates over its conditions for the given table and
/// evaluates the full predicate against the row. Emits a `ConditionEval` child
/// span per evaluated condition showing sub_id, filter, and match result.
pub(crate) fn check(
    ctx: &mut ReactiveContext,
    registry: &SubscriptionRegistry,
    candidates: FnvHashSet<SubscriptionId>,
    table: &str,
    row: &[CellValue],
) -> FnvHashMap<SubscriptionId, FnvHashSet<usize>> {
    ctx.span_with(|ctx| {
        let num_candidates = candidates.len();
        let mut result: FnvHashMap<SubscriptionId, FnvHashSet<usize>> = FnvHashMap::default();

        for sub_id in candidates {
            let conditions = registry.conditions(sub_id);
            let sources = registry.sources(sub_id);
            if conditions.is_empty() {
                result.insert(sub_id, FnvHashSet::default());
                continue;
            }

            let mut triggered = FnvHashSet::default();
            for (idx, cond) in conditions.iter().enumerate() {
                if cond.table != table {
                    continue;
                }
                let matches = eval_predicate(&cond.verify_filter, &|col: ColumnRef| {
                    row.get(col.col).cloned().unwrap_or(CellValue::Null)
                });
                let filter = pretty_print_filter(&cond.verify_filter, sources);
                ctx.record_condition(idx, matches);
                ctx.span(ReactiveSpanOperation::ConditionEval {
                    sub_id,
                    idx,
                    filter,
                    matched: matches,
                }, |_| {});
                if matches {
                    triggered.insert(idx);
                }
            }

            if !triggered.is_empty() {
                result.insert(sub_id, triggered);
            }
        }

        let num_triggered = result.len();
        let op = ReactiveSpanOperation::Verify {
            candidates: num_candidates,
            triggered: num_triggered,
        };
        (op, result)
    })
}

fn pretty_print_filter(
    filter: &PlanFilterPredicate,
    sources: &[crate::planner::shared::plan::PlanSourceEntry],
) -> String {
    if matches!(filter, PlanFilterPredicate::None) {
        return "None".to_string();
    }
    let mut out = String::new();
    filter.pretty_print_to(&mut out, sources);
    out
}
