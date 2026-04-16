//! Reactive optimizer: logical `ReactiveCondition` → `OptimizedReactiveCondition`.
//!
//! Passes:
//! 1. `or_to_in` (shared) — normalize OR-chains on the same column into IN.
//! 2. `lookup_keys`       — extract composite reverse-index keys from the
//!                          normalized predicate (AND chain of equalities + INs).
//!
//! The verify filter is ALWAYS preserved (in its normalized form) so correctness
//! never depends on the chosen lookup strategy.

pub(crate) mod lookup_keys;

use crate::planner::shared::optimize::or_to_in;
use crate::planner::shared::plan::PlanFilterPredicate;

use super::{
    OptimizedReactiveCondition, ReactiveCondition, ReactiveConditionKind,
    ReactiveLookupStrategy,
};

/// Optimize a set of logical reactive conditions into optimized conditions.
pub fn optimize(conditions: Vec<ReactiveCondition>) -> Vec<OptimizedReactiveCondition> {
    conditions.into_iter().map(optimize_condition).collect()
}

/// Optimize a single reactive condition.
pub(crate) fn optimize_condition(cond: ReactiveCondition) -> OptimizedReactiveCondition {
    match cond.kind {
        ReactiveConditionKind::TableLevel => OptimizedReactiveCondition {
            table: cond.table,
            source_idx: cond.source_idx,
            strategy: ReactiveLookupStrategy::TableScan,
            verify_filter: PlanFilterPredicate::None,
        },
        ReactiveConditionKind::Condition { filter } => {
            // Normalize OR-chains of equalities on the same column into IN,
            // so `extract_lookup_key_sets` can expand them into multiple hash
            // lookups (same mechanism as IN literals).
            let filter = or_to_in::normalize(filter);
            let key_sets = lookup_keys::extract_lookup_key_sets(&filter);
            let strategy = if key_sets.is_empty() {
                ReactiveLookupStrategy::TableScan
            } else {
                ReactiveLookupStrategy::IndexLookup { lookup_key_sets: key_sets }
            };
            OptimizedReactiveCondition {
                table: cond.table,
                source_idx: cond.source_idx,
                strategy,
                // Full (normalized) predicate — always evaluated after candidate lookup.
                verify_filter: filter,
            }
        }
    }
}
