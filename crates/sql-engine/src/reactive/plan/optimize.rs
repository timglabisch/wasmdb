//! Reactive condition optimization: extract lookup strategies from logical conditions.
//!
//! Takes logical `ReactiveCondition`s and produces `OptimizedReactiveCondition`s
//! with reverse-index lookup keys while ALWAYS preserving the full original predicate
//! as verify_filter for correctness.
//!
//! Mirrors the structure of `planner::sql::optimize::physical::scan_method` for query
//! optimization: lookup keys are extracted from equality predicates in AND chains,
//! but the verify filter is never stripped.

use crate::planner::plan::{ColumnRef, PlanFilterPredicate};
use super::*;

/// Optimize a set of logical reactive conditions into optimized conditions.
pub fn optimize(conditions: Vec<ReactiveCondition>) -> Vec<OptimizedReactiveCondition> {
    conditions.into_iter().map(optimize_condition).collect()
}

/// Optimize a single reactive condition.
fn optimize_condition(cond: ReactiveCondition) -> OptimizedReactiveCondition {
    match cond.kind {
        ReactiveConditionKind::TableLevel => OptimizedReactiveCondition {
            table: cond.table,
            source_idx: cond.source_idx,
            strategy: ReactiveLookupStrategy::TableScan,
            verify_filter: PlanFilterPredicate::None,
        },
        ReactiveConditionKind::Condition { filter } => {
            let lookup_keys = extract_lookup_keys(&filter);
            let strategy = if lookup_keys.is_empty() {
                ReactiveLookupStrategy::TableScan
            } else {
                ReactiveLookupStrategy::IndexLookup { lookup_keys }
            };
            OptimizedReactiveCondition {
                table: cond.table,
                source_idx: cond.source_idx,
                strategy,
                // ALWAYS preserve the full original predicate as verify filter.
                // Lookup keys narrow candidates, verify filter ensures correctness.
                verify_filter: filter,
            }
        }
    }
}

// ── Lookup key extraction ───────────────────────────────────────────────

/// Intermediate representation for extracted equality predicates.
struct ExtractedKey {
    col_ref: ColumnRef,
    value: sql_parser::ast::Value,
}

/// Extract equality predicates from an AND chain as lookup keys.
///
/// Only `Equals { col, value }` predicates from top-level AND chains are extracted.
/// All keys must reference the same source table; otherwise returns empty Vec
/// (graceful degradation to TableScan).
fn extract_lookup_keys(pred: &PlanFilterPredicate) -> Vec<ReactiveLookupKey> {
    let mut keys = Vec::new();
    collect_eq_keys(pred, &mut keys);

    if keys.is_empty() {
        return Vec::new();
    }

    // All keys must reference the same source table.
    let first_source = keys[0].col_ref.source;
    if keys.iter().any(|k| k.col_ref.source != first_source) {
        return Vec::new();
    }

    keys.into_iter()
        .map(|k| ReactiveLookupKey {
            col: k.col_ref.col,
            value: k.value,
        })
        .collect()
}

/// Recursively collect Equals predicates from AND chains.
fn collect_eq_keys(pred: &PlanFilterPredicate, keys: &mut Vec<ExtractedKey>) {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            collect_eq_keys(l, keys);
            collect_eq_keys(r, keys);
        }
        PlanFilterPredicate::Equals { col, value } => {
            keys.push(ExtractedKey {
                col_ref: *col,
                value: value.clone(),
            });
        }
        _ => {} // Non-equality predicates stay in verify_filter only
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;
    use crate::planner::plan::ColumnRef;

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    #[test]
    fn test_table_level_to_table_scan() {
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::TableLevel,
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        assert!(matches!(opt.strategy, ReactiveLookupStrategy::TableScan));
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::None));
    }

    #[test]
    fn test_single_equality_extracts_lookup_key() {
        let filter = PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Int(42),
        };
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter: filter.clone() },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        match &opt.strategy {
            ReactiveLookupStrategy::IndexLookup { lookup_keys } => {
                assert_eq!(lookup_keys.len(), 1);
                assert_eq!(lookup_keys[0].col, 0);
                assert!(matches!(lookup_keys[0].value, Value::Int(42)));
            }
            _ => panic!("expected IndexLookup"),
        }
        // verify_filter must be the FULL original predicate
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::Equals { .. }));
    }

    #[test]
    fn test_and_chain_extracts_multiple_keys_with_full_verify_filter() {
        // This is the bug regression test:
        // reactive(id = 4 AND name = 'foo') must keep the full AND as verify_filter
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 0),
                value: Value::Int(4),
            }),
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 1),
                value: Value::Text("foo".into()),
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        match &opt.strategy {
            ReactiveLookupStrategy::IndexLookup { lookup_keys } => {
                assert_eq!(lookup_keys.len(), 2);
            }
            _ => panic!("expected IndexLookup"),
        }
        // verify_filter must be the FULL And(...) predicate, not None!
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::And(_, _)));
    }

    #[test]
    fn test_non_equality_only_becomes_table_scan_with_filter() {
        let filter = PlanFilterPredicate::GreaterThan {
            col: c(0, 2),
            value: Value::Int(18),
        };
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        assert!(matches!(opt.strategy, ReactiveLookupStrategy::TableScan));
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::GreaterThan { .. }));
    }

    #[test]
    fn test_mixed_eq_and_range_extracts_eq_keys_only() {
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 0),
                value: Value::Int(1),
            }),
            Box::new(PlanFilterPredicate::GreaterThan {
                col: c(0, 2),
                value: Value::Int(100),
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        match &opt.strategy {
            ReactiveLookupStrategy::IndexLookup { lookup_keys } => {
                assert_eq!(lookup_keys.len(), 1);
                assert_eq!(lookup_keys[0].col, 0);
            }
            _ => panic!("expected IndexLookup"),
        }
        // verify_filter is the full AND(Eq, Gt)
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::And(_, _)));
    }

    #[test]
    fn test_mixed_sources_degrades_to_table_scan() {
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 0),
                value: Value::Int(1),
            }),
            Box::new(PlanFilterPredicate::Equals {
                col: c(1, 0),
                value: Value::Int(2),
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        assert!(matches!(opt.strategy, ReactiveLookupStrategy::TableScan));
        // Still has verify_filter
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::And(_, _)));
    }
}
