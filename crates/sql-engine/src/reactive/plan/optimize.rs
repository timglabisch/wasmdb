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
            let key_sets = extract_lookup_key_sets(&filter);
            let strategy = if key_sets.is_empty() {
                ReactiveLookupStrategy::TableScan
            } else {
                ReactiveLookupStrategy::IndexLookup { lookup_key_sets: key_sets }
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

/// Intermediate equality predicate: single value.
struct ExtractedEq {
    col_ref: ColumnRef,
    value: sql_parser::ast::Value,
}

/// Intermediate IN predicate: multiple values for one column.
struct ExtractedIn {
    col_ref: ColumnRef,
    values: Vec<sql_parser::ast::Value>,
}

/// Extract equality and IN predicates from an AND chain, then compute the
/// Cartesian product to produce one or more composite key sets.
///
/// Examples:
/// - `id = 1`                       → `[[(id, 1)]]`
/// - `id IN (1, 2)`                 → `[[(id, 1)], [(id, 2)]]`
/// - `status = 'a' AND id IN (1,2)` → `[[(status,'a'),(id,1)], [(status,'a'),(id,2)]]`
/// - `id IN (1,2) AND name IN ('a','b')` → 4 key sets (Cartesian product)
///
/// Returns empty Vec (→ TableScan fallback) when:
/// - No equality or IN predicates found
/// - Keys reference multiple source tables
fn extract_lookup_key_sets(pred: &PlanFilterPredicate) -> Vec<Vec<ReactiveLookupKey>> {
    let mut eqs = Vec::new();
    let mut ins = Vec::new();
    collect_extractable_keys(pred, &mut eqs, &mut ins);

    if eqs.is_empty() && ins.is_empty() {
        return Vec::new();
    }

    // All keys must reference the same source table.
    let first_source = eqs.first().map(|k| k.col_ref.source)
        .or_else(|| ins.first().map(|k| k.col_ref.source))
        .unwrap();
    if eqs.iter().any(|k| k.col_ref.source != first_source)
        || ins.iter().any(|k| k.col_ref.source != first_source)
    {
        return Vec::new();
    }

    // Fixed keys from equalities (present in every key set).
    let fixed: Vec<ReactiveLookupKey> = eqs.into_iter()
        .map(|k| ReactiveLookupKey { col: k.col_ref.col, value: k.value })
        .collect();

    if ins.is_empty() {
        // No IN predicates — single key set from equalities.
        return vec![fixed];
    }

    // Cartesian product: start with one set containing the fixed keys,
    // then for each IN predicate multiply by its values.
    let mut sets: Vec<Vec<ReactiveLookupKey>> = vec![fixed];
    for in_pred in ins {
        let mut expanded = Vec::with_capacity(sets.len() * in_pred.values.len());
        for val in &in_pred.values {
            for existing in &sets {
                let mut new_set = existing.clone();
                new_set.push(ReactiveLookupKey {
                    col: in_pred.col_ref.col,
                    value: val.clone(),
                });
                expanded.push(new_set);
            }
        }
        sets = expanded;
    }

    sets
}

/// Recursively collect Equals and In predicates from AND chains.
fn collect_extractable_keys(
    pred: &PlanFilterPredicate,
    eqs: &mut Vec<ExtractedEq>,
    ins: &mut Vec<ExtractedIn>,
) {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            collect_extractable_keys(l, eqs, ins);
            collect_extractable_keys(r, eqs, ins);
        }
        PlanFilterPredicate::Equals { col, value } => {
            eqs.push(ExtractedEq {
                col_ref: *col,
                value: value.clone(),
            });
        }
        PlanFilterPredicate::In { col, values } => {
            ins.push(ExtractedIn {
                col_ref: *col,
                values: values.clone(),
            });
        }
        _ => {} // Non-equality/IN predicates stay in verify_filter only
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

    /// Helper: assert IndexLookup with exactly N key sets, return them.
    fn assert_key_sets(opt: &OptimizedReactiveCondition, expected_sets: usize) -> &Vec<Vec<ReactiveLookupKey>> {
        match &opt.strategy {
            ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
                assert_eq!(lookup_key_sets.len(), expected_sets, "expected {expected_sets} key sets, got {}", lookup_key_sets.len());
                lookup_key_sets
            }
            _ => panic!("expected IndexLookup"),
        }
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
        let sets = assert_key_sets(&opt, 1);
        assert_eq!(sets[0].len(), 1);
        assert_eq!(sets[0][0].col, 0);
        assert!(matches!(sets[0][0].value, Value::Int(42)));
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::Equals { .. }));
    }

    #[test]
    fn test_and_chain_extracts_multiple_keys_with_full_verify_filter() {
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
        let sets = assert_key_sets(&opt, 1); // single composite key
        assert_eq!(sets[0].len(), 2);
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
        let sets = assert_key_sets(&opt, 1);
        assert_eq!(sets[0].len(), 1);
        assert_eq!(sets[0][0].col, 0);
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
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::And(_, _)));
    }

    // ── IN expansion tests ────────────────────────────────────────────

    #[test]
    fn test_in_expands_to_multiple_key_sets() {
        // REACTIVE(id IN (1, 2, 3)) → 3 key sets
        let filter = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
        };
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        let sets = assert_key_sets(&opt, 3);
        assert_eq!(sets[0], vec![ReactiveLookupKey { col: 0, value: Value::Int(1) }]);
        assert_eq!(sets[1], vec![ReactiveLookupKey { col: 0, value: Value::Int(2) }]);
        assert_eq!(sets[2], vec![ReactiveLookupKey { col: 0, value: Value::Int(3) }]);
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::In { .. }));
    }

    #[test]
    fn test_eq_and_in_cartesian_product() {
        // REACTIVE(status = 'active' AND id IN (1, 2)) → 2 composite key sets
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 1),
                value: Value::Text("active".into()),
            }),
            Box::new(PlanFilterPredicate::In {
                col: c(0, 0),
                values: vec![Value::Int(1), Value::Int(2)],
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        let sets = assert_key_sets(&opt, 2);
        // Each set has 2 keys: the fixed equality + one IN value
        assert_eq!(sets[0].len(), 2);
        assert_eq!(sets[1].len(), 2);
        // Fixed key (status) is first in each set
        assert_eq!(sets[0][0], ReactiveLookupKey { col: 1, value: Value::Text("active".into()) });
        assert_eq!(sets[0][1], ReactiveLookupKey { col: 0, value: Value::Int(1) });
        assert_eq!(sets[1][0], ReactiveLookupKey { col: 1, value: Value::Text("active".into()) });
        assert_eq!(sets[1][1], ReactiveLookupKey { col: 0, value: Value::Int(2) });
    }

    #[test]
    fn test_two_ins_cartesian_product() {
        // REACTIVE(id IN (1, 2) AND name IN ('a', 'b')) → 4 key sets
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::In {
                col: c(0, 0),
                values: vec![Value::Int(1), Value::Int(2)],
            }),
            Box::new(PlanFilterPredicate::In {
                col: c(0, 1),
                values: vec![Value::Text("a".into()), Value::Text("b".into())],
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        let sets = assert_key_sets(&opt, 4);
        for set in sets {
            assert_eq!(set.len(), 2); // each set has 2 keys (id + name)
        }
    }

    #[test]
    fn test_in_with_range_keeps_in_expansion() {
        // REACTIVE(id IN (1, 2) AND age > 30) → 2 key sets (only IN extracted, range in verify)
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::In {
                col: c(0, 0),
                values: vec![Value::Int(1), Value::Int(2)],
            }),
            Box::new(PlanFilterPredicate::GreaterThan {
                col: c(0, 2),
                value: Value::Int(30),
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        let sets = assert_key_sets(&opt, 2);
        assert_eq!(sets[0].len(), 1); // just the IN key
        assert_eq!(sets[1].len(), 1);
        assert!(matches!(opt.verify_filter, PlanFilterPredicate::And(_, _)));
    }

    #[test]
    fn test_in_with_mixed_sources_degrades() {
        // IN on source 0, equality on source 1 → TableScan
        let filter = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::In {
                col: c(0, 0),
                values: vec![Value::Int(1), Value::Int(2)],
            }),
            Box::new(PlanFilterPredicate::Equals {
                col: c(1, 0),
                value: Value::Int(3),
            }),
        );
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        assert!(matches!(opt.strategy, ReactiveLookupStrategy::TableScan));
    }

    #[test]
    fn test_in_single_value_same_as_equality() {
        // REACTIVE(id IN (42)) → 1 key set with 1 key (same as id = 42)
        let filter = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Int(42)],
        };
        let cond = ReactiveCondition {
            table: "users".into(),
            kind: ReactiveConditionKind::Condition { filter },
            source_idx: 0,
        };
        let opt = optimize_condition(cond);
        let sets = assert_key_sets(&opt, 1);
        assert_eq!(sets[0].len(), 1);
        assert_eq!(sets[0][0].col, 0);
        assert!(matches!(sets[0][0].value, Value::Int(42)));
    }
}
