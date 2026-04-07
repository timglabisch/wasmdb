//! Reactive query subscription registry with reverse index.
//!
//! When a query contains `INVALIDATE_ON(condition)`, the engine can register
//! a subscription. On INSERT/UPDATE/DELETE the registry determines which
//! subscriptions are affected using a reverse hash index + verify filter.

use std::collections::{HashMap, HashSet};

use crate::execute::filter_row::eval_predicate;
use crate::execute::{value_to_cell, Columns, Params};
use crate::planner::plan::*;
use crate::storage::CellValue;

/// Unique subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubId(pub u64);

/// Reverse-index key: (table, column, value).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReverseKey {
    table: String,
    col: usize,
    value: CellValue,
}

/// A registered subscription.
struct Subscription {
    #[allow(dead_code)]
    id: SubId,
    conditions: Vec<InvalidationCondition>,
    #[allow(dead_code)]
    strategy: InvalidationStrategy,
    #[allow(dead_code)]
    plan: ExecutionPlan,
    #[allow(dead_code)]
    params: Params,
    #[allow(dead_code)]
    cached_result: Option<Columns>,
    /// For deregistration: which keys belong to this subscription.
    reverse_keys: Vec<ReverseKey>,
}

/// Manages subscriptions and the reverse index.
pub struct SubscriptionRegistry {
    next_id: u64,
    subscriptions: HashMap<SubId, Subscription>,
    reverse_index: HashMap<ReverseKey, Vec<SubId>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            subscriptions: HashMap::new(),
            reverse_index: HashMap::new(),
        }
    }

    /// Register a subscription. The plan must have params already resolved.
    pub fn subscribe(
        &mut self,
        plan: &ExecutionPlan,
        params: &Params,
        initial_result: Columns,
    ) -> SubId {
        let id = SubId(self.next_id);
        self.next_id += 1;
        let reactive = plan
            .reactive
            .as_ref()
            .expect("plan has no reactive metadata");

        let mut reverse_keys = Vec::new();
        for cond in &reactive.conditions {
            for key in &cond.index_keys {
                let cell = value_to_cell(&key.value);
                let rk = ReverseKey {
                    table: key.table.clone(),
                    col: key.col,
                    value: cell,
                };
                self.reverse_index
                    .entry(rk.clone())
                    .or_default()
                    .push(id);
                reverse_keys.push(rk);
            }
        }

        self.subscriptions.insert(
            id,
            Subscription {
                id,
                conditions: reactive.conditions.clone(),
                strategy: reactive.strategy,
                plan: plan.clone(),
                params: params.clone(),
                cached_result: Some(initial_result),
                reverse_keys,
            },
        );

        id
    }

    /// Remove a subscription and clean up reverse index entries.
    pub fn unsubscribe(&mut self, id: SubId) {
        if let Some(sub) = self.subscriptions.remove(&id) {
            for rk in &sub.reverse_keys {
                if let Some(subs) = self.reverse_index.get_mut(rk) {
                    subs.retain(|s| *s != id);
                    if subs.is_empty() {
                        self.reverse_index.remove(rk);
                    }
                }
            }
        }
    }

    /// Check which subscriptions are affected by an INSERT.
    pub fn on_insert(&self, table: &str, new_row: &[CellValue]) -> Vec<SubId> {
        self.check_row(table, new_row)
    }

    /// Check which subscriptions are affected by a DELETE.
    pub fn on_delete(&self, table: &str, old_row: &[CellValue]) -> Vec<SubId> {
        self.check_row(table, old_row)
    }

    /// Check which subscriptions are affected by an UPDATE.
    pub fn on_update(
        &self,
        table: &str,
        old_row: &[CellValue],
        new_row: &[CellValue],
    ) -> Vec<SubId> {
        let mut affected = HashSet::new();
        affected.extend(self.check_row(table, old_row));
        affected.extend(self.check_row(table, new_row));
        affected.into_iter().collect()
    }

    fn check_row(&self, table: &str, row: &[CellValue]) -> Vec<SubId> {
        let mut candidates = HashSet::new();

        // 1. Reverse-index lookup per column
        for (col_idx, cell) in row.iter().enumerate() {
            let rk = ReverseKey {
                table: table.to_string(),
                col: col_idx,
                value: cell.clone(),
            };
            if let Some(subs) = self.reverse_index.get(&rk) {
                candidates.extend(subs);
            }
        }

        // 2. Verify filter
        candidates
            .into_iter()
            .filter(|sub_id| {
                let sub = &self.subscriptions[sub_id];
                sub.conditions.iter().any(|cond| {
                    if cond.table != table {
                        return false;
                    }
                    eval_condition(&cond.verify_filter, row)
                })
            })
            .collect()
    }
}

/// Evaluate a PlanFilterPredicate against a single row.
/// ColumnRef.source is always 0 (single-table condition).
fn eval_condition(pred: &PlanFilterPredicate, row: &[CellValue]) -> bool {
    eval_predicate(pred, &|col: ColumnRef| {
        row.get(col.col).cloned().unwrap_or(CellValue::Null)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;

    fn make_plan(conditions: Vec<InvalidationCondition>, strategy: InvalidationStrategy) -> ExecutionPlan {
        ExecutionPlan {
            materializations: vec![],
            main: PlanSelect {
                sources: vec![],
                filter: PlanFilterPredicate::None,
                group_by: vec![],
                aggregates: vec![],
                order_by: vec![],
                limit: None,
                result_columns: vec![],
            },
            reactive: Some(ReactiveMetadata {
                conditions,
                strategy,
            }),
        }
    }

    fn simple_condition(table: &str, col: usize, value: Value) -> InvalidationCondition {
        InvalidationCondition {
            table: table.into(),
            index_keys: vec![InvalidationKey {
                table: table.into(),
                col,
                value,
            }],
            verify_filter: PlanFilterPredicate::None,
            source_idx: 0,
        }
    }

    #[test]
    fn test_subscribe_and_on_insert_matching() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        let sub_id = reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_insert("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_subscribe_and_on_insert_non_matching() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_insert("users", &[CellValue::I64(99), CellValue::Str("Bob".into())]);
        assert!(affected.is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        let sub_id = reg.subscribe(&plan, &HashMap::new(), vec![]);
        reg.unsubscribe(sub_id);

        let affected = reg.on_insert("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert!(affected.is_empty());
    }

    #[test]
    fn test_on_delete_matching() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        let sub_id = reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_delete("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_on_update_old_matches() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        let sub_id = reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_update(
            "users",
            &[CellValue::I64(42), CellValue::Str("Alice".into())],
            &[CellValue::I64(42), CellValue::Str("Bobby".into())],
        );
        assert!(affected.contains(&sub_id));
    }

    #[test]
    fn test_on_update_neither_matches() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_update(
            "users",
            &[CellValue::I64(99), CellValue::Str("X".into())],
            &[CellValue::I64(99), CellValue::Str("Y".into())],
        );
        assert!(affected.is_empty());
    }

    #[test]
    fn test_verify_filter() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![InvalidationCondition {
                table: "orders".into(),
                index_keys: vec![InvalidationKey {
                    table: "orders".into(),
                    col: 1, // user_id
                    value: Value::Int(42),
                }],
                verify_filter: PlanFilterPredicate::GreaterThan {
                    col: ColumnRef { source: 0, col: 2 }, // amount
                    value: Value::Int(100),
                },
                source_idx: 0,
            }],
            InvalidationStrategy::ReExecute,
        );
        let sub_id = reg.subscribe(&plan, &HashMap::new(), vec![]);

        // amount=50 → verify fails
        let affected = reg.on_insert("orders", &[CellValue::I64(1), CellValue::I64(42), CellValue::I64(50)]);
        assert!(affected.is_empty());

        // amount=200 → verify passes
        let affected = reg.on_insert("orders", &[CellValue::I64(2), CellValue::I64(42), CellValue::I64(200)]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_multiple_subscriptions() {
        let mut reg = SubscriptionRegistry::new();
        let plan1 = make_plan(
            vec![simple_condition("users", 0, Value::Int(1))],
            InvalidationStrategy::ReExecute,
        );
        let plan2 = make_plan(
            vec![simple_condition("users", 0, Value::Int(2))],
            InvalidationStrategy::ReExecute,
        );
        let sub1 = reg.subscribe(&plan1, &HashMap::new(), vec![]);
        let sub2 = reg.subscribe(&plan2, &HashMap::new(), vec![]);

        let affected = reg.on_insert("users", &[CellValue::I64(1), CellValue::Str("A".into())]);
        assert_eq!(affected, vec![sub1]);

        let affected = reg.on_insert("users", &[CellValue::I64(2), CellValue::Str("B".into())]);
        assert_eq!(affected, vec![sub2]);

        let affected = reg.on_insert("users", &[CellValue::I64(99), CellValue::Str("C".into())]);
        assert!(affected.is_empty());
    }

    #[test]
    fn test_different_table_not_affected() {
        let mut reg = SubscriptionRegistry::new();
        let plan = make_plan(
            vec![simple_condition("users", 0, Value::Int(42))],
            InvalidationStrategy::ReExecute,
        );
        reg.subscribe(&plan, &HashMap::new(), vec![]);

        let affected = reg.on_insert("orders", &[CellValue::I64(42), CellValue::I64(1)]);
        assert!(affected.is_empty());
    }
}
