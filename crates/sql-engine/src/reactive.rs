//! Reactive query subscription registry with reverse index.
//!
//! When a query contains `REACTIVE(condition)`, the engine can register
//! a subscription. On INSERT/UPDATE/DELETE the registry determines which
//! subscriptions are affected using a reverse hash index + verify filter.

use std::collections::{HashMap, HashSet};

use crate::execute::filter_row::eval_predicate;
use crate::execute::value_to_cell;
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
    conditions: Vec<ReactiveCondition>,
    /// For deregistration: which keys belong to this subscription.
    reverse_keys: Vec<ReverseKey>,
}

/// Manages subscriptions and the reverse index.
pub struct SubscriptionRegistry {
    next_id: u64,
    subscriptions: HashMap<SubId, Subscription>,
    reverse_index: HashMap<ReverseKey, Vec<SubId>>,
    /// Table-level subscriptions: any mutation on the table triggers the subscription.
    table_subs: HashMap<String, HashSet<SubId>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            subscriptions: HashMap::new(),
            reverse_index: HashMap::new(),
            table_subs: HashMap::new(),
        }
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn table_subscriptions(&self) -> &HashMap<String, HashSet<SubId>> {
        &self.table_subs
    }

    pub fn reverse_index_size(&self) -> usize {
        self.reverse_index.len()
    }

    /// Register a subscription from a plan with reactive metadata.
    /// The plan must have params already resolved.
    pub fn subscribe(
        &mut self,
        conditions: &[ReactiveCondition],
    ) -> SubId {
        let id = SubId(self.next_id);
        self.next_id += 1;

        let mut reverse_keys = Vec::new();
        for cond in conditions {
            match &cond.kind {
                ReactiveConditionKind::TableLevel => {
                    self.table_subs.entry(cond.table.clone()).or_default().insert(id);
                }
                ReactiveConditionKind::Condition { eq_keys, .. } => {
                    for key in eq_keys {
                        let cell = value_to_cell(&key.value);
                        let rk = ReverseKey {
                            table: cond.table.clone(),
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
            }
        }

        self.subscriptions.insert(
            id,
            Subscription {
                conditions: conditions.to_vec(),
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
        // Clean up table-level subscriptions.
        for subs in self.table_subs.values_mut() {
            subs.remove(&id);
        }
        self.table_subs.retain(|_, subs| !subs.is_empty());
    }

    /// Check which subscriptions are affected by an INSERT.
    pub fn on_insert(&self, table: &str, new_row: &[CellValue]) -> Vec<SubId> {
        self.check_row_detailed(table, new_row).into_keys().collect()
    }

    /// Check which subscriptions are affected by a DELETE.
    pub fn on_delete(&self, table: &str, old_row: &[CellValue]) -> Vec<SubId> {
        self.check_row_detailed(table, old_row).into_keys().collect()
    }

    /// Check which subscriptions are affected by an UPDATE.
    pub fn on_update(
        &self,
        table: &str,
        old_row: &[CellValue],
        new_row: &[CellValue],
    ) -> Vec<SubId> {
        let mut affected = self.check_row_detailed(table, old_row);
        for (sub_id, indices) in self.check_row_detailed(table, new_row) {
            affected.entry(sub_id).or_default().extend(indices);
        }
        affected.into_keys().collect()
    }

    /// Like on_insert but also returns which condition indices triggered per subscription.
    pub fn on_insert_detailed(&self, table: &str, new_row: &[CellValue]) -> HashMap<SubId, HashSet<usize>> {
        self.check_row_detailed(table, new_row)
    }

    /// Like on_delete but also returns which condition indices triggered per subscription.
    pub fn on_delete_detailed(&self, table: &str, old_row: &[CellValue]) -> HashMap<SubId, HashSet<usize>> {
        self.check_row_detailed(table, old_row)
    }

    fn check_row_detailed(&self, table: &str, row: &[CellValue]) -> HashMap<SubId, HashSet<usize>> {
        let mut candidates = HashSet::new();

        // 1. Table-level subscriptions always match.
        if let Some(subs) = self.table_subs.get(table) {
            candidates.extend(subs);
        }

        // 2. Reverse-index lookup per column (fine-grained REACTIVE).
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

        // 3. Verify filter — collect which condition indices triggered.
        let mut result: HashMap<SubId, HashSet<usize>> = HashMap::new();
        for sub_id in candidates {
            let sub = &self.subscriptions[&sub_id];
            if sub.conditions.is_empty() {
                result.insert(sub_id, HashSet::new());
                continue;
            }
            let mut triggered = HashSet::new();
            for (idx, cond) in sub.conditions.iter().enumerate() {
                if cond.table != table {
                    continue;
                }
                let matches = match &cond.kind {
                    ReactiveConditionKind::TableLevel => true,
                    ReactiveConditionKind::Condition { verify_filter, .. } => {
                        eval_predicate(verify_filter, &|col: ColumnRef| {
                            row.get(col.col).cloned().unwrap_or(CellValue::Null)
                        })
                    }
                };
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;

    fn cond_eq(table: &str, col: usize, value: Value) -> ReactiveCondition {
        ReactiveCondition {
            table: table.into(),
            kind: ReactiveConditionKind::Condition {
                eq_keys: vec![ReactiveKey { col, value }],
                verify_filter: PlanFilterPredicate::None,
            },
            source_idx: 0,
        }
    }

    fn cond_table(table: &str) -> ReactiveCondition {
        ReactiveCondition {
            table: table.into(),
            kind: ReactiveConditionKind::TableLevel,
            source_idx: 0,
        }
    }

    #[test]
    fn test_insert_matching() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        let affected = reg.on_insert("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_insert_non_matching() {
        let mut reg = SubscriptionRegistry::new();
        reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        let affected = reg.on_insert("users", &[CellValue::I64(99), CellValue::Str("Bob".into())]);
        assert!(affected.is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        reg.unsubscribe(sub_id);
        let affected = reg.on_insert("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert!(affected.is_empty());
    }

    #[test]
    fn test_delete_matching() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        let affected = reg.on_delete("users", &[CellValue::I64(42), CellValue::Str("Alice".into())]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_update_old_matches() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        let affected = reg.on_update(
            "users",
            &[CellValue::I64(42), CellValue::Str("Alice".into())],
            &[CellValue::I64(42), CellValue::Str("Bobby".into())],
        );
        assert!(affected.contains(&sub_id));
    }

    #[test]
    fn test_update_neither_matches() {
        let mut reg = SubscriptionRegistry::new();
        reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
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
        let sub_id = reg.subscribe(&[ReactiveCondition {
            table: "orders".into(),
            kind: ReactiveConditionKind::Condition {
                eq_keys: vec![ReactiveKey { col: 1, value: Value::Int(42) }],
                verify_filter: PlanFilterPredicate::GreaterThan {
                    col: ColumnRef { source: 0, col: 2 },
                    value: Value::Int(100),
                },
            },
            source_idx: 0,
        }]);

        let affected = reg.on_insert("orders", &[CellValue::I64(1), CellValue::I64(42), CellValue::I64(50)]);
        assert!(affected.is_empty());

        let affected = reg.on_insert("orders", &[CellValue::I64(2), CellValue::I64(42), CellValue::I64(200)]);
        assert_eq!(affected, vec![sub_id]);
    }

    #[test]
    fn test_multiple_subscriptions() {
        let mut reg = SubscriptionRegistry::new();
        let sub1 = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))]);
        let sub2 = reg.subscribe(&[cond_eq("users", 0, Value::Int(2))]);

        assert_eq!(reg.on_insert("users", &[CellValue::I64(1), CellValue::Str("A".into())]), vec![sub1]);
        assert_eq!(reg.on_insert("users", &[CellValue::I64(2), CellValue::Str("B".into())]), vec![sub2]);
        assert!(reg.on_insert("users", &[CellValue::I64(99), CellValue::Str("C".into())]).is_empty());
    }

    #[test]
    fn test_different_table_not_affected() {
        let mut reg = SubscriptionRegistry::new();
        reg.subscribe(&[cond_eq("users", 0, Value::Int(42))]);
        assert!(reg.on_insert("orders", &[CellValue::I64(42), CellValue::I64(1)]).is_empty());
    }

    #[test]
    fn test_table_level() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")]);
        assert_eq!(reg.on_insert("users", &[CellValue::I64(1), CellValue::Str("Alice".into())]), vec![sub_id]);
        assert_eq!(reg.on_insert("users", &[CellValue::I64(99), CellValue::Str("Bob".into())]), vec![sub_id]);
        assert!(reg.on_insert("orders", &[CellValue::I64(1), CellValue::I64(1)]).is_empty());
    }

    #[test]
    fn test_table_level_unsubscribe() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")]);
        reg.unsubscribe(sub_id);
        assert!(reg.on_insert("users", &[CellValue::I64(1), CellValue::Str("Alice".into())]).is_empty());
    }
}
