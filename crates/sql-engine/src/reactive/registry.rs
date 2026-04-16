//! Reactive subscription registry — state management.
//!
//! Manages subscriptions and the reverse index. The registry is pure state:
//! subscribe/unsubscribe manage the data structures, the execution logic
//! (checking which subscriptions are affected by a mutation) lives in
//! `reactive::execute`.

use std::collections::{HashMap, HashSet};

use crate::execute::bind::{resolve_filter, resolve_value};
use crate::execute::value_to_cell;
use crate::execute::{ExecuteError, Params};
use crate::reactive::plan::{OptimizedReactiveCondition, ReactiveLookupStrategy};
use crate::storage::CellValue;

/// Unique subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubId(pub u64);

/// Materialized lookup key used as hash-map key in the reverse index.
///
/// This is the runtime counterpart of `ReactiveLookupKey`: after parameter
/// binding, the AST `Value` is converted to a concrete `CellValue` so it
/// can be matched against incoming rows in O(1).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MaterializedLookupKey {
    pub(crate) table: String,
    pub(crate) col: usize,
    pub(crate) value: CellValue,
}

/// A registered subscription.
struct Subscription {
    conditions: Vec<OptimizedReactiveCondition>,
    /// For deregistration: which keys belong to this subscription.
    reverse_keys: Vec<MaterializedLookupKey>,
}

/// Manages subscriptions and the reverse index.
pub struct SubscriptionRegistry {
    next_id: u64,
    subscriptions: HashMap<SubId, Subscription>,
    reverse_index: HashMap<MaterializedLookupKey, Vec<SubId>>,
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

    /// Register a subscription: bind parameters and insert into the reverse index.
    pub fn subscribe(
        &mut self,
        conditions: &[OptimizedReactiveCondition],
        params: &Params,
    ) -> Result<SubId, ExecuteError> {
        let resolved = resolve_conditions(conditions, params)?;

        let id = SubId(self.next_id);
        self.next_id += 1;

        let mut reverse_keys = Vec::new();
        for cond in &resolved {
            match &cond.strategy {
                ReactiveLookupStrategy::TableScan => {
                    self.table_subs.entry(cond.table.clone()).or_default().insert(id);
                }
                ReactiveLookupStrategy::IndexLookup { lookup_keys } => {
                    for key in lookup_keys {
                        let cell = value_to_cell(&key.value);
                        let rk = MaterializedLookupKey {
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
                conditions: resolved,
                reverse_keys,
            },
        );

        Ok(id)
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
        for subs in self.table_subs.values_mut() {
            subs.remove(&id);
        }
        self.table_subs.retain(|_, subs| !subs.is_empty());
    }

    // ── Accessors for reactive::execute ────────────────────────────────

    pub(crate) fn table_level_subs(&self, table: &str) -> Option<&HashSet<SubId>> {
        self.table_subs.get(table)
    }

    pub(crate) fn index_lookup(&self, key: &MaterializedLookupKey) -> Option<&[SubId]> {
        self.reverse_index.get(key).map(|v| v.as_slice())
    }

    pub(crate) fn conditions(&self, id: SubId) -> &[OptimizedReactiveCondition] {
        self.subscriptions
            .get(&id)
            .map(|s| s.conditions.as_slice())
            .unwrap_or(&[])
    }
}

// ── Internal: parameter binding ────────────────────────────────────────

fn resolve_conditions(
    conditions: &[OptimizedReactiveCondition],
    params: &Params,
) -> Result<Vec<OptimizedReactiveCondition>, ExecuteError> {
    if params.is_empty() {
        return Ok(conditions.to_vec());
    }
    let mut resolved = conditions.to_vec();
    for cond in &mut resolved {
        if let ReactiveLookupStrategy::IndexLookup { ref mut lookup_keys } = cond.strategy {
            for key in lookup_keys.iter_mut() {
                key.value = resolve_value(&key.value, params)?;
            }
        }
        cond.verify_filter = resolve_filter(&cond.verify_filter, params)?;
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::{ColumnRef, PlanFilterPredicate};
    use crate::reactive::plan::ReactiveLookupKey;
    use sql_parser::ast::Value;
    use std::collections::HashMap;

    fn cond_eq(table: &str, col: usize, value: Value) -> OptimizedReactiveCondition {
        let verify = PlanFilterPredicate::Equals {
            col: ColumnRef { source: 0, col },
            value: value.clone(),
        };
        OptimizedReactiveCondition {
            table: table.into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::IndexLookup {
                lookup_keys: vec![ReactiveLookupKey { col, value }],
            },
            verify_filter: verify,
        }
    }

    fn cond_table(table: &str) -> OptimizedReactiveCondition {
        OptimizedReactiveCondition {
            table: table.into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::TableScan,
            verify_filter: PlanFilterPredicate::None,
        }
    }

    fn empty_params() -> Params {
        HashMap::new()
    }

    #[test]
    fn test_subscribe_and_accessors() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &empty_params()).unwrap();
        assert_eq!(reg.subscription_count(), 1);
        assert_eq!(reg.conditions(sub_id).len(), 1);
    }

    #[test]
    fn test_subscribe_table_level() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")], &empty_params()).unwrap();
        assert!(reg.table_level_subs("users").unwrap().contains(&sub_id));
        assert!(reg.table_level_subs("orders").is_none());
    }

    #[test]
    fn test_subscribe_index_lookup() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &empty_params()).unwrap();
        let key = MaterializedLookupKey {
            table: "users".into(),
            col: 0,
            value: CellValue::I64(42),
        };
        assert_eq!(reg.index_lookup(&key), Some(&[sub_id][..]));
    }

    #[test]
    fn test_unsubscribe_cleans_reverse_index() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &empty_params()).unwrap();
        reg.unsubscribe(sub_id);
        assert_eq!(reg.subscription_count(), 0);
        assert_eq!(reg.reverse_index_size(), 0);
    }

    #[test]
    fn test_unsubscribe_cleans_table_subs() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")], &empty_params()).unwrap();
        reg.unsubscribe(sub_id);
        assert!(reg.table_level_subs("users").is_none());
    }

    #[test]
    fn test_subscribe_with_params() {
        use crate::execute::ParamValue;
        let mut reg = SubscriptionRegistry::new();
        let cond = OptimizedReactiveCondition {
            table: "users".into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::IndexLookup {
                lookup_keys: vec![ReactiveLookupKey {
                    col: 0,
                    value: Value::Placeholder("uid".into()),
                }],
            },
            verify_filter: PlanFilterPredicate::Equals {
                col: ColumnRef { source: 0, col: 0 },
                value: Value::Placeholder("uid".into()),
            },
        };
        let params = HashMap::from([("uid".into(), ParamValue::Int(7))]);
        let sub_id = reg.subscribe(&[cond], &params).unwrap();
        // Lookup key should be resolved to 7
        let key = MaterializedLookupKey {
            table: "users".into(),
            col: 0,
            value: CellValue::I64(7),
        };
        assert_eq!(reg.index_lookup(&key), Some(&[sub_id][..]));
    }
}
