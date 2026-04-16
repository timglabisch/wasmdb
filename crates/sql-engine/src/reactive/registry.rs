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
use crate::planner::plan::PlanSourceEntry;
use crate::reactive::plan::{OptimizedReactiveCondition, ReactiveLookupStrategy};
use crate::storage::CellValue;

/// Unique subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubId(pub u64);

/// Composite lookup key: table + sorted list of (col, value) pairs.
///
/// For `REACTIVE(id = 1 AND name = 'Alice')` this becomes:
/// `{ table: "users", cols: [(0, I64(1)), (1, Str("Alice"))] }`.
///
/// A single equality `REACTIVE(id = 1)` produces `cols: [(0, I64(1))]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CompositeKey {
    pub(crate) table: String,
    pub(crate) cols: Vec<(usize, CellValue)>,
}

/// Which columns a composite key uses — for building keys at mutation time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ColumnSet {
    pub(crate) table: String,
    pub(crate) cols: Vec<usize>,
}

/// A registered subscription.
struct Subscription {
    conditions: Vec<OptimizedReactiveCondition>,
    sources: Vec<PlanSourceEntry>,
    /// For deregistration: which composite keys belong to this subscription.
    composite_keys: Vec<CompositeKey>,
}

/// Manages subscriptions and the reverse index.
pub struct SubscriptionRegistry {
    next_id: u64,
    subscriptions: HashMap<SubId, Subscription>,
    /// Composite reverse index: composite key → subscriptions.
    reverse_index: HashMap<CompositeKey, Vec<SubId>>,
    /// Which column-sets are registered per table — for building keys at mutation time.
    column_sets: HashMap<String, HashSet<ColumnSet>>,
    /// Table-level subscriptions: any mutation on the table triggers the subscription.
    table_subs: HashMap<String, HashSet<SubId>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            subscriptions: HashMap::new(),
            reverse_index: HashMap::new(),
            column_sets: HashMap::new(),
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
        sources: &[PlanSourceEntry],
        params: &Params,
    ) -> Result<SubId, ExecuteError> {
        let resolved = resolve_conditions(conditions, params)?;

        let id = SubId(self.next_id);
        self.next_id += 1;

        let mut composite_keys = Vec::new();
        for cond in &resolved {
            match &cond.strategy {
                ReactiveLookupStrategy::TableScan => {
                    self.table_subs.entry(cond.table.clone()).or_default().insert(id);
                }
                ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
                    for keys in lookup_key_sets {
                        let mut cols: Vec<(usize, CellValue)> = keys
                            .iter()
                            .map(|k| (k.col, value_to_cell(&k.value)))
                            .collect();
                        cols.sort_by_key(|(col, _)| *col);

                        let ck = CompositeKey {
                            table: cond.table.clone(),
                            cols,
                        };
                        self.reverse_index
                            .entry(ck.clone())
                            .or_default()
                            .push(id);

                        // Track the column-set for this table.
                        let cs = ColumnSet {
                            table: cond.table.clone(),
                            cols: ck.cols.iter().map(|(c, _)| *c).collect(),
                        };
                        self.column_sets
                            .entry(cond.table.clone())
                            .or_default()
                            .insert(cs);

                        composite_keys.push(ck);
                    }
                }
            }
        }

        self.subscriptions.insert(
            id,
            Subscription {
                conditions: resolved,
                sources: sources.to_vec(),
                composite_keys,
            },
        );

        Ok(id)
    }

    /// Remove a subscription and clean up reverse index entries.
    pub fn unsubscribe(&mut self, id: SubId) {
        if let Some(sub) = self.subscriptions.remove(&id) {
            for ck in &sub.composite_keys {
                if let Some(subs) = self.reverse_index.get_mut(ck) {
                    subs.retain(|s| *s != id);
                    if subs.is_empty() {
                        self.reverse_index.remove(ck);
                        // Check if this column-set is still in use.
                        let cs = ColumnSet {
                            table: ck.table.clone(),
                            cols: ck.cols.iter().map(|(c, _)| *c).collect(),
                        };
                        let still_used = self.reverse_index.keys().any(|k| {
                            k.table == cs.table
                                && k.cols.iter().map(|(c, _)| *c).collect::<Vec<_>>() == cs.cols
                        });
                        if !still_used {
                            if let Some(sets) = self.column_sets.get_mut(&cs.table) {
                                sets.remove(&cs);
                                if sets.is_empty() {
                                    self.column_sets.remove(&cs.table);
                                }
                            }
                        }
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

    /// Look up subscriptions by composite key.
    pub(crate) fn composite_lookup(&self, key: &CompositeKey) -> Option<&[SubId]> {
        self.reverse_index.get(key).map(|v| v.as_slice())
    }

    /// Get the registered column-sets for a table — used to build keys at mutation time.
    pub(crate) fn column_sets_for_table(&self, table: &str) -> Option<&HashSet<ColumnSet>> {
        self.column_sets.get(table)
    }

    pub(crate) fn conditions(&self, id: SubId) -> &[OptimizedReactiveCondition] {
        self.subscriptions
            .get(&id)
            .map(|s| s.conditions.as_slice())
            .unwrap_or(&[])
    }

    pub(crate) fn sources(&self, id: SubId) -> &[PlanSourceEntry] {
        self.subscriptions
            .get(&id)
            .map(|s| s.sources.as_slice())
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
        if let ReactiveLookupStrategy::IndexLookup { ref mut lookup_key_sets } = cond.strategy {
            for keys in lookup_key_sets.iter_mut() {
                for key in keys.iter_mut() {
                    key.value = resolve_value(&key.value, params)?;
                }
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
                lookup_key_sets: vec![vec![ReactiveLookupKey { col, value }]],
            },
            verify_filter: verify,
        }
    }

    fn cond_multi_eq(table: &str, keys: &[(usize, Value)]) -> OptimizedReactiveCondition {
        let lookup_keys: Vec<ReactiveLookupKey> = keys
            .iter()
            .map(|(col, value)| ReactiveLookupKey { col: *col, value: value.clone() })
            .collect();
        // Build AND chain for verify filter
        let filters: Vec<PlanFilterPredicate> = keys
            .iter()
            .map(|(col, value)| PlanFilterPredicate::Equals {
                col: ColumnRef { source: 0, col: *col },
                value: value.clone(),
            })
            .collect();
        let verify = filters.into_iter().reduce(|a, b| {
            PlanFilterPredicate::And(Box::new(a), Box::new(b))
        }).unwrap();
        OptimizedReactiveCondition {
            table: table.into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::IndexLookup { lookup_key_sets: vec![lookup_keys] },
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
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &[], &empty_params()).unwrap();
        assert_eq!(reg.subscription_count(), 1);
        assert_eq!(reg.conditions(sub_id).len(), 1);
    }

    #[test]
    fn test_subscribe_table_level() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")], &[], &empty_params()).unwrap();
        assert!(reg.table_level_subs("users").unwrap().contains(&sub_id));
        assert!(reg.table_level_subs("orders").is_none());
    }

    #[test]
    fn test_subscribe_single_eq_composite_lookup() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &[], &empty_params()).unwrap();
        let key = CompositeKey {
            table: "users".into(),
            cols: vec![(0, CellValue::I64(42))],
        };
        assert_eq!(reg.composite_lookup(&key), Some(&[sub_id][..]));
    }

    #[test]
    fn test_subscribe_multi_eq_composite_lookup() {
        let mut reg = SubscriptionRegistry::new();
        let cond = cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("Alice".into()))]);
        let sub_id = reg.subscribe(&[cond], &[], &empty_params()).unwrap();

        // Single-key lookup should NOT find it
        let single = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(1))] };
        assert_eq!(reg.composite_lookup(&single), None);

        // Composite-key lookup should find it
        let composite = CompositeKey {
            table: "users".into(),
            cols: vec![(0, CellValue::I64(1)), (1, CellValue::Str("Alice".into()))],
        };
        assert_eq!(reg.composite_lookup(&composite), Some(&[sub_id][..]));
    }

    #[test]
    fn test_column_sets_tracked() {
        let mut reg = SubscriptionRegistry::new();
        reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        reg.subscribe(
            &[cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("Alice".into()))])],
            &[], &empty_params(),
        ).unwrap();

        let sets = reg.column_sets_for_table("users").unwrap();
        assert_eq!(sets.len(), 2);
        assert!(sets.contains(&ColumnSet { table: "users".into(), cols: vec![0] }));
        assert!(sets.contains(&ColumnSet { table: "users".into(), cols: vec![0, 1] }));
    }

    #[test]
    fn test_unsubscribe_cleans_reverse_index() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_eq("users", 0, Value::Int(42))], &[], &empty_params()).unwrap();
        reg.unsubscribe(sub_id);
        assert_eq!(reg.subscription_count(), 0);
        assert_eq!(reg.reverse_index_size(), 0);
        assert!(reg.column_sets_for_table("users").is_none());
    }

    #[test]
    fn test_unsubscribe_cleans_table_subs() {
        let mut reg = SubscriptionRegistry::new();
        let sub_id = reg.subscribe(&[cond_table("users")], &[], &empty_params()).unwrap();
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
                lookup_key_sets: vec![vec![ReactiveLookupKey {
                    col: 0,
                    value: Value::Placeholder("uid".into()),
                }]],
            },
            verify_filter: PlanFilterPredicate::Equals {
                col: ColumnRef { source: 0, col: 0 },
                value: Value::Placeholder("uid".into()),
            },
        };
        let params = HashMap::from([("uid".into(), ParamValue::Int(7))]);
        let sub_id = reg.subscribe(&[cond], &[], &params).unwrap();
        let key = CompositeKey {
            table: "users".into(),
            cols: vec![(0, CellValue::I64(7))],
        };
        assert_eq!(reg.composite_lookup(&key), Some(&[sub_id][..]));
    }
}
