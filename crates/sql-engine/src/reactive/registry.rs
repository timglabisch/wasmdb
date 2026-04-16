//! Reactive subscription registry — state management.
//!
//! Manages subscriptions and the reverse index. The registry is pure state:
//! subscribe/unsubscribe manage the data structures, the execution logic
//! (checking which subscriptions are affected by a mutation) lives in
//! `reactive::execute`.

use fnv::{FnvHashMap, FnvHashSet};

use crate::execute::bind::{resolve_filter, resolve_value};
use crate::execute::value_to_cell;
use crate::execute::{ExecuteError, Params};
use crate::planner::shared::plan::PlanSourceEntry;
use crate::planner::reactive::{OptimizedReactiveCondition, ReactiveLookupStrategy};
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

/// A registered subscription.
struct Subscription {
    conditions: Vec<OptimizedReactiveCondition>,
    sources: Vec<PlanSourceEntry>,
    /// For deregistration: which composite keys belong to this subscription.
    /// May contain duplicates (e.g. `IN (1,2,3)` registers one key per value),
    /// each entry corresponds to one refcount increment at subscribe time.
    composite_keys: Vec<CompositeKey>,
    /// For deregistration: which tables this subscription watches at table-level.
    /// Each table appears at most once, even if multiple TableScan conditions
    /// target it — we only increment `table_subs` once per (sub, table) pair.
    table_level_tables: Vec<String>,
}

/// Manages subscriptions and the reverse index.
pub struct SubscriptionRegistry {
    next_id: u64,
    subscriptions: FnvHashMap<SubId, Subscription>,
    /// Composite reverse index: composite key → subscriptions.
    /// Value is a HashSet so unsubscribe can remove a SubId in O(1).
    reverse_index: FnvHashMap<CompositeKey, FnvHashSet<SubId>>,
    /// Per-table, which sorted column index sets are registered and how often.
    /// The inner map is `sorted column indices → refcount`. Refcount tracks the
    /// number of composite-key registrations using this column-set; when it
    /// reaches 0 the entry is removed. This avoids scanning the reverse index
    /// on unsubscribe to check whether a column-set is still in use.
    column_sets: FnvHashMap<String, FnvHashMap<Vec<usize>, usize>>,
    /// Table-level subscriptions: any mutation on the table triggers the subscription.
    table_subs: FnvHashMap<String, FnvHashSet<SubId>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            subscriptions: FnvHashMap::default(),
            reverse_index: FnvHashMap::default(),
            column_sets: FnvHashMap::default(),
            table_subs: FnvHashMap::default(),
        }
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn table_subscriptions(&self) -> &FnvHashMap<String, FnvHashSet<SubId>> {
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
        let mut table_level_tables = Vec::new();
        for cond in &resolved {
            match &cond.strategy {
                ReactiveLookupStrategy::TableScan => {
                    let newly_inserted = self
                        .table_subs
                        .entry(cond.table.clone())
                        .or_default()
                        .insert(id);
                    if newly_inserted {
                        table_level_tables.push(cond.table.clone());
                    }
                }
                ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
                    for keys in lookup_key_sets {
                        let mut cols: Vec<(usize, CellValue)> = keys
                            .iter()
                            .map(|k| (k.col, value_to_cell(&k.value)))
                            .collect();
                        cols.sort_by_key(|(col, _)| *col);
                        let col_indices: Vec<usize> = cols.iter().map(|(c, _)| *c).collect();

                        let ck = CompositeKey {
                            table: cond.table.clone(),
                            cols,
                        };
                        self.reverse_index
                            .entry(ck.clone())
                            .or_default()
                            .insert(id);

                        *self.column_sets
                            .entry(cond.table.clone())
                            .or_default()
                            .entry(col_indices)
                            .or_insert(0) += 1;

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
                table_level_tables,
            },
        );

        Ok(id)
    }

    /// Remove a subscription and clean up reverse index entries.
    ///
    /// Complexity: O(composite_keys + table_level_tables) — proportional to the
    /// work this subscription registered at `subscribe` time. Each step is O(1):
    /// reverse-index removal uses `HashSet::remove`, column-set refcount is
    /// decremented via map access (no full index scan), and table-level cleanup
    /// iterates only the tables the sub actually watched.
    pub fn unsubscribe(&mut self, id: SubId) {
        let Some(sub) = self.subscriptions.remove(&id) else { return };

        for ck in &sub.composite_keys {
            if let Some(subs) = self.reverse_index.get_mut(ck) {
                subs.remove(&id);
                if subs.is_empty() {
                    self.reverse_index.remove(ck);
                }
            }

            if let Some(table_map) = self.column_sets.get_mut(&ck.table) {
                let col_indices: Vec<usize> = ck.cols.iter().map(|(c, _)| *c).collect();
                if let Some(ref_count) = table_map.get_mut(&col_indices) {
                    *ref_count -= 1;
                    if *ref_count == 0 {
                        table_map.remove(&col_indices);
                    }
                }
                if table_map.is_empty() {
                    self.column_sets.remove(&ck.table);
                }
            }
        }

        for table in &sub.table_level_tables {
            if let Some(subs) = self.table_subs.get_mut(table) {
                subs.remove(&id);
                if subs.is_empty() {
                    self.table_subs.remove(table);
                }
            }
        }
    }

    // ── Accessors for reactive::execute ────────────────────────────────

    pub(crate) fn table_level_subs(&self, table: &str) -> Option<&FnvHashSet<SubId>> {
        self.table_subs.get(table)
    }

    /// Look up subscriptions by composite key.
    pub(crate) fn composite_lookup(&self, key: &CompositeKey) -> Option<&FnvHashSet<SubId>> {
        self.reverse_index.get(key)
    }

    /// Iterate the sorted column-index sets registered for a table.
    /// Used at mutation time to know which composite keys to build from a row.
    pub(crate) fn column_sets_for_table(&self, table: &str) -> Option<impl Iterator<Item = &Vec<usize>>> {
        self.column_sets.get(table).map(|m| m.keys())
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
    use crate::planner::shared::plan::{ColumnRef, PlanFilterPredicate};
    use crate::planner::reactive::ReactiveLookupKey;
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

    fn lookup_single(reg: &SubscriptionRegistry, key: &CompositeKey) -> Option<Vec<SubId>> {
        reg.composite_lookup(key).map(|s| {
            let mut v: Vec<SubId> = s.iter().copied().collect();
            v.sort_by_key(|s| s.0);
            v
        })
    }

    /// Check every internal invariant of the registry. Panics with a pointed
    /// message on the first violation. Called after every state transition in
    /// the churn tests below to prove the data structures stay consistent.
    ///
    /// Invariants (numbered to match panic messages):
    /// 1. Every reverse_index entry has a non-empty HashSet.
    /// 2. Every SubId in the reverse_index refers to a live subscription whose
    ///    composite_keys list contains that CompositeKey.
    /// 3. Every column_sets table entry has a non-empty inner map; every
    ///    (cols → refcount) pair has refcount > 0.
    /// 4. The stored refcount equals the actual count of composite_keys across
    ///    all live subscriptions that match (table, cols).
    /// 5. Every table_subs entry is a non-empty HashSet of live subscriptions
    ///    that list the table in their table_level_tables.
    /// 6. Every composite_key listed on a subscription is reflected in the
    ///    reverse_index and has a matching column_sets entry.
    /// 7. Every table in a subscription's table_level_tables is reflected in
    ///    table_subs.
    fn check_invariants(reg: &SubscriptionRegistry) {
        // 1 & 2: reverse_index integrity
        for (ck, subs) in &reg.reverse_index {
            assert!(!subs.is_empty(), "[1] reverse_index has empty HashSet for {ck:?}");
            for sub_id in subs {
                let sub = reg.subscriptions.get(sub_id).unwrap_or_else(|| {
                    panic!("[2] reverse_index[{ck:?}] contains dangling {sub_id:?}")
                });
                assert!(
                    sub.composite_keys.contains(ck),
                    "[2] sub {sub_id:?}.composite_keys does not list {ck:?}"
                );
            }
        }

        // 3 & 4: column_sets integrity + refcount accuracy
        for (table, inner) in &reg.column_sets {
            assert!(!inner.is_empty(), "[3] column_sets: empty inner map for {table}");
            for (cols, &rc) in inner {
                assert!(rc > 0, "[3] column_sets[{table}][{cols:?}]: refcount is 0");
                let actual: usize = reg
                    .subscriptions
                    .values()
                    .flat_map(|s| &s.composite_keys)
                    .filter(|ck| {
                        ck.table == *table
                            && ck.cols.iter().map(|(c, _)| *c).collect::<Vec<_>>() == *cols
                    })
                    .count();
                assert_eq!(
                    rc, actual,
                    "[4] refcount mismatch column_sets[{table}][{cols:?}]: stored={rc}, actual={actual}"
                );
            }
        }

        // 5: table_subs integrity
        for (table, subs) in &reg.table_subs {
            assert!(!subs.is_empty(), "[5] table_subs: empty HashSet for {table}");
            for sub_id in subs {
                let sub = reg.subscriptions.get(sub_id).unwrap_or_else(|| {
                    panic!("[5] table_subs[{table}] contains dangling {sub_id:?}")
                });
                assert!(
                    sub.table_level_tables.contains(table),
                    "[5] sub {sub_id:?}.table_level_tables does not list {table}"
                );
            }
        }

        // 6 & 7: subscription → index back-references
        for (sub_id, sub) in &reg.subscriptions {
            for ck in &sub.composite_keys {
                let subs = reg.reverse_index.get(ck).unwrap_or_else(|| {
                    panic!("[6] sub {sub_id:?} lists ck {ck:?} missing from reverse_index")
                });
                assert!(
                    subs.contains(sub_id),
                    "[6] reverse_index[{ck:?}] does not contain {sub_id:?}"
                );
                let cols: Vec<usize> = ck.cols.iter().map(|(c, _)| *c).collect();
                let inner = reg.column_sets.get(&ck.table).unwrap_or_else(|| {
                    panic!("[6] column_sets missing table {} (for sub {sub_id:?})", ck.table)
                });
                assert!(
                    inner.contains_key(&cols),
                    "[6] column_sets[{}] missing shape {cols:?} (for sub {sub_id:?})",
                    ck.table
                );
            }
            for table in &sub.table_level_tables {
                let subs = reg.table_subs.get(table).unwrap_or_else(|| {
                    panic!("[7] sub {sub_id:?} lists table {table} missing from table_subs")
                });
                assert!(
                    subs.contains(sub_id),
                    "[7] table_subs[{table}] does not contain {sub_id:?}"
                );
            }
        }
    }

    fn registry_is_empty(reg: &SubscriptionRegistry) -> bool {
        reg.subscriptions.is_empty()
            && reg.reverse_index.is_empty()
            && reg.column_sets.is_empty()
            && reg.table_subs.is_empty()
    }

    fn assert_empty(reg: &SubscriptionRegistry) {
        assert!(
            registry_is_empty(reg),
            "registry not fully drained: subs={}, reverse_index={}, column_sets={}, table_subs={}",
            reg.subscriptions.len(),
            reg.reverse_index.len(),
            reg.column_sets.len(),
            reg.table_subs.len()
        );
    }

    fn cond_in_list(table: &str, col: usize, values: &[i64]) -> OptimizedReactiveCondition {
        let lookup_key_sets: Vec<Vec<ReactiveLookupKey>> = values
            .iter()
            .map(|&v| vec![ReactiveLookupKey { col, value: Value::Int(v) }])
            .collect();
        OptimizedReactiveCondition {
            table: table.into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::IndexLookup { lookup_key_sets },
            verify_filter: PlanFilterPredicate::None,
        }
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
        assert_eq!(lookup_single(&reg, &key), Some(vec![sub_id]));
    }

    #[test]
    fn test_subscribe_multi_eq_composite_lookup() {
        let mut reg = SubscriptionRegistry::new();
        let cond = cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("Alice".into()))]);
        let sub_id = reg.subscribe(&[cond], &[], &empty_params()).unwrap();

        // Single-key lookup should NOT find it
        let single = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(1))] };
        assert_eq!(lookup_single(&reg, &single), None);

        // Composite-key lookup should find it
        let composite = CompositeKey {
            table: "users".into(),
            cols: vec![(0, CellValue::I64(1)), (1, CellValue::Str("Alice".into()))],
        };
        assert_eq!(lookup_single(&reg, &composite), Some(vec![sub_id]));
    }

    #[test]
    fn test_column_sets_tracked() {
        let mut reg = SubscriptionRegistry::new();
        reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        reg.subscribe(
            &[cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("Alice".into()))])],
            &[], &empty_params(),
        ).unwrap();

        let sets: Vec<Vec<usize>> = reg
            .column_sets_for_table("users")
            .unwrap()
            .cloned()
            .collect();
        assert_eq!(sets.len(), 2);
        assert!(sets.contains(&vec![0]));
        assert!(sets.contains(&vec![0, 1]));
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
        assert_eq!(lookup_single(&reg, &key), Some(vec![sub_id]));
    }

    #[test]
    fn test_unsubscribe_shared_column_set_keeps_entry() {
        let mut reg = SubscriptionRegistry::new();
        let a = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        let _b = reg.subscribe(&[cond_eq("users", 0, Value::Int(2))], &[], &empty_params()).unwrap();

        // Both use column-set [0], different values.
        assert_eq!(reg.column_sets_for_table("users").unwrap().count(), 1);

        reg.unsubscribe(a);
        // Column-set [0] still in use by b.
        assert_eq!(reg.column_sets_for_table("users").unwrap().count(), 1);
        let key = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(1))] };
        assert!(reg.composite_lookup(&key).is_none());
        let key_b = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(2))] };
        assert!(reg.composite_lookup(&key_b).is_some());
    }

    #[test]
    fn test_unsubscribe_same_key_two_subs() {
        // Two subs registering the exact same composite key.
        let mut reg = SubscriptionRegistry::new();
        let a = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        let b = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        let key = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(1))] };
        assert_eq!(lookup_single(&reg, &key), Some({
            let mut v = vec![a, b]; v.sort_by_key(|s| s.0); v
        }));

        reg.unsubscribe(a);
        assert_eq!(lookup_single(&reg, &key), Some(vec![b]));
        reg.unsubscribe(b);
        assert!(reg.composite_lookup(&key).is_none());
        assert!(reg.column_sets_for_table("users").is_none());
    }

    #[test]
    fn test_unsubscribe_in_list_decrements_refcount() {
        // `REACTIVE(id IN (1,2,3))` registers 3 composite keys, all with column-set [0].
        let mut reg = SubscriptionRegistry::new();
        let cond = OptimizedReactiveCondition {
            table: "users".into(),
            source_idx: 0,
            strategy: ReactiveLookupStrategy::IndexLookup {
                lookup_key_sets: vec![
                    vec![ReactiveLookupKey { col: 0, value: Value::Int(1) }],
                    vec![ReactiveLookupKey { col: 0, value: Value::Int(2) }],
                    vec![ReactiveLookupKey { col: 0, value: Value::Int(3) }],
                ],
            },
            verify_filter: PlanFilterPredicate::None,
        };
        let a = reg.subscribe(&[cond], &[], &empty_params()).unwrap();
        assert_eq!(reg.reverse_index_size(), 3);
        assert_eq!(reg.column_sets_for_table("users").unwrap().count(), 1);

        reg.unsubscribe(a);
        assert_eq!(reg.reverse_index_size(), 0);
        assert!(reg.column_sets_for_table("users").is_none());
    }

    #[test]
    fn test_unsubscribe_mixed_table_level_and_index() {
        let mut reg = SubscriptionRegistry::new();
        let a = reg.subscribe(
            &[cond_table("users"), cond_eq("orders", 0, Value::Int(42))],
            &[],
            &empty_params(),
        ).unwrap();
        let b = reg.subscribe(&[cond_table("users")], &[], &empty_params()).unwrap();

        assert!(reg.table_level_subs("users").unwrap().contains(&a));
        assert!(reg.table_level_subs("users").unwrap().contains(&b));

        reg.unsubscribe(a);
        // users table-level: only b remains
        assert_eq!(reg.table_level_subs("users").unwrap().len(), 1);
        assert!(reg.table_level_subs("users").unwrap().contains(&b));
        // orders composite-index gone
        assert!(reg.column_sets_for_table("orders").is_none());
    }

    #[test]
    fn test_unsubscribe_duplicate_table_scan_same_table() {
        // One sub with two TableScan conditions on the same table should only
        // maintain a single (sub, table) entry and leave it clean on unsubscribe.
        let mut reg = SubscriptionRegistry::new();
        let a = reg.subscribe(&[cond_table("users"), cond_table("users")], &[], &empty_params()).unwrap();
        assert_eq!(reg.table_level_subs("users").unwrap().len(), 1);
        reg.unsubscribe(a);
        assert!(reg.table_level_subs("users").is_none());
    }

    // ── Leak-freedom and invariant-preservation tests ────────────────────

    #[test]
    fn invariants_hold_on_empty_registry() {
        let reg = SubscriptionRegistry::new();
        check_invariants(&reg);
        assert_empty(&reg);
    }

    #[test]
    fn no_leaks_after_full_unsubscribe_comprehensive() {
        let mut reg = SubscriptionRegistry::new();
        let mut ids = Vec::new();

        // Same shape [0], two different values — refcount [0] must reach 2.
        ids.push(reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap());
        check_invariants(&reg);
        ids.push(reg.subscribe(&[cond_eq("users", 0, Value::Int(2))], &[], &empty_params()).unwrap());
        check_invariants(&reg);

        // New shape [0,1] on same table.
        ids.push(reg.subscribe(
            &[cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("Alice".into()))])],
            &[], &empty_params()
        ).unwrap());
        check_invariants(&reg);

        // Different table.
        ids.push(reg.subscribe(&[cond_eq("orders", 0, Value::Int(42))], &[], &empty_params()).unwrap());
        check_invariants(&reg);

        // Table-level on users (adds to table_subs).
        ids.push(reg.subscribe(&[cond_table("users")], &[], &empty_params()).unwrap());
        check_invariants(&reg);

        // IN-list: multiple composite keys under the same shape [0].
        ids.push(reg.subscribe(
            &[cond_in_list("products", 0, &[10, 20, 30])],
            &[], &empty_params()
        ).unwrap());
        check_invariants(&reg);

        // Multi-condition sub: table-level + index + cross-table.
        ids.push(reg.subscribe(
            &[
                cond_table("users"),
                cond_table("orders"),
                cond_eq("products", 1, Value::Text("widget".into())),
            ],
            &[], &empty_params()
        ).unwrap());
        check_invariants(&reg);

        // Drain in LIFO order, check invariant after each step.
        while let Some(id) = ids.pop() {
            reg.unsubscribe(id);
            check_invariants(&reg);
        }

        assert_empty(&reg);
    }

    #[test]
    fn no_leaks_unsubscribe_scrambled_order() {
        let mut reg = SubscriptionRegistry::new();
        let mut ids = Vec::new();

        for i in 0..20 {
            let cond = match i % 5 {
                0 => cond_eq("users", 0, Value::Int(i)),
                1 => cond_eq("users", 1, Value::Text(format!("n{i}"))),
                2 => cond_multi_eq("users", &[(0, Value::Int(i)), (1, Value::Text(format!("n{i}")))]),
                3 => cond_table("orders"),
                _ => cond_eq("products", 2, Value::Int(i)),
            };
            ids.push(reg.subscribe(&[cond], &[], &empty_params()).unwrap());
            check_invariants(&reg);
        }

        // Scrambled removal order (no pattern, just non-sequential).
        let order = [5, 0, 17, 10, 3, 8, 14, 2, 19, 1, 11, 6, 15, 9, 18, 4, 12, 7, 16, 13];
        for &pos in &order {
            reg.unsubscribe(ids[pos]);
            check_invariants(&reg);
        }

        assert_empty(&reg);
    }

    #[test]
    fn no_leaks_after_repeated_churn_cycles() {
        // Subscribe the same shapes, unsubscribe, repeat. Verifies no slow
        // leakage across cycles (ever-growing next_id does NOT count as leak).
        let mut reg = SubscriptionRegistry::new();

        for _ in 0..10 {
            let a = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
            let b = reg.subscribe(
                &[cond_multi_eq("users", &[(0, Value::Int(1)), (1, Value::Text("x".into()))])],
                &[], &empty_params()
            ).unwrap();
            let c = reg.subscribe(&[cond_table("users")], &[], &empty_params()).unwrap();
            let d = reg.subscribe(&[cond_in_list("orders", 0, &[1, 2, 3, 4, 5])], &[], &empty_params()).unwrap();
            check_invariants(&reg);

            reg.unsubscribe(a);
            reg.unsubscribe(b);
            reg.unsubscribe(c);
            reg.unsubscribe(d);
            check_invariants(&reg);
            assert_empty(&reg);
        }
    }

    #[test]
    fn no_leaks_under_pseudo_random_churn() {
        // Deterministic LCG-driven subscribe/unsubscribe mix. Checks the
        // invariant after every single operation and requires an empty
        // registry after draining all survivors.
        let mut state: u64 = 0xdeadbeef_cafef00d;
        let mut rand = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };

        let mut reg = SubscriptionRegistry::new();
        let mut alive: Vec<SubId> = Vec::new();

        for _ in 0..500 {
            let prob = rand() % 100;
            if prob < 65 || alive.is_empty() {
                // Subscribe with one of 6 condition shapes, randomised value.
                let shape = rand() % 6;
                let val = (rand() % 8) as i64;
                let cond = match shape {
                    0 => cond_eq("users", 0, Value::Int(val)),
                    1 => cond_eq("users", 1, Value::Text(format!("u{val}"))),
                    2 => cond_multi_eq(
                        "users",
                        &[(0, Value::Int(val)), (1, Value::Text(format!("u{val}")))],
                    ),
                    3 => cond_table("users"),
                    4 => cond_table("orders"),
                    _ => cond_eq("products", 2, Value::Int(val)),
                };
                let id = reg.subscribe(&[cond], &[], &empty_params()).unwrap();
                alive.push(id);
            } else {
                // Remove a random alive sub via swap_remove (O(1)).
                let idx = (rand() as usize) % alive.len();
                let id = alive.swap_remove(idx);
                reg.unsubscribe(id);
            }
            check_invariants(&reg);
        }

        // Drain whatever's left.
        while let Some(id) = alive.pop() {
            reg.unsubscribe(id);
            check_invariants(&reg);
        }
        assert_empty(&reg);
    }

    #[test]
    fn no_leaks_duplicate_and_overlapping_subs() {
        // Two subs with the identical composite key, one sub with overlapping
        // TableScan + IndexLookup on the same table, and one IN-list sub whose
        // shape overlaps with the simple eq subs. Verifies refcount handling
        // under heavy overlap.
        let mut reg = SubscriptionRegistry::new();

        let a = reg.subscribe(&[cond_eq("users", 0, Value::Int(7))], &[], &empty_params()).unwrap();
        let b = reg.subscribe(&[cond_eq("users", 0, Value::Int(7))], &[], &empty_params()).unwrap();
        let c = reg.subscribe(&[cond_eq("users", 0, Value::Int(8))], &[], &empty_params()).unwrap();
        let d = reg.subscribe(&[cond_in_list("users", 0, &[7, 8, 9])], &[], &empty_params()).unwrap();
        let e = reg.subscribe(
            &[cond_table("users"), cond_eq("users", 0, Value::Int(7))],
            &[], &empty_params()
        ).unwrap();
        check_invariants(&reg);

        // Composite key (users, [(0, 7)]) is registered by a, b, d (IN-list), e.
        let key_7 = CompositeKey { table: "users".into(), cols: vec![(0, CellValue::I64(7))] };
        let subs_at_7: FnvHashSet<SubId> = reg.composite_lookup(&key_7).unwrap().clone();
        let expected: FnvHashSet<SubId> = [a, b, d, e].into_iter().collect();
        assert_eq!(subs_at_7, expected);
        // Shape [0] has refcount 6: a, b, c, e contribute 1 each, d contributes 3.
        let cols: Vec<Vec<usize>> = reg.column_sets_for_table("users").unwrap().cloned().collect();
        assert_eq!(cols, vec![vec![0]]);

        // Unsubscribe a, b, c, d, e in a non-trivial order and verify at every step.
        reg.unsubscribe(c);
        check_invariants(&reg);
        reg.unsubscribe(a);
        check_invariants(&reg);
        reg.unsubscribe(d);
        check_invariants(&reg);
        reg.unsubscribe(e);
        check_invariants(&reg);
        reg.unsubscribe(b);
        check_invariants(&reg);

        assert_empty(&reg);
    }

    #[test]
    fn no_leaks_unsubscribe_unknown_sub_is_noop() {
        let mut reg = SubscriptionRegistry::new();
        let a = reg.subscribe(&[cond_eq("users", 0, Value::Int(1))], &[], &empty_params()).unwrap();
        check_invariants(&reg);

        // Unsubscribe a non-existent SubId — must not touch anything.
        reg.unsubscribe(SubId(9999));
        check_invariants(&reg);

        // Unsubscribing the same sub twice — second call is a no-op.
        reg.unsubscribe(a);
        check_invariants(&reg);
        reg.unsubscribe(a);
        check_invariants(&reg);

        assert_empty(&reg);
    }
}
