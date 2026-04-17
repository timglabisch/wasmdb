use std::collections::HashMap;

use fnv::{FnvHashMap, FnvHashSet};

use database::{Database, DbError, MutResult};
use sql_engine::execute::{Columns, Params, Span};
use sql_engine::reactive::execute::on_zset;
use sql_engine::reactive::registry::SubscriptionRegistry;
use sql_engine::reactive::{SubscriptionHandle, SubscriptionId, SubscriptionKey};
use sql_engine::schema::TableSchema;
use sql_engine::storage::ZSet;
use sql_parser::ast::Statement;

use crate::error::SubscribeError;
use crate::subscription::{Callback, Subscription};

/// Database wrapped with a subscription registry and callback dispatch.
///
/// Mutating methods (`execute_mut`, `apply_zset`) automatically notify any
/// subscriptions affected by the emitted ZSet. Read methods (`execute`) do not
/// notify. Use `db_mut_raw()` when you need to mutate without firing callbacks
/// (e.g. for sync-client replay where `notify` is called explicitly once per
/// batch).
///
/// # Identity layers
///
/// Callers see [`SubscriptionHandle`]s and [`SubscriptionId`]s:
///
/// - `subscribe` dedups by [`SubscriptionKey`] (currently the SQL text). Two
///   callers that subscribe with equivalent SQL share one `SubscriptionId` —
///   the reactive engine plans and indexes the query exactly once.
/// - Each caller gets its own `SubscriptionHandle` for safe unsubscribe. The
///   last handle to be released tears down the deduped subscription.
///
/// See [`sql_engine::reactive::identity`] for the full model.
pub struct ReactiveDatabase {
    db: Database,
    registry: SubscriptionRegistry,
    /// Deduped subscriptions keyed by runtime id.
    subscriptions: FnvHashMap<SubscriptionId, Subscription>,
    /// Content-dedup: SQL → runtime id. A new subscribe with matching SQL
    /// reuses the existing id and only attaches an additional callback.
    by_key: FnvHashMap<SubscriptionKey, SubscriptionId>,
    /// Per-caller handle → its deduped subscription. Handle is the only
    /// identity callers need for `unsubscribe`.
    handles: FnvHashMap<SubscriptionHandle, SubscriptionId>,
    next_handle: u64,
}

impl ReactiveDatabase {
    pub fn new() -> Self {
        Self::from_database(Database::new())
    }

    pub fn from_database(db: Database) -> Self {
        Self {
            db,
            registry: SubscriptionRegistry::new(),
            subscriptions: FnvHashMap::default(),
            by_key: FnvHashMap::default(),
            handles: FnvHashMap::default(),
            next_handle: 0,
        }
    }

    // ── Read-only / delegating accessors ─────────────────────────────

    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Mutable access to the underlying `Database` without triggering notify.
    /// Use only when you plan to call `notify(&zset)` yourself (batched replay).
    pub fn db_mut_raw(&mut self) -> &mut Database {
        &mut self.db
    }

    // ── DDL delegation ───────────────────────────────────────────────

    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), DbError> {
        self.db.create_table(schema)
    }

    pub fn execute_ddl(&mut self, ddl: &str) -> Result<(), DbError> {
        self.db.execute_ddl(ddl)
    }

    pub fn execute_all(&mut self, sql: &str) -> Result<(), DbError> {
        self.db.execute_all(sql)
    }

    // ── Query delegation (no notify) ─────────────────────────────────

    pub fn execute(&mut self, sql: &str) -> Result<Columns, DbError> {
        self.db.execute(sql)
    }

    pub fn execute_with_params(&mut self, sql: &str, params: Params) -> Result<Columns, DbError> {
        self.db.execute_with_params(sql, params)
    }

    pub fn execute_traced(&mut self, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        self.db.execute_traced(sql)
    }

    // ── Mutation with auto-notify ────────────────────────────────────

    pub fn execute_mut(&mut self, sql: &str) -> Result<MutResult, DbError> {
        let result = self.db.execute_mut(sql)?;
        if let MutResult::Mutation(zset) = &result {
            self.notify(zset);
        }
        Ok(result)
    }

    pub fn execute_mut_with_params(&mut self, sql: &str, params: Params) -> Result<MutResult, DbError> {
        let result = self.db.execute_mut_with_params(sql, params)?;
        if let MutResult::Mutation(zset) = &result {
            self.notify(zset);
        }
        Ok(result)
    }

    pub fn apply_zset(&mut self, zset: &ZSet) -> Result<(), DbError> {
        self.db.apply_zset(zset)?;
        self.notify(zset);
        Ok(())
    }

    // ── Subscribe / unsubscribe ──────────────────────────────────────

    /// Register a reactive subscription. `sql` must be a SELECT statement.
    ///
    /// If another caller has already subscribed to equivalent SQL, this call
    /// reuses that subscription — the reactive engine plans and indexes the
    /// query exactly once across all callers. The returned
    /// [`SubscriptionHandle`] is per-caller and is the only identity needed
    /// for [`Self::unsubscribe`]. The returned [`SubscriptionId`] is the
    /// shared runtime id used by `notify` callbacks and the debug APIs; many
    /// handles may map to the same id.
    pub fn subscribe(
        &mut self,
        sql: &str,
        callback: Callback,
    ) -> Result<(SubscriptionHandle, SubscriptionId), SubscribeError> {
        let key = SubscriptionKey::from_sql(sql);

        let sub_id = if let Some(&id) = self.by_key.get(&key) {
            id
        } else {
            let stmt = sql_parser::parser::parse_statement(sql)
                .map_err(|e| SubscribeError::Parse(format!("{e:?}")))?;
            let select = match stmt {
                Statement::Select(s) => s,
                _ => return Err(SubscribeError::NotSelect),
            };

            let table_schemas = self.db.table_schemas();
            let plan = sql_engine::planner::reactive::plan_reactive(&select, &table_schemas)?;
            let sub_id = self.registry.subscribe(&plan.conditions, &plan.sources, &HashMap::new())?;

            self.subscriptions.insert(sub_id, Subscription {
                sql: sql.to_string(),
                callbacks: FnvHashMap::default(),
                last_triggered: FnvHashSet::default(),
            });
            self.by_key.insert(key, sub_id);
            sub_id
        };

        let handle = SubscriptionHandle(self.next_handle);
        self.next_handle += 1;

        let sub = self.subscriptions.get_mut(&sub_id)
            .expect("sub must exist at this point");
        sub.callbacks.insert(handle, callback);
        self.handles.insert(handle, sub_id);

        Ok((handle, sub_id))
    }

    /// Release a caller's handle. If this was the last handle on the
    /// underlying deduped subscription, the subscription is torn down from
    /// the registry as well.
    ///
    /// Unknown or already-released handles are a no-op — they return `false`
    /// so callers can surface a warning if they wish.
    pub fn unsubscribe(&mut self, handle: SubscriptionHandle) -> bool {
        let Some(sub_id) = self.handles.remove(&handle) else {
            return false;
        };
        let Some(sub) = self.subscriptions.get_mut(&sub_id) else {
            return true;
        };
        sub.callbacks.remove(&handle);
        if sub.callbacks.is_empty() {
            let sub = self.subscriptions.remove(&sub_id).expect("just checked");
            self.by_key.remove(&SubscriptionKey::from_sql(&sub.sql));
            self.registry.unsubscribe(sub_id);
        }
        true
    }

    // ── Reactive query helpers ───────────────────────────────────────

    /// Query with this subscription's last-triggered conditions. Consumes
    /// (clears) the triggered set after reading — next call returns empty until
    /// the next mutation triggers again.
    pub fn execute_for_sub(&mut self, sub_id: SubscriptionId, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        let triggered = self.subscriptions.get_mut(&sub_id)
            .map(|s| std::mem::take(&mut s.last_triggered))
            .unwrap_or_default();
        let triggered_std: std::collections::HashSet<usize> = triggered.into_iter().collect();
        self.db.execute_traced_with_triggered(sql, Some(triggered_std))
    }

    /// Query looking up the subscription by matching SQL. Used when the caller
    /// (e.g. JS) only has the SQL string, not the SubscriptionId. Consumes
    /// triggered.
    pub fn execute_for_sql(&mut self, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        let triggered = self.take_triggered_for_sql(sql);
        self.db.execute_traced_with_triggered(sql, triggered)
    }

    /// Peek & clear the triggered condition set for the subscription matching
    /// `sql` (there is at most one after dedup). Returned as std HashSet for
    /// interop with `execute_traced_with_triggered`.
    pub fn take_triggered_for_sql(&mut self, sql: &str) -> Option<std::collections::HashSet<usize>> {
        let key = SubscriptionKey::from_sql(sql);
        let sub_id = *self.by_key.get(&key)?;
        let sub = self.subscriptions.get_mut(&sub_id)?;
        if sub.last_triggered.is_empty() {
            return None;
        }
        let triggered = std::mem::take(&mut sub.last_triggered);
        Some(triggered.into_iter().collect())
    }

    // ── Notify ───────────────────────────────────────────────────────

    /// Dispatch a ZSet to subscribers: runs `on_zset` to get affected SubIds +
    /// triggered conditions, updates each subscription's `last_triggered`, and
    /// fires every callback registered against each affected subscription.
    pub fn notify(&mut self, zset: &ZSet) {
        if self.subscriptions.is_empty() {
            return;
        }
        let affected = on_zset(&self.registry, zset);
        for (sub_id, triggered) in affected {
            let Some(sub) = self.subscriptions.get_mut(&sub_id) else { continue };
            sub.last_triggered = triggered;
            let indices: Vec<usize> = sub.last_triggered.iter().copied().collect();
            for cb in sub.callbacks.values() {
                (cb)(sub_id, &indices);
            }
        }
    }

    /// Fire every registered callback with an empty triggered set. Used after
    /// bulk state changes (e.g. SyncClient rebuild) where we can't compute a
    /// precise diff.
    pub fn notify_all(&self) {
        for (sub_id, sub) in &self.subscriptions {
            for cb in sub.callbacks.values() {
                (cb)(*sub_id, &[]);
            }
        }
    }

    /// Replace the inner table data with a clone of `other`. Keeps the registry
    /// and subscriptions intact. Used by sync-client to rebuild optimistic from
    /// confirmed without losing subscriptions.
    pub fn replace_data(&mut self, other: &Database) {
        self.db = other.clone();
    }

    // ── Introspection ────────────────────────────────────────────────

    /// Number of unique (deduped) subscriptions currently live. A SQL string
    /// shared by N callers counts once.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Number of outstanding caller handles across all subscriptions. Sums the
    /// callback count of every live subscription.
    pub fn handle_count(&self) -> usize {
        self.handles.len()
    }

    pub fn registry(&self) -> &SubscriptionRegistry {
        &self.registry
    }

    pub fn subscription_sql(&self, sub_id: SubscriptionId) -> Option<&str> {
        self.subscriptions.get(&sub_id).map(|s| s.sql.as_str())
    }

    pub fn subscription_ids(&self) -> impl Iterator<Item = SubscriptionId> + '_ {
        self.subscriptions.keys().copied()
    }
}

impl Default for ReactiveDatabase {
    fn default() -> Self { Self::new() }
}
