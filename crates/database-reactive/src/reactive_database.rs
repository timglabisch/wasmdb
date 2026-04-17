use std::collections::HashMap;

use fnv::{FnvHashMap, FnvHashSet};

use database::{Database, DbError, MutResult};
use sql_engine::execute::{Columns, Params, Span};
use sql_engine::reactive::execute::on_zset;
use sql_engine::reactive::registry::{SubId, SubscriptionRegistry};
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
pub struct ReactiveDatabase {
    db: Database,
    registry: SubscriptionRegistry,
    subscriptions: FnvHashMap<SubId, Subscription>,
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

    /// Register a reactive subscription. `sql` must be a SELECT statement; it
    /// is parsed, `plan_reactive` extracts the reactive conditions and registers
    /// them in the registry. `callback` fires each time a mutation affects this
    /// subscription (see `Callback` docs for args).
    pub fn subscribe(&mut self, sql: &str, callback: Callback) -> Result<SubId, SubscribeError> {
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
            callback,
            last_triggered: FnvHashSet::default(),
        });
        Ok(sub_id)
    }

    pub fn unsubscribe(&mut self, sub_id: SubId) {
        self.registry.unsubscribe(sub_id);
        self.subscriptions.remove(&sub_id);
    }

    // ── Reactive query helpers ───────────────────────────────────────

    /// Query with this subscription's last-triggered conditions. Consumes
    /// (clears) the triggered set after reading — next call returns empty until
    /// the next mutation triggers again.
    pub fn execute_for_sub(&mut self, sub_id: SubId, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        let triggered = self.subscriptions.get_mut(&sub_id)
            .map(|s| std::mem::take(&mut s.last_triggered))
            .unwrap_or_default();
        let triggered_std: std::collections::HashSet<usize> = triggered.into_iter().collect();
        self.db.execute_traced_with_triggered(sql, Some(triggered_std))
    }

    /// Query looking up the subscription by matching SQL. Used when the caller
    /// (e.g. JS) only has the SQL string, not the SubId. Consumes triggered.
    pub fn execute_for_sql(&mut self, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        let triggered = self.take_triggered_for_sql(sql);
        self.db.execute_traced_with_triggered(sql, triggered)
    }

    /// Peek & clear the triggered condition set for a subscription matching
    /// `sql`. Returned as std HashSet for interop with `execute_traced_with_triggered`.
    pub fn take_triggered_for_sql(&mut self, sql: &str) -> Option<std::collections::HashSet<usize>> {
        let sub_id = self.subscriptions.iter()
            .find(|(_, s)| s.sql == sql)
            .map(|(id, _)| *id)?;
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
    /// fires callbacks.
    pub fn notify(&mut self, zset: &ZSet) {
        if self.subscriptions.is_empty() {
            return;
        }
        let affected = on_zset(&self.registry, zset);
        for (sub_id, triggered) in affected {
            let Some(sub) = self.subscriptions.get_mut(&sub_id) else { continue };
            sub.last_triggered = triggered;
            let indices: Vec<usize> = sub.last_triggered.iter().copied().collect();
            (sub.callback)(sub_id, &indices);
        }
    }

    /// Fire every registered callback with an empty triggered set. Used after
    /// bulk state changes (e.g. SyncClient rebuild) where we can't compute a
    /// precise diff.
    pub fn notify_all(&self) {
        for (sub_id, sub) in &self.subscriptions {
            (sub.callback)(*sub_id, &[]);
        }
    }

    /// Replace the inner table data with a clone of `other`. Keeps the registry
    /// and subscriptions intact. Used by sync-client to rebuild optimistic from
    /// confirmed without losing subscriptions.
    pub fn replace_data(&mut self, other: &Database) {
        self.db = other.clone();
    }

    // ── Introspection ────────────────────────────────────────────────

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn registry(&self) -> &SubscriptionRegistry {
        &self.registry
    }

    pub fn subscription_sql(&self, sub_id: SubId) -> Option<&str> {
        self.subscriptions.get(&sub_id).map(|s| s.sql.as_str())
    }

    pub fn subscription_ids(&self) -> impl Iterator<Item = SubId> + '_ {
        self.subscriptions.keys().copied()
    }
}

impl Default for ReactiveDatabase {
    fn default() -> Self { Self::new() }
}
