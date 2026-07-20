use std::collections::{HashMap, VecDeque};

use dirty_set::{DirtySet, DirtySlotId};
use fnv::{FnvHashMap, FnvHashSet};

use database::{Database, DbError, MutResult};
use database_projection::db_host::DatabaseHost;
use database_projection::{DeriveFailure, DeriveOutcome, ProjectionEngine};
use sql_engine::execute::{Columns, Params, Span};
use sql_engine::reactive::execute::on_zset;
use sql_engine::reactive::registry::SubscriptionRegistry;
use sql_engine::reactive::{SubscriptionHandle, SubscriptionId, SubscriptionKey};
use sql_engine::schema::TableSchema;
use sql_engine::storage::{CellValue, ZSet};
use sql_parser::ast::Statement;

use crate::dirty_notification::DirtyNotification;
use crate::error::SubscribeError;
use crate::subscription::Subscription;

/// Capacity of the inline fast-path list in the internal `DirtySet`. Marks
/// beyond this spill to the heap bitmap. Sized so typical ticks stay on the
/// fast path without being wasteful at rest.
const DIRTY_LIST_CAP: usize = 128;

/// Database wrapped with a subscription registry and a dirty-set based
/// pull API.
///
/// Mutating methods (`execute_mut`, `apply_zset`) call `notify(&zset)` which
/// marks affected subscriptions dirty and fires a single edge-triggered wake
/// signal. Consumers drain via [`Self::next_dirty`] when they're ready —
/// perfect for JS hosts that want to process notifications across
/// `requestIdleCallback` frames rather than blocking the UI thread.
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
    /// reuses the existing id and only bumps its refcount.
    by_key: FnvHashMap<SubscriptionKey, SubscriptionId>,
    /// Per-caller handle → its deduped subscription. Handle is the only
    /// identity callers need for `unsubscribe`.
    handles: FnvHashMap<SubscriptionHandle, SubscriptionId>,
    next_handle: u64,

    /// Dirty-set of u32 slots corresponding to live subscriptions. Writer
    /// side of the pull API; `next_dirty` drains.
    dirty: DirtySet<DIRTY_LIST_CAP>,
    /// SubscriptionId → slot in the dirty-set.
    sub_to_slot: FnvHashMap<SubscriptionId, u32>,
    /// Slot → SubscriptionId. `None` means the slot is free and awaiting reuse.
    slot_to_sub: Vec<Option<SubscriptionId>>,
    /// Free-list of released slots — reused on the next `alloc_slot` so the
    /// u32 namespace doesn't grow unbounded over subscribe/unsubscribe churn.
    free_slots: Vec<u32>,
    /// Snapshot of the current drain cycle. Populated lazily by `next_dirty`
    /// when empty and the dirty-set has marks, then popped one-by-one until
    /// exhausted.
    drain_buffer: VecDeque<DirtyNotification>,
    /// Edge-triggered wake signal. Fires once when `dirty` transitions from
    /// empty → non-empty; stays silent while marks pile up before the next
    /// drain.
    wake: Option<Box<dyn Fn()>>,

    /// Projection engine (materialized views as Rust functions). When
    /// installed, every `notify` runs a derive pass FIRST: affected
    /// partitions are recomputed, derived deltas applied to the database, and the
    /// combined change set is what subscribers are notified with —
    /// same-batch atomicity, no torn reads between source and derived
    /// tables. See `database-projection` / docs/wasmdb-projections-design.md.
    projections: Option<ProjectionEngine>,
    /// Failure/recovery bookkeeping events in derive-pass order. ONE
    /// ordered stream (not separate failure/recovery lists) because
    /// multiple derive passes can run between two drains — the last event
    /// per partition must win at the consumer. Drained via
    /// `take_projection_events`.
    projection_events: Vec<ProjectionEvent>,
    /// `(projection id, display partition)` → message of the reported,
    /// not-yet-recovered failure. Bounds the event stream: an unchanged
    /// repeat failure records no new event, and successes are only
    /// recorded for partitions present here.
    failed_projection_partitions: FnvHashMap<(String, String), String>,
}

/// One failure/recovery bookkeeping event from a derive pass. Consumers
/// (e.g. the wasm requirements drain) must apply events in order: a
/// `Failed` followed by a `Recovered` for the same partition means the
/// partition is healthy now, and vice versa.
#[derive(Debug, Clone)]
pub enum ProjectionEvent {
    /// A partition's recompute errored; its previous output stays in
    /// place. Repeated failures with an unchanged message are recorded once.
    Failed(DeriveFailure),
    /// A previously-failed partition re-derived successfully. Never
    /// emitted for partitions without a prior `Failed` event.
    Recovered { projection: String, partition: String },
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
            dirty: DirtySet::new(0),
            sub_to_slot: FnvHashMap::default(),
            slot_to_sub: Vec::new(),
            free_slots: Vec::new(),
            drain_buffer: VecDeque::new(),
            wake: None,
            projections: None,
            projection_events: Vec::new(),
            failed_projection_partitions: FnvHashMap::default(),
        }
    }

    // ── Projections ──────────────────────────────────────────────────

    /// Install the projection engine. Runs an initial full derive so
    /// pre-existing source rows are materialized; call `notify_all` after
    /// if subscribers should see the initial state.
    pub fn install_projections(&mut self, mut engine: ProjectionEngine) {
        if !engine.is_empty() {
            let outcome = {
                let mut host = DatabaseHost::new(&mut self.db);
                engine.reset_and_rederive(&mut host)
            };
            self.absorb_outcome_bookkeeping(outcome);
        }
        self.projections = Some(engine);
    }

    /// Drain the failure/recovery events accumulated by derive passes, in
    /// derive order. Embedders route them to their error surfaces (e.g.
    /// pin/unpin `SlotKind::Projected` slots) by applying them in order.
    pub fn take_projection_events(&mut self) -> Vec<ProjectionEvent> {
        std::mem::take(&mut self.projection_events)
    }

    /// Activate a dynamic projection instance (§12): materialize
    /// `(id, name)` from the current local data and keep it in sync until
    /// the matching `deactivate_projection`. Repeated activation refcounts.
    /// The instance's delta goes straight to the subscribers — it touches
    /// only projection-owned tables and must not re-enter the derive pass.
    pub fn activate_projection(&mut self, id: &str, name: Vec<CellValue>) -> Result<(), DbError> {
        let Some(engine) = self.projections.as_mut() else {
            return Err(DbError::Projection("no projection engine installed".into()));
        };
        let outcome = {
            let mut host = DatabaseHost::new(&mut self.db);
            engine.activate(id, &name, &mut host).map_err(DbError::Projection)?
        };
        let delta = self.absorb_outcome_bookkeeping(outcome);
        if !delta.is_empty() {
            self.dispatch_to_subscribers(&delta);
        }
        Ok(())
    }

    /// Release one activation of `(id, name)`. The last release retracts
    /// the instance's output rows and notifies subscribers.
    pub fn deactivate_projection(&mut self, id: &str, name: &[CellValue]) -> Result<(), DbError> {
        let Some(engine) = self.projections.as_mut() else {
            return Err(DbError::Projection("no projection engine installed".into()));
        };
        let outcome = {
            let mut host = DatabaseHost::new(&mut self.db);
            engine.deactivate(id, name, &mut host).map_err(DbError::Projection)?
        };
        let delta = self.absorb_outcome_bookkeeping(outcome);
        if !delta.is_empty() {
            self.dispatch_to_subscribers(&delta);
        }
        Ok(())
    }

    /// Fold a derive outcome into failure/recovery bookkeeping. Returns the
    /// outcome's delta for the caller.
    fn absorb_outcome_bookkeeping(&mut self, outcome: DeriveOutcome) -> ZSet {
        for pair in outcome.succeeded {
            if self.failed_projection_partitions.remove(&pair).is_some() {
                let (projection, partition) = pair;
                self.projection_events
                    .push(ProjectionEvent::Recovered { projection, partition });
            }
        }
        for f in outcome.failures {
            match &f.partition {
                Some(partition) => {
                    let pair = (f.projection.clone(), partition.clone());
                    let unchanged = self
                        .failed_projection_partitions
                        .get(&pair)
                        .is_some_and(|prev| *prev == f.message);
                    if unchanged {
                        continue;
                    }
                    self.failed_projection_partitions.insert(pair, f.message.clone());
                    self.projection_events.push(ProjectionEvent::Failed(f));
                }
                // Not tied to a partition (e.g. reset teardown) — always surface.
                None => self.projection_events.push(ProjectionEvent::Failed(f)),
            }
        }
        outcome.delta
    }

    /// Derive pass for one external change set (already applied to the
    /// database). Returns the combined zset (external + derived deltas) if
    /// anything was derived.
    fn derive_pass(&mut self, zset: &ZSet) -> Option<ZSet> {
        let engine = self.projections.as_mut()?;
        if engine.is_empty() || zset.is_empty() {
            return None;
        }
        // Batches from `apply_zset` are pre-guarded; a violation here means
        // a raw write path (SQL / db_mut_raw) touched an owned table —
        // a programming error, surfaced in debug builds.
        debug_assert!(
            engine.guard_external(zset).is_ok(),
            "write to projection-owned table reached notify: {}",
            engine.guard_external(zset).unwrap_err(),
        );
        let outcome = {
            let mut host = DatabaseHost::new(&mut self.db);
            engine.derive(zset, &mut host)
        };
        let delta = self.absorb_outcome_bookkeeping(outcome);
        if delta.is_empty() {
            None
        } else {
            let mut combined = zset.clone();
            combined.extend(delta);
            Some(combined)
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

    pub fn execute_traced_with_params(
        &mut self,
        sql: &str,
        params: Params,
    ) -> Result<(Columns, Vec<Span>), DbError> {
        self.db.execute_traced_with_params(sql, params)
    }

    pub fn execute_traced_with_triggered_and_params(
        &mut self,
        sql: &str,
        triggered_conditions: Option<std::collections::HashSet<usize>>,
        params: Params,
    ) -> Result<(Columns, Vec<Span>), DbError> {
        self.db.execute_traced_with_triggered_and_params(sql, triggered_conditions, params)
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
        // Ownership guard: derived tables are written exclusively by the
        // projection engine. Rejected BEFORE anything is applied.
        if let Some(engine) = self.projections.as_ref() {
            if let Err(v) = engine.guard_external(zset) {
                return Err(DbError::OwnedByProjection { table: v.table, owner: v.owner });
            }
        }
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
    /// shared runtime id used by the pull-API drain and the debug APIs; many
    /// handles may map to the same id.
    pub fn subscribe(
        &mut self,
        sql: &str,
    ) -> Result<(SubscriptionHandle, SubscriptionId), SubscribeError> {
        let key = SubscriptionKey::from_sql(sql);

        let sub_id = if let Some(&id) = self.by_key.get(&key) {
            let sub = self.subscriptions.get_mut(&id)
                .expect("invariant: by_key id must have a matching subscription");
            sub.refcount += 1;
            id
        } else {
            let stmt = sql_parser::parser::parse_statement(sql)?;
            let select = match stmt {
                Statement::Select(s) => s,
                _ => return Err(SubscribeError::NotSelect),
            };

            let table_schemas = self.db.table_schemas();
            let plan = sql_engine::planner::reactive::plan_reactive(
                &select,
                &table_schemas,
            )?;
            let sub_id = self.registry.subscribe(&plan.conditions, &plan.sources, &HashMap::new())?;

            self.subscriptions.insert(sub_id, Subscription {
                sql: sql.to_string(),
                key: key.clone(),
                pending_triggered: FnvHashSet::default(),
                refcount: 1,
            });
            self.by_key.insert(key, sub_id);
            self.alloc_slot(sub_id);
            sub_id
        };

        let handle = SubscriptionHandle(self.next_handle);
        self.next_handle += 1;
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
        let sub = self.subscriptions.get_mut(&sub_id)
            .expect("invariant: a live handle must point at a live subscription");
        sub.refcount -= 1;
        if sub.refcount == 0 {
            let sub = self.subscriptions.remove(&sub_id).expect("just checked");
            self.by_key.remove(&sub.key);
            self.free_slot(sub_id);
            self.registry.unsubscribe(sub_id);
        }
        true
    }

    // ── Slot allocator (dirty-set u32 slots) ─────────────────────────

    fn alloc_slot(&mut self, sub_id: SubscriptionId) {
        let slot = if let Some(s) = self.free_slots.pop() {
            self.slot_to_sub[s as usize] = Some(sub_id);
            s
        } else {
            let s = self.slot_to_sub.len() as u32;
            self.slot_to_sub.push(Some(sub_id));
            s
        };
        self.sub_to_slot.insert(sub_id, slot);
    }

    fn free_slot(&mut self, sub_id: SubscriptionId) {
        if let Some(slot) = self.sub_to_slot.remove(&sub_id) {
            if let Some(cell) = self.slot_to_sub.get_mut(slot as usize) {
                *cell = None;
            }
            self.free_slots.push(slot);
        }
    }

    /// Fire the wake callback if the dirty-set just transitioned from empty
    /// to non-empty. Edge-triggered: consecutive marks in the same drain
    /// cycle don't re-fire. `was_empty` must be snapshotted *before* the
    /// marks that may have caused the transition.
    fn fire_wake_if_transitioned(&self, was_empty: bool) {
        if was_empty && !self.dirty.is_empty() {
            if let Some(w) = &self.wake {
                w();
            }
        }
    }

    // ── Notify ───────────────────────────────────────────────────────

    /// Dispatch a ZSet to subscribers. If a projection engine is installed,
    /// a derive pass runs FIRST (recompute affected partitions, apply derived
    /// deltas) and subscribers are notified with the combined change set —
    /// source and derived tables always appear as ONE consistent update.
    /// Then marks affected subscriptions dirty, accumulates their
    /// triggered-condition indices, and fires the edge-triggered wake
    /// signal if the dirty-set transitions from empty to non-empty.
    ///
    /// Consumers drain via [`Self::next_dirty`] when ready.
    pub fn notify(&mut self, zset: &ZSet) {
        // Derivation must run even with zero subscribers — derived tables
        // stay consistent regardless of who is watching.
        let combined = self.derive_pass(zset);
        let zset = combined.as_ref().unwrap_or(zset);
        self.dispatch_to_subscribers(zset);
    }

    /// Registry routing + dirty marking + wake — the second half of
    /// `notify()`. `activate_projection`/`deactivate_projection` call ONLY
    /// this with their delta: it touches projection-owned tables
    /// exclusively and must never re-enter `derive_pass` (whose
    /// `guard_external` debug-assert would fire).
    fn dispatch_to_subscribers(&mut self, zset: &ZSet) {
        if self.subscriptions.is_empty() {
            return;
        }
        let was_empty = self.dirty.is_empty();
        let affected = on_zset(&self.registry, zset);
        for (sub_id, triggered) in affected {
            let Some(sub) = self.subscriptions.get_mut(&sub_id) else { continue };
            sub.pending_triggered.extend(triggered);
            if let Some(&slot) = self.sub_to_slot.get(&sub_id) {
                self.dirty.mark_dirty(DirtySlotId(slot));
            }
        }
        self.fire_wake_if_transitioned(was_empty);
    }

    /// Mark every live subscription dirty (with no triggered-condition
    /// precision — `DirtyNotification.triggered` will be empty for these
    /// marks). Used after bulk state changes (e.g. SyncClient rebuild)
    /// where a precise diff isn't available.
    pub fn notify_all(&mut self) {
        if self.subscriptions.is_empty() {
            return;
        }
        let was_empty = self.dirty.is_empty();
        for &slot in self.sub_to_slot.values() {
            self.dirty.mark_dirty(DirtySlotId(slot));
        }
        self.fire_wake_if_transitioned(was_empty);
    }

    // ── Pull API ─────────────────────────────────────────────────────

    /// Register the edge-triggered wake callback. Fires exactly once when the
    /// internal dirty-set transitions from empty to non-empty — subsequent
    /// marks in the same drain cycle do not re-fire. Intended as a signal for
    /// the consumer to schedule a drain (e.g. via `requestIdleCallback`).
    ///
    /// Only one wake callback is supported; subsequent calls replace it.
    pub fn on_dirty(&mut self, wake: Box<dyn Fn()>) {
        self.wake = Some(wake);
    }

    /// Pull the next dirty notification, or `None` when there's nothing to
    /// drain. The first call of a drain cycle snapshots the dirty-set into
    /// an internal buffer (atomically: iter + clear in one Rust call) so
    /// marks arriving between subsequent `next_dirty` calls land in the now
    /// empty dirty-set and will be surfaced in the *next* cycle — never
    /// dropped, never double-surfaced.
    pub fn next_dirty(&mut self) -> Option<DirtyNotification> {
        if self.drain_buffer.is_empty() && !self.dirty.is_empty() {
            // DirtySet's list path does not dedupe; a sub marked multiple times
            // on the fast path shows up multiple times in `iter()`. Dedup at
            // drain-build time so a sub surfaces exactly once per cycle.
            let mut seen: FnvHashSet<SubscriptionId> = FnvHashSet::default();
            for slot in self.dirty.iter() {
                let Some(sub_id) = self.slot_to_sub
                    .get(slot.0 as usize)
                    .copied()
                    .flatten()
                else { continue };
                if !seen.insert(sub_id) { continue; }
                let triggered: Vec<usize> = self.subscriptions
                    .get_mut(&sub_id)
                    .map(|s| std::mem::take(&mut s.pending_triggered).into_iter().collect())
                    .unwrap_or_default();
                self.drain_buffer.push_back(DirtyNotification { sub_id, triggered });
            }
            self.dirty.clear();
        }

        // Skip stale entries (subscription unsubscribed since snapshot).
        while let Some(n) = self.drain_buffer.pop_front() {
            if self.subscriptions.contains_key(&n.sub_id) {
                return Some(n);
            }
        }
        None
    }

    /// Replace the inner table data with a clone of `other`'s tables. Keeps
    /// the subscription registry, requirement registry, and fetcher runtime
    /// intact. Used by sync-client to rebuild optimistic from confirmed
    /// without losing subscriptions or registered fetchers.
    ///
    /// With projections installed, the wholesale swap invalidates the
    /// engine's `last_render` bookkeeping — derived state is dropped and
    /// rebuilt from the new source contents (derived tables are cache,
    /// always reconstructible). Callers are expected to `notify_all` after,
    /// as before.
    pub fn replace_data(&mut self, other: &Database) {
        self.db.replace_tables(other);
        if let Some(engine) = self.projections.as_mut() {
            if !engine.is_empty() {
                let outcome = {
                    let mut host = DatabaseHost::new(&mut self.db);
                    engine.reset_and_rederive(&mut host)
                };
                self.absorb_outcome_bookkeeping(outcome);
            }
        }
    }

    // ── Introspection ────────────────────────────────────────────────

    /// Number of unique (deduped) subscriptions currently live. A SQL string
    /// shared by N callers counts once.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Number of outstanding caller handles across all subscriptions.
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
