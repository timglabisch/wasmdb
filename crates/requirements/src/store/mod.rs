//! Runtime store for requirements.
//!
//! Synchronous orchestration over [`Slot`]. Owns the
//! `HashMap<Key, Slot>`, the subscriber holdings map, and the graph
//! walks (transitive subscribe/unsubscribe along `upstream`, status
//! propagation along `downstream`). Async fetch dispatch lives in the
//! embedder, which implements [`FetchDispatcher`] using its own runtime
//! and applies results back via [`RequirementStore::apply_ready`] /
//! [`RequirementStore::apply_error`].

pub mod key;
pub mod slot;

use std::collections::HashMap;
use std::sync::Arc;

use fnv::{FnvHashMap, FnvHashSet};
use sql_engine::execute::ParamValue;
use sql_parser::ast::Value;

pub use key::{make_derived_key, make_fetched_key};
pub use slot::{FetchError, RequirementKey, Slot, SlotKind, SlotState, SubscriberId};

/// Side-effect interface for starting / cancelling fetches. The store
/// stays sync and single-threaded; the dispatcher is where the
/// embedder's async runtime hooks in. Tests inject a recording mock.
pub trait FetchDispatcher {
    /// Start a fetch for `key`, tagged with `generation`. Implementations
    /// must arrange for the result to come back via
    /// [`RequirementStore::apply_ready`] / [`RequirementStore::apply_error`].
    fn dispatch(&mut self, key: &RequirementKey, kind: &SlotKind, generation: u64);

    /// Best-effort cancel the in-flight fetch tagged with `generation`.
    /// Late responses are still safely rejected by the slot's
    /// generation check, so a no-op cancel is acceptable.
    fn cancel(&mut self, key: &RequirementKey, generation: u64);
}

/// Runtime store: HashMap<Key, Slot> + subscriber holdings + graph walks.
pub struct RequirementStore {
    slots: FnvHashMap<RequirementKey, Slot>,
    /// Per-subscriber: which slots this subscriber's transitive walk
    /// touched. Mirror of `Subscription { refcount }` + `handles` in
    /// `database-reactive::ReactiveDatabase`.
    holdings: FnvHashMap<SubscriberId, Vec<RequirementKey>>,
    next_subscriber_id: u64,
}

impl RequirementStore {
    pub fn new() -> Self {
        Self {
            slots: FnvHashMap::default(),
            holdings: FnvHashMap::default(),
            next_subscriber_id: 0,
        }
    }

    // ── Read accessors ───────────────────────────────────────────────

    pub fn get(&self, key: &RequirementKey) -> Option<&Slot> {
        self.slots.get(key)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn subscriber_count(&self) -> usize {
        self.holdings.len()
    }

    // ── Slot creation ────────────────────────────────────────────────

    /// Get or create a Fetched slot. Returns the canonical key. Idempotent
    /// — calling with the same `(name, args)` returns the existing key
    /// without altering state.
    pub fn upsert_fetched(&mut self, name: &str, args: Vec<Value>) -> RequirementKey {
        let key = make_fetched_key(name, &args);
        if !self.slots.contains_key(&key) {
            let slot = Slot::new(
                key.clone(),
                SlotKind::Fetched {
                    registered_id: Arc::from(name),
                    args,
                },
                Vec::new(),
            );
            self.slots.insert(key.clone(), slot);
        }
        key
    }

    /// Get or create a Derived slot. Upstream slots must already be
    /// registered. Returns the canonical key. Idempotent.
    pub fn upsert_derived(
        &mut self,
        sql: Arc<str>,
        params: HashMap<String, ParamValue>,
        upstream: Vec<RequirementKey>,
        name: Option<Arc<str>>,
    ) -> RequirementKey {
        let key = make_derived_key(&sql, &params, &upstream);
        if self.slots.contains_key(&key) {
            return key;
        }
        for u in &upstream {
            assert!(
                self.slots.contains_key(u),
                "upsert_derived: upstream not registered: {}",
                u.as_str()
            );
        }
        for u in &upstream {
            self.slots
                .get_mut(u)
                .expect("checked above")
                .downstream
                .push(key.clone());
        }
        let slot = Slot::new(
            key.clone(),
            SlotKind::Derived { sql, params, name },
            upstream,
        );
        self.slots.insert(key.clone(), slot);
        key
    }

    // ── Subscribe / unsubscribe ──────────────────────────────────────

    /// Subscribe to `key`. Walks `upstream` transitively, increments
    /// refcounts on every reached slot, dispatches initial fetches for
    /// any newly-activated Fetched leaves, and recomputes Derived
    /// statuses from their (possibly newly-Loading) upstreams.
    ///
    /// Returns a fresh `SubscriberId`. Pair with [`Self::unsubscribe`].
    pub fn subscribe<D: FetchDispatcher>(
        &mut self,
        key: &RequirementKey,
        dispatcher: &mut D,
    ) -> SubscriberId {
        debug_assert!(
            self.slots.contains_key(key),
            "subscribe: key not registered: {}",
            key.as_str()
        );

        let order = self.collect_upstream_postorder(key);

        let mut activated: Vec<RequirementKey> = Vec::new();
        for k in &order {
            if self
                .slots
                .get_mut(k)
                .expect("walked slot exists")
                .incref()
            {
                activated.push(k.clone());
            }
        }

        for k in &activated {
            let (kind, generation, upstream_keys) = {
                let slot = &self.slots[k];
                (slot.kind.clone(), slot.generation, slot.upstream.clone())
            };
            match &kind {
                SlotKind::Fetched { .. } => {
                    self.slots.get_mut(k).unwrap().start_fetch();
                    dispatcher.dispatch(k, &kind, generation);
                }
                SlotKind::Derived { .. } => {
                    let states: Vec<SlotState> = upstream_keys
                        .iter()
                        .map(|u| self.slots[u].state)
                        .collect();
                    self.slots
                        .get_mut(k)
                        .unwrap()
                        .recompute_status_from_upstream(&states);
                }
            }
        }

        let sub_id = SubscriberId(self.next_subscriber_id);
        self.next_subscriber_id += 1;
        self.holdings.insert(sub_id, order);
        sub_id
    }

    /// Release a subscriber. Decrefs all slots its subscribe-walk touched;
    /// any slot reaching refcount 0 is dropped immediately.
    ///
    /// Unknown `sub_id` is a no-op (matches the `false` return of
    /// `ReactiveDatabase::unsubscribe`).
    pub fn unsubscribe(&mut self, sub: SubscriberId) -> bool {
        let Some(held) = self.holdings.remove(&sub) else {
            return false;
        };
        // Reverse postorder = root first, so cleanups walk from a slot
        // toward its still-alive upstreams.
        for k in held.iter().rev() {
            let now_zero = self
                .slots
                .get_mut(k)
                .expect("held slot still in store")
                .decref();
            if now_zero {
                self.drop_slot(k);
            }
        }
        true
    }

    // ── Invalidate ───────────────────────────────────────────────────

    /// Bump the generation; cancel any in-flight fetch (best-effort);
    /// if subscribed and Fetched, dispatch a fresh fetch tagged with
    /// the new generation. Guarantees that *some* fetch is started
    /// (or already in-flight at the new generation) after this call,
    /// so a subsequent subscribe will get fresh data.
    ///
    /// On Derived: bumps the generation, no fetch dispatch (Derived has
    /// no HTTP fetch). For transitive HTTP refetches, see future
    /// `invalidate_deep`.
    pub fn invalidate<D: FetchDispatcher>(
        &mut self,
        key: &RequirementKey,
        dispatcher: &mut D,
    ) {
        let (old_gen, new_gen, was_inflight, kind_for_dispatch, refcount, is_fetched) = {
            let slot = self
                .slots
                .get_mut(key)
                .expect("invalidate: slot not registered");
            let old_gen = slot.generation;
            slot.invalidate();
            let new_gen = slot.generation;
            let was_inflight = std::mem::replace(&mut slot.inflight, false);
            let kind = slot.kind.clone();
            let refcount = slot.refcount;
            let is_fetched = matches!(slot.kind, SlotKind::Fetched { .. });
            (old_gen, new_gen, was_inflight, kind, refcount, is_fetched)
        };

        if was_inflight {
            dispatcher.cancel(key, old_gen);
        }
        if refcount > 0 && is_fetched {
            self.slots.get_mut(key).unwrap().start_fetch();
            dispatcher.dispatch(key, &kind_for_dispatch, new_gen);
        }
    }

    // ── Result delivery ──────────────────────────────────────────────

    /// Deliver a successful fetch. Returns `false` if the apply was
    /// stale (later invalidate has bumped the generation past `gen`).
    /// On success, propagates the new state to downstream Deriveds.
    pub fn apply_ready(&mut self, key: &RequirementKey, gen: u64) -> bool {
        let applied = self
            .slots
            .get_mut(key)
            .expect("apply_ready: slot not registered")
            .apply_ready(gen);
        if applied {
            self.propagate_status_to_downstream(key);
        }
        applied
    }

    /// Deliver a failed fetch. Returns `false` if stale.
    pub fn apply_error(&mut self, key: &RequirementKey, gen: u64, err: FetchError) -> bool {
        let applied = self
            .slots
            .get_mut(key)
            .expect("apply_error: slot not registered")
            .apply_error(gen, err);
        if applied {
            self.propagate_status_to_downstream(key);
        }
        applied
    }

    // ── Internals: graph walks ───────────────────────────────────────

    /// DFS post-order over `upstream`. Each reachable slot appears
    /// exactly once; leaves come before inner nodes.
    fn collect_upstream_postorder(&self, start: &RequirementKey) -> Vec<RequirementKey> {
        let mut visited = FnvHashSet::default();
        let mut order = Vec::new();
        self.dfs_post(start, &mut visited, &mut order);
        order
    }

    fn dfs_post(
        &self,
        k: &RequirementKey,
        visited: &mut FnvHashSet<RequirementKey>,
        order: &mut Vec<RequirementKey>,
    ) {
        if !visited.insert(k.clone()) {
            return;
        }
        // Clone to release the borrow on self.slots before recursing.
        let upstream: Vec<RequirementKey> = self.slots[k].upstream.clone();
        for u in &upstream {
            self.dfs_post(u, visited, order);
        }
        order.push(k.clone());
    }

    /// BFS over `downstream`, recomputing each Derived's status from its
    /// upstreams. Only re-enqueues a downstream slot's own downstream if
    /// the recomputation actually changed state — avoids unnecessary
    /// fan-out when status was already consistent.
    fn propagate_status_to_downstream(&mut self, key: &RequirementKey) {
        let mut worklist: Vec<RequirementKey> = self.slots[key].downstream.clone();
        while let Some(d_key) = worklist.pop() {
            let upstream_keys: Vec<RequirementKey> = self.slots[&d_key].upstream.clone();
            let states: Vec<SlotState> = upstream_keys
                .iter()
                .map(|u| self.slots[u].state)
                .collect();
            let prev = self.slots[&d_key].state;
            self.slots
                .get_mut(&d_key)
                .unwrap()
                .recompute_status_from_upstream(&states);
            let new_state = self.slots[&d_key].state;
            if prev != new_state {
                for further in &self.slots[&d_key].downstream {
                    worklist.push(further.clone());
                }
            }
        }
    }

    // ── Internals: drop ──────────────────────────────────────────────

    fn drop_slot(&mut self, key: &RequirementKey) {
        let upstream: Vec<RequirementKey> = self.slots[key].upstream.clone();
        for u in &upstream {
            if let Some(us) = self.slots.get_mut(u) {
                us.downstream.retain(|d| d != key);
            }
        }
        self.slots.remove(key);
    }
}

impl Default for RequirementStore {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Recording dispatcher: appends every `dispatch` / `cancel` call to
    /// a Vec for assertion. Generation captured at call site so tests
    /// can verify the generation contract.
    #[derive(Default)]
    struct MockDispatcher {
        dispatched: Vec<(RequirementKey, u64)>,
        cancelled: Vec<(RequirementKey, u64)>,
    }
    impl FetchDispatcher for MockDispatcher {
        fn dispatch(&mut self, key: &RequirementKey, _kind: &SlotKind, generation: u64) {
            self.dispatched.push((key.clone(), generation));
        }
        fn cancel(&mut self, key: &RequirementKey, generation: u64) {
            self.cancelled.push((key.clone(), generation));
        }
    }

    fn upsert_fetched_int(
        store: &mut RequirementStore,
        name: &str,
        i: i64,
    ) -> RequirementKey {
        store.upsert_fetched(name, vec![Value::Int(i)])
    }

    fn state_of(store: &RequirementStore, k: &RequirementKey) -> SlotState {
        store.get(k).expect("slot exists").state
    }

    fn refcount_of(store: &RequirementStore, k: &RequirementKey) -> u32 {
        store.get(k).expect("slot exists").refcount
    }

    // ── invariants ───────────────────────────────────────────────────

    /// Walk every internal data structure and assert invariants. Called
    /// after every state transition in churn tests. Pattern lifted from
    /// `sql_engine::reactive::registry`.
    fn check_invariants(store: &RequirementStore) {
        // 1. holdings refer to live slots only.
        for (sub, held) in &store.holdings {
            for k in held {
                assert!(
                    store.slots.contains_key(k),
                    "[1] sub {sub:?} holds dangling key {}",
                    k.as_str()
                );
            }
        }

        // 2. refcount equals number of distinct (sub, key) holdings.
        //    Each holdings entry contributes 1 to its slot's refcount.
        let mut accounted: FnvHashMap<RequirementKey, u32> = FnvHashMap::default();
        for held in store.holdings.values() {
            for k in held {
                *accounted.entry(k.clone()).or_insert(0) += 1;
            }
        }
        for (k, slot) in &store.slots {
            let from_holdings = accounted.get(k).copied().unwrap_or(0);
            assert_eq!(
                slot.refcount,
                from_holdings,
                "[2] refcount mismatch for {}: stored={}, accounted={}",
                k.as_str(),
                slot.refcount,
                from_holdings
            );
        }

        // 3. upstream/downstream are mutually consistent.
        for (k, slot) in &store.slots {
            for u_key in &slot.upstream {
                let u = store.slots.get(u_key).unwrap_or_else(|| {
                    panic!("[3] {} lists upstream {} not in store", k.as_str(), u_key.as_str())
                });
                assert!(
                    u.downstream.contains(k),
                    "[3] {} lists upstream {} but {} does not list {} as downstream",
                    k.as_str(),
                    u_key.as_str(),
                    u_key.as_str(),
                    k.as_str()
                );
            }
            for d_key in &slot.downstream {
                let d = store.slots.get(d_key).unwrap_or_else(|| {
                    panic!("[3] {} lists downstream {} not in store", k.as_str(), d_key.as_str())
                });
                assert!(
                    d.upstream.contains(k),
                    "[3] {} lists downstream {} but {} does not list {} as upstream",
                    k.as_str(),
                    d_key.as_str(),
                    d_key.as_str(),
                    k.as_str()
                );
            }
        }
    }

    // ── single Fetched ───────────────────────────────────────────────

    #[test]
    fn subscribe_fetched_dispatches_with_gen_zero() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);

        let _sub = store.subscribe(&k, &mut d);

        assert_eq!(d.dispatched, vec![(k.clone(), 0)]);
        assert!(d.cancelled.is_empty());
        assert_eq!(state_of(&store, &k), SlotState::Loading);
        assert_eq!(refcount_of(&store, &k), 1);
        check_invariants(&store);
    }

    #[test]
    fn subscribe_fetched_apply_ready_transitions_to_ready() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        store.subscribe(&k, &mut d);

        assert!(store.apply_ready(&k, 0));
        assert_eq!(state_of(&store, &k), SlotState::Ready);
        check_invariants(&store);
    }

    #[test]
    fn apply_ready_with_stale_generation_rejected() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        store.subscribe(&k, &mut d);

        store.invalidate(&k, &mut d); // gen 0 → 1

        // Old fetch returns late with gen=0:
        assert!(!store.apply_ready(&k, 0));
        assert_eq!(state_of(&store, &k), SlotState::Loading);
    }

    // ── Derived requires Fetched ─────────────────────────────────────

    #[test]
    fn subscribe_derived_dispatches_only_upstream_fetched() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let f = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![f.clone()],
            None,
        );

        store.subscribe(&der, &mut d);

        // Only the Fetched leaf gets dispatched; Derived is not HTTP-fetched.
        assert_eq!(d.dispatched, vec![(f.clone(), 0)]);
        // Both refs incremented.
        assert_eq!(refcount_of(&store, &f), 1);
        assert_eq!(refcount_of(&store, &der), 1);
        // Derived state reflects upstream's Loading.
        assert_eq!(state_of(&store, &f), SlotState::Loading);
        assert_eq!(state_of(&store, &der), SlotState::Loading);
        check_invariants(&store);
    }

    #[test]
    fn derived_becomes_ready_when_all_upstreams_ready() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let a = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let b = upsert_fetched_int(&mut store, "positions.by_invoice", 1);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![a.clone(), b.clone()],
            None,
        );
        store.subscribe(&der, &mut d);

        assert_eq!(state_of(&store, &der), SlotState::Loading);

        // Only one upstream becomes Ready — Derived stays Loading.
        store.apply_ready(&a, 0);
        assert_eq!(state_of(&store, &der), SlotState::Loading);

        // Second upstream becomes Ready — Derived flips to Ready.
        store.apply_ready(&b, 0);
        assert_eq!(state_of(&store, &der), SlotState::Ready);
        check_invariants(&store);
    }

    #[test]
    fn derived_becomes_error_when_any_upstream_errors() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let a = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let b = upsert_fetched_int(&mut store, "positions.by_invoice", 1);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![a.clone(), b.clone()],
            None,
        );
        store.subscribe(&der, &mut d);

        store.apply_ready(&a, 0);
        store.apply_error(&b, 0, FetchError::Network("x".into()));
        assert_eq!(state_of(&store, &der), SlotState::Error);
    }

    #[test]
    fn deeply_nested_derived_propagates_status() {
        // F → D1 → D2; subscribe(D2) should dispatch F, leave D1/D2 Loading.
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let f = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let d1 = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![f.clone()],
            None,
        );
        let d2 = store.upsert_derived(
            Arc::from("SELECT 2"),
            HashMap::new(),
            vec![d1.clone()],
            None,
        );
        store.subscribe(&d2, &mut d);

        assert_eq!(d.dispatched, vec![(f.clone(), 0)]);
        assert_eq!(state_of(&store, &f), SlotState::Loading);
        assert_eq!(state_of(&store, &d1), SlotState::Loading);
        assert_eq!(state_of(&store, &d2), SlotState::Loading);

        store.apply_ready(&f, 0);
        // Propagation: D1 from upstream F=Ready → D1 Ready; D2 from D1=Ready → D2 Ready.
        assert_eq!(state_of(&store, &d1), SlotState::Ready);
        assert_eq!(state_of(&store, &d2), SlotState::Ready);
        check_invariants(&store);
    }

    // ── shared upstream / multi-subscriber ───────────────────────────

    #[test]
    fn two_subscribers_share_slot() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);

        let _s1 = store.subscribe(&k, &mut d);
        let _s2 = store.subscribe(&k, &mut d);

        // Second subscribe must NOT trigger another dispatch.
        assert_eq!(d.dispatched.len(), 1);
        assert_eq!(refcount_of(&store, &k), 2);
        check_invariants(&store);
    }

    #[test]
    fn unsubscribe_one_keeps_slot_alive() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let s1 = store.subscribe(&k, &mut d);
        let _s2 = store.subscribe(&k, &mut d);

        assert!(store.unsubscribe(s1));
        assert!(store.get(&k).is_some());
        assert_eq!(refcount_of(&store, &k), 1);
        check_invariants(&store);
    }

    #[test]
    fn unsubscribe_last_drops_slot() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let s = store.subscribe(&k, &mut d);

        store.unsubscribe(s);
        assert!(store.get(&k).is_none());
        assert_eq!(store.slot_count(), 0);
        check_invariants(&store);
    }

    #[test]
    fn unsubscribe_drops_derived_and_cleans_downstream_backlinks() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let f = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![f.clone()],
            None,
        );

        // Before subscribe: f.downstream contains der.
        assert!(store.get(&f).unwrap().downstream.contains(&der));

        let s = store.subscribe(&der, &mut d);
        store.unsubscribe(s);

        // Both slots dropped.
        assert!(store.get(&f).is_none());
        assert!(store.get(&der).is_none());
        check_invariants(&store);
    }

    #[test]
    fn unsubscribe_unknown_subscriber_is_no_op() {
        let mut store = RequirementStore::new();
        assert!(!store.unsubscribe(SubscriberId(999)));
    }

    // ── invalidate ───────────────────────────────────────────────────

    #[test]
    fn invalidate_subscribed_dispatches_with_new_gen_and_cancels_old() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        store.subscribe(&k, &mut d);
        // Initial dispatch at gen 0:
        assert_eq!(d.dispatched, vec![(k.clone(), 0)]);

        store.invalidate(&k, &mut d);

        // Cancel old generation, dispatch new generation.
        assert_eq!(d.cancelled, vec![(k.clone(), 0)]);
        assert_eq!(d.dispatched, vec![(k.clone(), 0), (k.clone(), 1)]);
        assert_eq!(store.get(&k).unwrap().generation, 1);
    }

    #[test]
    fn invalidate_unsubscribed_only_bumps_gen_no_dispatch() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);

        store.invalidate(&k, &mut d);

        assert!(d.dispatched.is_empty());
        assert!(d.cancelled.is_empty());
        assert_eq!(store.get(&k).unwrap().generation, 1);
    }

    #[test]
    fn subscribe_after_invalidate_uses_current_gen() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let k = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        store.invalidate(&k, &mut d); // gen 0 → 1, no dispatch yet
        store.subscribe(&k, &mut d);

        assert_eq!(d.dispatched, vec![(k.clone(), 1)]);
    }

    #[test]
    fn invalidate_on_derived_only_bumps_gen() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();
        let f = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![f.clone()],
            None,
        );
        store.subscribe(&der, &mut d);
        let dispatched_before = d.dispatched.len();
        let cancelled_before = d.cancelled.len();

        store.invalidate(&der, &mut d);

        assert_eq!(d.dispatched.len(), dispatched_before);
        assert_eq!(d.cancelled.len(), cancelled_before);
        assert_eq!(store.get(&der).unwrap().generation, 1);
    }

    // ── churn / invariants under load ────────────────────────────────

    #[test]
    fn invariants_hold_under_subscribe_unsubscribe_churn() {
        let mut store = RequirementStore::new();
        let mut d = MockDispatcher::default();

        let f1 = upsert_fetched_int(&mut store, "invoices.by_id", 1);
        let f2 = upsert_fetched_int(&mut store, "invoices.by_id", 2);
        let der = store.upsert_derived(
            Arc::from("SELECT 1"),
            HashMap::new(),
            vec![f1.clone(), f2.clone()],
            None,
        );
        check_invariants(&store);

        let s1 = store.subscribe(&f1, &mut d);
        check_invariants(&store);
        let s2 = store.subscribe(&der, &mut d);
        check_invariants(&store);
        let s3 = store.subscribe(&f2, &mut d);
        check_invariants(&store);

        // f1 held by s1 and s2(via der); f2 held by s2(via der) and s3.
        assert_eq!(refcount_of(&store, &f1), 2);
        assert_eq!(refcount_of(&store, &f2), 2);
        assert_eq!(refcount_of(&store, &der), 1);

        store.unsubscribe(s2);
        check_invariants(&store);
        // der dropped; f1, f2 still alive.
        assert!(store.get(&der).is_none());
        assert_eq!(refcount_of(&store, &f1), 1);
        assert_eq!(refcount_of(&store, &f2), 1);

        store.unsubscribe(s1);
        check_invariants(&store);
        assert!(store.get(&f1).is_none());
        assert!(store.get(&f2).is_some());

        store.unsubscribe(s3);
        check_invariants(&store);
        assert_eq!(store.slot_count(), 0);
        assert_eq!(store.subscriber_count(), 0);
    }
}
