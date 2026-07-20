//! Per-identity record in the requirement store.
//!
//! Pure data + mutation methods. No async, no IO, no store wiring — that
//! lives in `store/mod.rs`. See `wasmdb-requirements-design.md` for the
//! role of `Slot` in the wider system, especially the generation-counter
//! correctness invariant and the stale-while-revalidate pattern.

use std::collections::HashMap;
use std::sync::Arc;

use sql_engine::execute::ParamValue;
use sql_parser::ast::Value;

/// Stable, hashable identity of a requirement in the store. Construction
/// is the store's job; this newtype only fixes the *type* of identity.
///
/// Canonical form (built by store helpers, not enforced here):
/// - Fetched: `"fetched:<registered_id>:<canonicalJson(args)>"`
/// - Derived: `"derived:<canonical(sql)>|{params}|[upstream_keys]"`
/// - Projected: `"projected:<projection_id>:<partition_repr>"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequirementKey(pub Arc<str>);

impl RequirementKey {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Subscriber identity inside the store. Process-local monotonic counter
/// minted by the store; not portable across processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriberId(pub u64);

/// What kind of requirement this slot represents. Determines fetch path
/// (HTTP via registry vs. local SQL eval) but not lifecycle mechanics —
/// those are uniform across both kinds.
#[derive(Debug, Clone)]
pub enum SlotKind {
    /// Server-defined leaf — emitted via `#[query]`. HTTP-fetched.
    Fetched {
        /// `RequirementRegistry` key, e.g. `"invoices.by_id"`.
        registered_id: Arc<str>,
        /// Positional args passed to the registered fetcher closure.
        args: Vec<Value>,
    },
    /// Anonymous client-side composition created by `useQuery({sql, requires})`.
    /// "Fetch" here means: run SQL on the local DB once all upstream
    /// `Fetched` slots are `Ready`.
    Derived {
        sql: Arc<str>,
        params: HashMap<String, ParamValue>,
        /// Optional human-readable name for DevTools display. Does not
        /// participate in identity.
        name: Option<Arc<str>>,
    },
    /// One partition of a registered projection (derived table maintained
    /// by the projection engine). Never fetched — the engine materializes
    /// it at the apply/notify chokepoint. The slot only tracks loading
    /// status (aggregated from the upstream log requirement, like
    /// `Derived`) and per-partition `project()` failures reported via
    /// `RequirementStore::report_projection_failure`.
    Projected {
        /// `ProjectionSpec::id` of the registered projection.
        projection_id: Arc<str>,
        /// Canonical display form of the partition (decimal for I64, raw
        /// for Str, hyphenated lowercase for Uuid) — must match the
        /// engine's `DeriveFailure::partition` formatting.
        partition_repr: Arc<str>,
    },
}

/// Loading-status tag. Independent of `inflight` — the combination
/// `state == Ready && inflight` is stale-while-revalidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Constructed but never fetched. Only valid before the first incref.
    Idle,
    /// Initial load in progress; no data to show yet.
    Loading,
    /// Has valid data. May coexist with an in-flight refresh.
    Ready,
    /// Last initial-load attempt failed; no data to show. May coexist
    /// with an in-flight retry.
    Error,
}

/// Transport-level outcome of a failed fetch.
#[derive(Debug, Clone)]
pub enum FetchError {
    /// Network layer failure (DNS, TCP, TLS, connection reset, ...).
    Network(String),
    /// Server returned a non-2xx response.
    Server { status: u16, body: String },
    /// Response parsed but failed to decode into the expected row shape.
    Decode(String),
    /// Fetch was aborted — typically because a later `invalidate`
    /// bumped the generation while this fetch was in flight.
    Cancelled,
    /// A projection's `project()` failed for this slot's key. The key's
    /// previous output stays materialized; the error pins the slot to
    /// `Error` until the key re-derives successfully or `invalidate`
    /// clears it.
    Projection(String),
}

/// Per-identity record in the requirement store. See module docs.
#[derive(Debug)]
pub struct Slot {
    pub key: RequirementKey,
    pub kind: SlotKind,
    pub state: SlotState,

    /// Monotonic counter bumped by every `invalidate`. A fetch result is
    /// only applied if its `started_at_generation == self.generation`.
    pub generation: u64,

    /// Number of active subscribers. The store owns the
    /// `SubscriberId → Vec<RequirementKey>` mapping (parallels
    /// `ReactiveDatabase::handles`); the slot only counts.
    pub refcount: u32,

    /// Most recent failure. Cleared on `invalidate` and on successful apply.
    pub last_error: Option<FetchError>,

    /// `true` while a fetch has been dispatched but not yet applied.
    /// Cancellation handles live in the embedder, not here.
    pub inflight: bool,

    /// Graph edges, both directions cached. `upstream` drives transitive
    /// subscribe / `invalidateDeep`; `downstream` drives status
    /// propagation.
    pub upstream: Vec<RequirementKey>,
    pub downstream: Vec<RequirementKey>,
}

impl Slot {
    pub fn new(key: RequirementKey, kind: SlotKind, upstream: Vec<RequirementKey>) -> Self {
        Self {
            key,
            kind,
            state: SlotState::Idle,
            generation: 0,
            refcount: 0,
            last_error: None,
            inflight: false,
            upstream,
            downstream: Vec::new(),
        }
    }

    /// Increment the refcount. Returns `true` iff this transitioned the
    /// slot from refcount 0 → 1 (so the store can trigger initial fetch).
    ///
    /// Subscriber-identity tracking lives at the store level — see the
    /// `Subscription { refcount }` + `ReactiveDatabase::handles` split
    /// in `database-reactive`.
    pub fn incref(&mut self) -> bool {
        let was_zero = self.refcount == 0;
        self.refcount += 1;
        was_zero
    }

    /// Decrement the refcount. Returns `true` iff this released the
    /// *last* subscriber (refcount hit 0). Underflows are a logic error
    /// from the caller — debug builds panic, release builds saturate.
    pub fn decref(&mut self) -> bool {
        debug_assert!(self.refcount > 0, "decref on slot with refcount 0");
        self.refcount = self.refcount.saturating_sub(1);
        self.refcount == 0
    }

    /// Record that a fetch has been dispatched. Transitions `state` only
    /// when starting from `Idle` — refresh-from-Ready and retry-from-Error
    /// keep their state so existing data / error UI stays visible during
    /// the in-flight.
    pub fn start_fetch(&mut self) {
        self.inflight = true;
        if matches!(self.state, SlotState::Idle) {
            self.state = SlotState::Loading;
        }
    }

    /// Apply a successful fetch result. Returns `false` and discards if
    /// `fetch_generation < self.generation` — a later `invalidate` has
    /// obsoleted this response and the store has already started a
    /// fresher fetch (or will on next subscribe).
    #[must_use]
    pub fn apply_ready(&mut self, fetch_generation: u64) -> bool {
        if fetch_generation < self.generation {
            return false;
        }
        self.state = SlotState::Ready;
        self.inflight = false;
        self.last_error = None;
        true
    }

    /// Apply a failed fetch. Returns `false` if stale.
    /// Initial-load failure (`Loading`) → `Error`. Refresh failure from
    /// `Ready` keeps `state = Ready` so existing data stays visible —
    /// the error is reported via `last_error` only.
    #[must_use]
    pub fn apply_error(&mut self, fetch_generation: u64, err: FetchError) -> bool {
        if fetch_generation < self.generation {
            return false;
        }
        self.last_error = Some(err);
        self.inflight = false;
        if matches!(self.state, SlotState::Loading) {
            self.state = SlotState::Error;
        }
        true
    }

    /// Bump the generation counter. The store is responsible for
    /// best-effort cancelling any existing inflight fetch (via the
    /// embedder's dispatcher) and (if subscribed) starting a new fetch
    /// tagged with the new generation.
    pub fn invalidate(&mut self) {
        self.generation += 1;
        self.last_error = None;
    }

    /// Recompute Derived/Projected status from upstream slot states.
    /// No-op for `Fetched` (its state is owned by its own fetch path).
    ///
    /// Aggregation rule: any `Error` wins; else any `Loading`/`Idle`
    /// keeps it loading; else all `Ready` → `Ready`. Empty upstream
    /// list → `Ready` (Derived with no requirements is trivially ready
    /// once subscribed — its SQL can run immediately).
    ///
    /// A Projected slot with a recorded projection failure stays pinned
    /// to `Error` regardless of upstream states — the failure is a
    /// property of the key's own render, not of its inputs. Cleared via
    /// [`Self::clear_projection_error`] or [`Self::invalidate`].
    pub fn recompute_status_from_upstream(&mut self, upstream_states: &[SlotState]) {
        match self.kind {
            SlotKind::Fetched { .. } => {}
            SlotKind::Derived { .. } => {
                self.state = aggregate_status(upstream_states);
            }
            SlotKind::Projected { .. } => {
                if matches!(self.last_error, Some(FetchError::Projection(_))) {
                    self.state = SlotState::Error;
                } else {
                    self.state = aggregate_status(upstream_states);
                }
            }
        }
    }

    /// Record a `project()` failure for this Projected slot's key. Pins
    /// the state to `Error` until the key re-derives successfully
    /// ([`Self::clear_projection_error`]) or `invalidate` resets it.
    /// Returns `false` when the identical failure is already pinned — the
    /// caller then skips change notifications.
    pub fn apply_projection_error(&mut self, message: String) -> bool {
        debug_assert!(
            matches!(self.kind, SlotKind::Projected { .. }),
            "apply_projection_error on non-Projected slot {}",
            self.key.as_str()
        );
        if self.state == SlotState::Error {
            if let Some(FetchError::Projection(prev)) = &self.last_error {
                if *prev == message {
                    return false;
                }
            }
        }
        self.last_error = Some(FetchError::Projection(message));
        self.state = SlotState::Error;
        true
    }

    /// Clear a recorded projection failure (the key re-derived
    /// successfully). Returns `true` iff a projection error was present —
    /// the caller then recomputes the state from upstream.
    pub fn clear_projection_error(&mut self) -> bool {
        if matches!(self.last_error, Some(FetchError::Projection(_))) {
            self.last_error = None;
            true
        } else {
            false
        }
    }
}

fn aggregate_status(states: &[SlotState]) -> SlotState {
    if states.is_empty() {
        return SlotState::Ready;
    }
    let mut has_error = false;
    let mut has_pending = false;
    for s in states {
        match s {
            SlotState::Error => has_error = true,
            SlotState::Loading | SlotState::Idle => has_pending = true,
            SlotState::Ready => {}
        }
    }
    if has_error {
        SlotState::Error
    } else if has_pending {
        SlotState::Loading
    } else {
        SlotState::Ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fetched_slot() -> Slot {
        Slot::new(
            RequirementKey::new("fetched:invoices.by_id:[1]"),
            SlotKind::Fetched {
                registered_id: Arc::from("invoices.by_id"),
                args: vec![Value::Int(1)],
            },
            Vec::new(),
        )
    }

    fn derived_slot(upstream: Vec<RequirementKey>) -> Slot {
        Slot::new(
            RequirementKey::new("derived:abc"),
            SlotKind::Derived {
                sql: Arc::from("SELECT 1"),
                params: HashMap::new(),
                name: None,
            },
            upstream,
        )
    }

    // ── refcount ─────────────────────────────────────────────────────

    #[test]
    fn incref_first_returns_was_zero_true() {
        let mut s = fetched_slot();
        assert!(s.incref());
        assert_eq!(s.refcount, 1);
    }

    #[test]
    fn incref_second_returns_was_zero_false() {
        let mut s = fetched_slot();
        assert!(s.incref());
        assert!(!s.incref());
        assert_eq!(s.refcount, 2);
    }

    #[test]
    fn decref_to_zero_returns_now_zero_true() {
        let mut s = fetched_slot();
        s.incref();
        assert!(s.decref());
        assert_eq!(s.refcount, 0);
    }

    #[test]
    fn decref_with_others_present_returns_now_zero_false() {
        let mut s = fetched_slot();
        s.incref();
        s.incref();
        assert!(!s.decref());
        assert_eq!(s.refcount, 1);
    }

    // ── start_fetch: state transitions ───────────────────────────────

    #[test]
    fn start_fetch_from_idle_transitions_to_loading() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert_eq!(s.state, SlotState::Loading);
        assert!(s.inflight);
    }

    #[test]
    fn start_fetch_from_ready_keeps_ready_for_swr() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_ready(0));
        assert_eq!(s.state, SlotState::Ready);

        s.start_fetch();
        assert_eq!(s.state, SlotState::Ready);
        assert!(s.inflight);
    }

    #[test]
    fn start_fetch_from_error_keeps_error_during_retry() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_error(0, FetchError::Network("x".into())));
        assert_eq!(s.state, SlotState::Error);

        s.start_fetch();
        assert_eq!(s.state, SlotState::Error);
        assert!(s.inflight);
    }

    // ── apply_ready / apply_error ────────────────────────────────────

    #[test]
    fn apply_ready_from_loading_transitions_to_ready() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_ready(0));
        assert_eq!(s.state, SlotState::Ready);
        assert!(!s.inflight);
        assert!(s.last_error.is_none());
    }

    #[test]
    fn apply_error_from_loading_transitions_to_error() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_error(0, FetchError::Network("x".into())));
        assert_eq!(s.state, SlotState::Error);
        assert!(matches!(s.last_error, Some(FetchError::Network(_))));
        assert!(!s.inflight);
    }

    #[test]
    fn apply_error_during_refresh_keeps_ready_state() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_ready(0));
        // Refresh starts:
        s.start_fetch();
        // Refresh fails — state must stay Ready (data still valid):
        assert!(s.apply_error(0, FetchError::Network("x".into())));
        assert_eq!(s.state, SlotState::Ready);
        assert!(matches!(s.last_error, Some(FetchError::Network(_))));
    }

    #[test]
    fn apply_clears_last_error_on_success() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_error(0, FetchError::Network("x".into())));
        s.start_fetch();
        assert!(s.apply_ready(0));
        assert!(s.last_error.is_none());
    }

    // ── generation: stale-apply rejection ────────────────────────────

    #[test]
    fn invalidate_bumps_generation_and_clears_last_error() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_error(0, FetchError::Network("x".into())));

        s.invalidate();
        assert_eq!(s.generation, 1);
        assert!(s.last_error.is_none());
    }

    #[test]
    fn apply_ready_with_stale_generation_returns_false_and_no_state_change() {
        let mut s = fetched_slot();
        s.start_fetch();
        s.invalidate(); // generation: 0 → 1
        // Old fetch returns late, with its old generation:
        assert!(!s.apply_ready(0));
        // State must NOT have transitioned to Ready:
        assert_eq!(s.state, SlotState::Loading);
    }

    #[test]
    fn apply_error_with_stale_generation_returns_false_and_no_state_change() {
        let mut s = fetched_slot();
        s.start_fetch();
        s.invalidate(); // generation: 0 → 1
        assert!(!s.apply_error(0, FetchError::Network("x".into())));
        assert_eq!(s.state, SlotState::Loading);
        assert!(s.last_error.is_none());
    }

    #[test]
    fn fresh_fetch_after_invalidate_is_applied() {
        let mut s = fetched_slot();
        s.start_fetch();
        s.invalidate(); // generation: 0 → 1
        // Store starts a fresh fetch with the new generation:
        s.start_fetch();
        assert!(s.apply_ready(1));
        assert_eq!(s.state, SlotState::Ready);
    }

    // ── Derived status aggregation ───────────────────────────────────

    #[test]
    fn derived_with_empty_upstream_aggregates_to_ready() {
        let mut s = derived_slot(Vec::new());
        s.recompute_status_from_upstream(&[]);
        assert_eq!(s.state, SlotState::Ready);
    }

    #[test]
    fn derived_with_all_ready_upstreams_is_ready() {
        let mut s = derived_slot(vec![RequirementKey::new("a"), RequirementKey::new("b")]);
        s.recompute_status_from_upstream(&[SlotState::Ready, SlotState::Ready]);
        assert_eq!(s.state, SlotState::Ready);
    }

    #[test]
    fn derived_with_one_loading_upstream_is_loading() {
        let mut s = derived_slot(vec![RequirementKey::new("a"), RequirementKey::new("b")]);
        s.recompute_status_from_upstream(&[SlotState::Ready, SlotState::Loading]);
        assert_eq!(s.state, SlotState::Loading);
    }

    #[test]
    fn derived_with_one_idle_upstream_is_loading() {
        let mut s = derived_slot(vec![RequirementKey::new("a")]);
        s.recompute_status_from_upstream(&[SlotState::Idle]);
        assert_eq!(s.state, SlotState::Loading);
    }

    #[test]
    fn derived_with_any_error_upstream_is_error() {
        let mut s = derived_slot(vec![
            RequirementKey::new("a"),
            RequirementKey::new("b"),
            RequirementKey::new("c"),
        ]);
        s.recompute_status_from_upstream(&[
            SlotState::Ready,
            SlotState::Loading,
            SlotState::Error,
        ]);
        assert_eq!(s.state, SlotState::Error);
    }

    // ── Projected status ─────────────────────────────────────────────

    fn projected_slot(upstream: Vec<RequirementKey>) -> Slot {
        Slot::new(
            RequirementKey::new("projected:totals:1"),
            SlotKind::Projected {
                projection_id: Arc::from("totals"),
                partition_repr: Arc::from("1"),
            },
            upstream,
        )
    }

    #[test]
    fn projected_aggregates_from_upstream_like_derived() {
        let mut s = projected_slot(vec![RequirementKey::new("a")]);
        s.recompute_status_from_upstream(&[SlotState::Loading]);
        assert_eq!(s.state, SlotState::Loading);
        s.recompute_status_from_upstream(&[SlotState::Ready]);
        assert_eq!(s.state, SlotState::Ready);
    }

    #[test]
    fn projection_error_pins_state_until_cleared() {
        let mut s = projected_slot(vec![RequirementKey::new("a")]);
        s.recompute_status_from_upstream(&[SlotState::Ready]);
        s.apply_projection_error("bad payload".into());
        assert_eq!(s.state, SlotState::Error);

        // Upstream flapping does NOT unpin — the failure belongs to the
        // key's own render.
        s.recompute_status_from_upstream(&[SlotState::Ready]);
        assert_eq!(s.state, SlotState::Error);

        assert!(s.clear_projection_error());
        s.recompute_status_from_upstream(&[SlotState::Ready]);
        assert_eq!(s.state, SlotState::Ready);
    }

    #[test]
    fn invalidate_unpins_projection_error() {
        let mut s = projected_slot(vec![RequirementKey::new("a")]);
        s.apply_projection_error("bad payload".into());
        s.invalidate();
        assert!(s.last_error.is_none());
        s.recompute_status_from_upstream(&[SlotState::Ready]);
        assert_eq!(s.state, SlotState::Ready);
    }

    #[test]
    fn clear_projection_error_without_error_is_false() {
        let mut s = projected_slot(Vec::new());
        assert!(!s.clear_projection_error());
    }

    #[test]
    fn identical_repeat_failure_reports_no_change() {
        let mut s = projected_slot(vec![RequirementKey::new("a")]);
        assert!(s.apply_projection_error("bad payload".into()));
        assert!(!s.apply_projection_error("bad payload".into()));
        // A different message IS a change — it surfaces to the UI.
        assert!(s.apply_projection_error("other".into()));
    }

    #[test]
    fn recompute_on_fetched_is_no_op() {
        let mut s = fetched_slot();
        s.start_fetch();
        assert!(s.apply_ready(0));
        let before = s.state;
        s.recompute_status_from_upstream(&[SlotState::Loading]);
        // Fetched ignores upstream aggregation entirely.
        assert_eq!(s.state, before);
    }
}
