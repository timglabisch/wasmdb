//! Three-layer identity model for reactive subscriptions.
//!
//! A subscription has three distinct identities, each answering a different
//! question. Do not conflate them.
//!
//! | Type                  | Scope            | Answers                                |
//! |-----------------------|------------------|----------------------------------------|
//! | [`SubscriptionKey`]   | content          | Are two subscribe calls equivalent?    |
//! | [`SubscriptionId`]    | runtime          | Which subscription fires for a dirty   |
//! |                       |                  | row? Which entry in the reverse index? |
//! | [`SubscriptionHandle`]| per-caller       | Which caller is asking to unsubscribe? |
//!
//! # Relations
//!
//! - `Key → Id` is 1:1 after dedup (same content → same runtime sub).
//! - `Handle → Id` is N:1 (many callers may share one runtime sub).
//! - `Id` is the only identity the reactive engine itself cares about;
//!   the other two are bookkeeping at adjacent layers.
//!
//! # Where each lives
//!
//! - `SubscriptionId` and `SubscriptionKey` are used inside
//!   [`crate::reactive::registry`] and by the dedup layer in `database-reactive`.
//! - `SubscriptionHandle` + `HandleRegistry` live at FFI / client boundaries
//!   where safe double-unsubscribe matters.

use std::collections::HashMap;

/// Runtime-shared subscription identity.
///
/// Assigned by the registry when a subscription is first created. Multiple
/// callers holding the same logical query (same [`SubscriptionKey`]) share
/// exactly one `SubscriptionId` — dedup happens one layer up.
///
/// This is the id the reactive engine writes into reverse-index buckets and
/// dirty-notification structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(pub u64);

/// Content-based identity used only to answer "is this subscribe request
/// equivalent to one we already have?".
///
/// Currently derived from the SQL text of the subscription. Two callers that
/// subscribe with the same SQL will resolve to the same `SubscriptionKey` and
/// therefore share one [`SubscriptionId`].
///
/// The concrete form (plain SQL string) is deliberate: it is collision-free
/// and the existing code already compares SQL strings for this purpose.
/// Should dedup ever need to see through whitespace/casing differences, swap
/// the inner representation for a normalized plan hash — callers do not care.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey(pub String);

impl SubscriptionKey {
    /// Compute the dedup key for a SQL subscription.
    pub fn from_sql(sql: &str) -> Self {
        SubscriptionKey(sql.to_string())
    }
}

/// Per-caller token returned from a subscribe call. Guarantees that a caller
/// can only drop its own reference — a bogus or already-released handle yields
/// a warning at the FFI layer instead of corrupting refcounts.
///
/// Multiple handles may map to the same [`SubscriptionId`] (N callers sharing
/// one deduped subscription).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionHandle(pub u64);

/// Maps per-caller [`SubscriptionHandle`]s to runtime [`SubscriptionId`]s.
///
/// Lives at the boundary between external callers and the reactive engine
/// (wasm-bindgen, future FFI). Issue a handle on subscribe, release it on
/// unsubscribe — [`Self::release`] returns `None` for unknown handles so the
/// caller can log a warning rather than mutate registry state.
pub struct HandleRegistry {
    next: u64,
    handles: HashMap<SubscriptionHandle, SubscriptionId>,
}

impl HandleRegistry {
    pub fn new() -> Self {
        Self { next: 0, handles: HashMap::new() }
    }

    /// Issue a fresh handle for `sub_id`. Monotone, never reuses ids.
    pub fn issue(&mut self, sub_id: SubscriptionId) -> SubscriptionHandle {
        let handle = SubscriptionHandle(self.next);
        self.next += 1;
        self.handles.insert(handle, sub_id);
        handle
    }

    /// Release `handle`. Returns the backing `SubscriptionId` on success,
    /// `None` if the handle was never issued or already released.
    pub fn release(&mut self, handle: SubscriptionHandle) -> Option<SubscriptionId> {
        self.handles.remove(&handle)
    }

    /// Resolve a handle without releasing it.
    pub fn get(&self, handle: SubscriptionHandle) -> Option<SubscriptionId> {
        self.handles.get(&handle).copied()
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }
}

impl Default for HandleRegistry {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_registry_issues_unique_handles_for_same_sub_id() {
        let mut r = HandleRegistry::new();
        let sub_id = SubscriptionId(7);
        let h1 = r.issue(sub_id);
        let h2 = r.issue(sub_id);
        assert_ne!(h1, h2);
        assert_eq!(r.get(h1), Some(sub_id));
        assert_eq!(r.get(h2), Some(sub_id));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn release_unknown_handle_returns_none() {
        let mut r = HandleRegistry::new();
        assert_eq!(r.release(SubscriptionHandle(9999)), None);
    }

    #[test]
    fn double_release_returns_none_second_time() {
        let mut r = HandleRegistry::new();
        let h = r.issue(SubscriptionId(1));
        assert_eq!(r.release(h), Some(SubscriptionId(1)));
        assert_eq!(r.release(h), None);
    }

    #[test]
    fn handles_for_same_sub_id_release_independently() {
        let mut r = HandleRegistry::new();
        let sub_id = SubscriptionId(42);
        let h1 = r.issue(sub_id);
        let h2 = r.issue(sub_id);
        assert_eq!(r.release(h1), Some(sub_id));
        assert_eq!(r.get(h2), Some(sub_id));
        assert_eq!(r.release(h2), Some(sub_id));
        assert!(r.is_empty());
    }

    #[test]
    fn subscription_key_from_sql_is_equal_for_identical_sql() {
        let a = SubscriptionKey::from_sql("SELECT * FROM users");
        let b = SubscriptionKey::from_sql("SELECT * FROM users");
        assert_eq!(a, b);
    }

    #[test]
    fn subscription_key_differs_on_different_sql() {
        let a = SubscriptionKey::from_sql("SELECT * FROM users");
        let b = SubscriptionKey::from_sql("SELECT * FROM orders");
        assert_ne!(a, b);
    }
}
