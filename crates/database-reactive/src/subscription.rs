use fnv::{FnvHashMap, FnvHashSet};

use sql_engine::reactive::{SubscriptionHandle, SubscriptionId};

/// Callback fired when a mutation affects a subscription.
///
/// Arguments:
/// - `SubscriptionId`: the subscription's runtime identifier. Multiple callers
///   subscribing to the same SQL share one id — all their callbacks fire with
///   that id.
/// - `&[usize]`: indices of triggered reactive conditions (empty for
///   table-level notifies).
pub type Callback = Box<dyn Fn(SubscriptionId, &[usize])>;

pub(crate) struct Subscription {
    pub sql: String,
    /// Per-caller callbacks, keyed by the handle returned to the caller.
    /// Multiple `subscribe(sql, cb)` calls with equivalent SQL share one
    /// `Subscription`; each call gets its own entry in this map. Removing a
    /// handle removes only that caller's callback — the subscription is torn
    /// down from the registry only once this map becomes empty.
    pub callbacks: FnvHashMap<SubscriptionHandle, Callback>,
    /// Condition indices triggered by the most recent notification.
    /// Read by `execute_for_sub` / `execute_for_sql` so reactive(...) columns
    /// reflect which predicates fired. Consumed (cleared) on read.
    pub last_triggered: FnvHashSet<usize>,
}
