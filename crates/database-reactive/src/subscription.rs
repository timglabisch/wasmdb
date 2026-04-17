use fnv::FnvHashSet;

use sql_engine::reactive::registry::SubId;

/// Callback fired when a mutation affects a subscription.
///
/// Arguments:
/// - `SubId`: the subscription's identifier
/// - `&[usize]`: indices of triggered reactive conditions (empty for table-level notifies)
pub type Callback = Box<dyn Fn(SubId, &[usize])>;

pub(crate) struct Subscription {
    pub sql: String,
    pub callback: Callback,
    /// Condition indices triggered by the most recent notification.
    /// Read by `execute_for_sub` / `execute_for_sql` so reactive(...) columns
    /// reflect which predicates fired. Consumed (cleared) on read.
    pub last_triggered: FnvHashSet<usize>,
}
