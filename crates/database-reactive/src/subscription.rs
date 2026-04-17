use fnv::FnvHashSet;
use sql_engine::reactive::SubscriptionKey;

pub(crate) struct Subscription {
    pub sql: String,
    /// Dedup key, computed once on subscribe and kept so teardown doesn't
    /// have to recompute it from `sql`. Also the right level of indirection
    /// if the key representation ever diverges from SQL (see
    /// [`SubscriptionKey`] docs).
    pub key: SubscriptionKey,
    /// Triggered condition indices accumulated since the last consumption
    /// (either via `next_dirty` drain or via a reactive query helper).
    /// Multiple `notify` calls between consumptions merge into this set.
    pub pending_triggered: FnvHashSet<usize>,
    /// Handle refcount — number of outstanding caller handles pointing at
    /// this subscription. Incremented on every `subscribe`, decremented on
    /// `unsubscribe`; the registry entry is torn down when it hits zero.
    pub refcount: u32,
}
