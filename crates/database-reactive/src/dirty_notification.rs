use crate::SubscriptionId;

/// One drain item produced by [`crate::ReactiveDatabase::next_dirty`].
///
/// Represents a subscription that has become dirty since the last drain, plus
/// the condition indices accumulated across all notifies that marked it dirty
/// in this batch. Consumers dispatch these to whatever per-subscription work
/// they need (JS store refresh, reactive query re-run, etc.).
#[derive(Debug, Clone)]
pub struct DirtyNotification {
    pub sub_id: SubscriptionId,
    /// Triggered condition indices, accumulated over every `notify`-call since
    /// the last drain cycle. Empty for marks coming from `notify_all` (full
    /// invalidation without precise diff).
    pub triggered: Vec<usize>,
}
