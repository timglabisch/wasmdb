/// Slot index into a [`crate::DirtySet`].
///
/// A `DirtySlotId` names a **bit position in the dirty bitmap** (and a slot
/// in the overflow-ring). Unlike `sql_engine::reactive::SubscriptionId`
/// (sparse u64, never recycled), `DirtySlotId`s are meant to be allocated
/// densely from 0 and reused on unsubscribe so the bitmap stays bounded.
///
/// Note that this type does not *enforce* density — it is a plain u32. The
/// caller's allocator is responsible for keeping slots packed.
///
/// The mapping `SubscriptionId` ↔ `DirtySlotId` is the integration layer's
/// responsibility — this crate never sees `SubscriptionId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DirtySlotId(pub u32);
