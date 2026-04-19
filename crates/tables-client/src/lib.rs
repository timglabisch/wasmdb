//! Client-side access to parameterized tables.
//!
//! The app calls `SomeTable::subscribe(params)` and gets back a `Live`
//! handle. While the handle is alive the client stays subscribed;
//! dropping the handle unsubscribes.

use std::marker::PhantomData;
use tables::Table;

/// RAII subscription handle. One per `subscribe()` call. Dropping it
/// decrements the refcount on the underlying `(TableId, ParamsHash)`
/// instance and unsubscribes when the last handle goes away.
pub struct Live<T: Table> {
    _marker: PhantomData<T>,
    // real fields later: sub_id, registry handle, current-rows read view.
}

impl<T: Table> Live<T> {
    /// Snapshot of the current rows.
    pub fn rows(&self) -> Vec<T::Row> {
        Vec::new()
    }
}

impl<T: Table> Drop for Live<T> {
    fn drop(&mut self) {
        // unsubscribe via registry
    }
}

/// Extension trait — lets every `Table` be called with
/// `MyTable::subscribe(params)` directly, no turbofish.
/// Blanket-impl'd for every `T: Table`, nothing for downstream types to
/// implement.
pub trait TableExt: Table {
    fn subscribe(params: Self::Params) -> Live<Self> where Self: Sized {
        let _ = params;
        Live { _marker: PhantomData }
    }
}

impl<T: Table> TableExt for T {}
