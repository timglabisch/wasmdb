//! Storage-side facade for invoice-demo tables. Native only.
//!
//! Owns `AppCtx`, the storage-side `StorageTable` impls, and
//! `register_all`. Param/row types are reused from
//! `invoice-demo-tables-client`; this crate's markers are local (so the
//! orphan rule permits the `StorageTable` impls).

pub mod customers;
pub use customers::Customers;

use tables_storage::Registry;

/// App-level storage context. Server boot constructs this once with a
/// connected pool.
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}

/// Call once at server boot with a ready `Registry`.
pub fn register_all(registry: &mut Registry<AppCtx>) {
    registry.register::<Customers>();
}
