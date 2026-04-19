//! Storage-side facade for invoice-demo tables. Native only.
//!
//! `register_all` hooks every app-level table into the runtime
//! registry at server startup. `StorageTable` impls themselves live in
//! the shared `invoice-demo-tables` crate (orphan-rule).

pub use invoice_demo_tables::customers;
pub use invoice_demo_tables::Customers;

use tables_storage::Registry;

/// Call once at server boot.
pub fn register_all(registry: &mut Registry) {
    registry.register::<Customers>();
}
