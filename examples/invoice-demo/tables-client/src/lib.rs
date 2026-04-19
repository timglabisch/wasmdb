//! Client-side facade. Re-exports the shared tables so client code has
//! a single path to import from: `invoice_demo_tables_client::*`.

pub use invoice_demo_tables::customers;
pub use invoice_demo_tables::Customers;
