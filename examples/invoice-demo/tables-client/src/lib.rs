//! Client-side facade for invoice-demo tables.
//!
//! Owns the `Table` markers + param/row types. The storage-side crate
//! reuses the param/row types and declares its own storage-side marker
//! with the same `TableId` (orphan-rule dance).

pub mod customers;
pub use customers::Customers;
pub use tables_client::FetchError;

pub mod bindings;

use tables::Table;

/// URL of the invoice-demo server's table-fetch endpoint.
pub const TABLE_FETCH_URL: &str = "/table-fetch";

/// Snapshot fetch against the invoice-demo server.
pub async fn fetch<T: Table>(params: T::Params) -> Result<Vec<T::Row>, FetchError> {
    tables_client::fetch::<T>(TABLE_FETCH_URL, params).await
}
