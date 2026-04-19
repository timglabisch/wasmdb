//! Client-side facade for invoice-demo tables.
//!
//! Declares the rows (`Customer`, …) and fetchers (`ByOwner`, …) the
//! server exposes. The storage-side crate reuses these types and wires
//! them to sqlx through `#[storage]`.

pub mod customers;
pub use customers::{ByOwner, Customer};
pub use tables_client::FetchError;

pub mod bindings;

use tables::Fetcher;

/// URL of the invoice-demo server's table-fetch endpoint.
pub const TABLE_FETCH_URL: &str = "/table-fetch";

/// Snapshot fetch against the invoice-demo server.
pub async fn fetch<F: Fetcher>(params: F::Params) -> Result<Vec<F::Row>, FetchError> {
    tables_client::fetch::<F>(TABLE_FETCH_URL, params).await
}
