//! Storage-side facade for invoice-demo tables. Native only.
//!
//! Owns `AppCtx` and `register_all`. Each fetcher lives in its own
//! module; `#[storage]` generates a `register_{fn}` we call from here.

mod customers;

use tables_storage::Registry;

/// App-level storage context. Server boot constructs this once with a
/// connected pool.
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}

/// Call once at server boot with a ready `Registry`.
pub fn register_all(registry: &mut Registry<AppCtx>) {
    customers::register_by_owner(registry);
}
