//! Storage-side facade for invoice-demo tables. Native only.
//!
//! Owns `AppCtx`. Rows + queries are declared in submodules with
//! `#[row]` / `#[query]`; `build.rs` runs `tables-codegen` to generate
//! `Params` structs, `impl Fetcher`, and `register_*` glue into
//! `$OUT_DIR/generated.rs` (included as `__generated` below).

mod customers;

/// App-level storage context. Server boot constructs this once with a
/// connected pool.
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}

pub mod __generated {
    include!(concat!(env!("OUT_DIR"), "/generated.rs"));
}

pub use __generated::register_all;
pub use customers::Customer;
