//! Storage-side facade for invoice-demo tables. Native only.
//!
//! Owns `AppCtx`. Rows + queries are declared in submodules with
//! `#[row]` / `#[query]`; `build.rs` runs `tables-codegen` to generate
//! `Params` structs, `impl Fetcher`, and `register_*` glue into
//! `$OUT_DIR/generated.rs` (included as `__generated` below).

mod activity_log;
mod contacts;
mod customers;
mod invoices;
mod payments;
mod positions;
mod products;
mod recurring_invoices;
mod recurring_positions;
mod sepa_mandates;

/// App-level storage context. Server boot constructs this once with a
/// connected pool.
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}

pub mod __generated {
    include!(concat!(env!("OUT_DIR"), "/generated.rs"));
}

pub use __generated::register_all;
pub use activity_log::ActivityLogEntry;
pub use contacts::Contact;
pub use customers::Customer;
pub use invoices::Invoice;
pub use payments::Payment;
pub use positions::Position;
pub use products::Product;
pub use recurring_invoices::RecurringInvoice;
pub use recurring_positions::RecurringPosition;
pub use sepa_mandates::SepaMandate;
