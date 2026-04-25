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

/// Hardcoded tenant for the demo. Mirrors `commands::helpers::DEMO_TENANT_ID`
/// — kept local to avoid pulling the commands crate into the storage facade.
/// All `SELECT` queries below bind this to the `tenant_id` column TiDB requires.
pub const DEMO_TENANT_ID: i64 = 0;

/// App-level storage context. Server boot constructs this once with a
/// connected pool.
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}

/// Convert a sqlx-fetched `BINARY(16)` column into a `Uuid`. The MySQL
/// driver hands us a `Vec<u8>`; failure to produce 16 bytes means the
/// column is malformed (truncated or wrong type), which we surface as a
/// row-decode error so the calling fetcher can bubble it up.
pub(crate) fn try_uuid(
    row: &sqlx::mysql::MySqlRow,
    col: &str,
) -> Result<sql_engine::storage::Uuid, sqlx::Error> {
    use sqlx::Row;
    let bytes: Vec<u8> = row.try_get(col)?;
    let arr: [u8; 16] = bytes.try_into().map_err(|v: Vec<u8>| {
        sqlx::Error::Decode(
            format!("column {col}: expected 16 bytes, got {}", v.len()).into(),
        )
    })?;
    Ok(sql_engine::storage::Uuid(arr))
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
