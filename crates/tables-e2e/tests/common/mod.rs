#![allow(dead_code)]

//! Shared harness for e2e tests: builds a `Database` with all generated
//! `DbTable` / `DbCaller` impls from the `tables-e2e` crate registered
//! against a given `AppCtx` fixture, and provides run/run_err helpers.

use std::sync::Arc;

use database::{Database, DbError};
use sql_engine::execute::Params;
use sql_engine::storage::CellValue;
use tables_e2e::{AppCtx, Customer, Invoice, Product};

/// Build a `Database` with all tables + callers registered.
pub fn setup_db(ctx: AppCtx) -> Database {
    let mut db = Database::new();
    register_all(&mut db, Arc::new(ctx)).expect("register_all");
    db
}

/// Hand-written index over every `DbTable` + `DbCaller` the test crate
/// produces. Must stay in lockstep with `src/*.rs` — adding a `#[row]` or
/// `#[query]` means adding a line here.
fn register_all(db: &mut Database, ctx: Arc<AppCtx>) -> Result<(), DbError> {
    // ── Tables ──────────────────────────────────────────────────────────
    db.register_table::<Customer>()?;
    db.register_table::<Product>()?;
    db.register_table::<Invoice>()?;

    // ── Callers (via codegen-emitted `impl DbCaller for #marker`) ───────
    use tables_e2e::__generated::customers as gen_customers;
    db.register_caller_of::<gen_customers::ByOwner>(ctx.clone());
    db.register_caller_of::<gen_customers::ByName>(ctx.clone());
    db.register_caller_of::<gen_customers::ByOwnerAndName>(ctx.clone());

    use tables_e2e::__generated::products as gen_products;
    db.register_caller_of::<gen_products::BySku>(ctx.clone());
    db.register_caller_of::<gen_products::CheaperThan>(ctx.clone());
    db.register_caller_of::<gen_products::WithOptionalPrice>(ctx.clone());

    use tables_e2e::__generated::invoices as gen_invoices;
    db.register_caller_of::<gen_invoices::ByCustomer>(ctx.clone());
    db.register_caller_of::<gen_invoices::WithNoteContaining>(ctx.clone());
    db.register_caller_of::<gen_invoices::WithOptionalNote>(ctx.clone());
    db.register_caller_of::<gen_invoices::MinAmount>(ctx);

    Ok(())
}

/// Execute a SQL statement through the async pipeline; panic on error.
pub fn run(db: &mut Database, sql: &str) -> Vec<Vec<CellValue>> {
    pollster::block_on(db.execute_async(sql)).expect("execute_async")
}

pub fn run_with_params(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Vec<Vec<CellValue>> {
    pollster::block_on(db.execute_with_params_async(sql, params)).expect("execute_async")
}

pub fn run_err(db: &mut Database, sql: &str) -> DbError {
    pollster::block_on(db.execute_async(sql)).expect_err("expected execute_async to fail")
}

// ── Cell constructors (less noise in assertions) ────────────────────────

pub fn i(v: i64) -> CellValue {
    CellValue::I64(v)
}
pub fn s(v: &str) -> CellValue {
    CellValue::Str(v.into())
}
