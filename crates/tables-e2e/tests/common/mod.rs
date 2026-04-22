#![allow(dead_code)]

//! Shared harness for e2e tests: builds a `Database` with all generated
//! `DbTable` / `DbCaller` impls from the `tables-e2e` crate registered
//! against a given `AppCtx` fixture, and provides run/run_err helpers.

use std::sync::Arc;

use database::{Database, DbError};
use sql_engine::execute::Params;
use sql_engine::planner;
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

// ── Plan snapshot helpers ───────────────────────────────────────────────
//
// `plans()` parses `sql` and prints RequirementPlan + ExecutionPlan +
// ReactivePlan in a stable, snapshot-friendly form against the live
// database state (table schemas + registered callers). Errors are
// rendered inline so snapshots also cover plan-time rejections.
//
// `check_plans()` asserts equality with a hand-written expected string
// and prints a full actual/expected diff on mismatch.

pub fn plans(db: &Database, sql: &str) -> String {
    let ast = match sql_parser::parser::parse(sql) {
        Ok(a) => a,
        Err(e) => return format!("=== parse error ===\n{e:?}\n"),
    };
    let schemas = db.table_schemas();
    let reqs = db.requirements();

    let mut out = String::new();

    out.push_str("=== RequirementPlan ===\n");
    match planner::requirement::plan_requirements(&ast) {
        Ok(p) => out.push_str(&p.pretty_print()),
        Err(e) => out.push_str(&format!("error: {e:?}\n")),
    }

    out.push_str("=== ExecutionPlan ===\n");
    match planner::sql::plan(&ast, &schemas, reqs) {
        Ok(p) => out.push_str(&p.pretty_print()),
        Err(e) => out.push_str(&format!("error: {e:?}\n")),
    }

    out.push_str("=== ReactivePlan ===\n");
    match planner::reactive::plan_reactive(&ast, &schemas, reqs) {
        Ok(p) => out.push_str(&p.pretty_print()),
        Err(e) => out.push_str(&format!("error: {e:?}\n")),
    }

    out
}

pub fn check_plans(db: &Database, sql: &str, expected: &str) {
    let actual = plans(db, sql);
    let actual_trim = actual.trim_end_matches('\n');

    // Generator mode: when `GEN_SNAPS=1` is set, append the actual snapshot
    // to `/tmp/tables_e2e_snaps.txt` with a begin/end marker and return
    // without asserting. Used once during authoring to collect all 74
    // expected strings; leave asserts live in normal test runs.
    if std::env::var("GEN_SNAPS").is_ok() {
        use std::io::Write;
        let path = "/tmp/tables_e2e_snaps.txt";
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open snaps file");
        writeln!(f, "<<<BEGIN {sql}>>>\n{actual_trim}\n<<<END>>>").unwrap();
        return;
    }

    let expected_trim = expected.trim_matches('\n');
    if actual_trim != expected_trim {
        panic!(
            "\n--- plan snapshot mismatch for SQL ---\n{sql}\n\n--- ACTUAL ---\n{actual_trim}\n\n--- EXPECTED ---\n{expected_trim}\n",
        );
    }
}

// ── Cell constructors (less noise in assertions) ────────────────────────

pub fn i(v: i64) -> CellValue {
    CellValue::I64(v)
}
pub fn s(v: &str) -> CellValue {
    CellValue::Str(v.into())
}
