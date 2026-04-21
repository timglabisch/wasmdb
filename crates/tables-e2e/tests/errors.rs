//! Error paths through the trait-path pipeline — plan-time rejections
//! and Phase 0 fetcher-error propagation.

mod common;

use std::sync::Arc;

use database::Database;
use sql_engine::execute::{FetcherFuture, RequirementsResult};
use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};
use sql_engine::schema::DataType;
use sql_engine::storage::CellValue;
use sql_engine::{Caller, DbTable};
use sql_parser::ast::Value;
use tables_e2e::{AppCtx, Customer};

use common::{run, run_err, setup_db};

#[test]
fn unknown_caller_id_at_plan_time() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let err = run_err(
        &mut db,
        "SELECT customer.name FROM customers.does_not_exist(1)",
    );
    let msg = format!("{err:?}");
    assert!(msg.contains("UnknownRequirement"), "got {msg}");
}

#[test]
fn duplicate_register_table_errors() {
    let mut db = Database::new();
    db.register_table::<Customer>().expect("first register");
    let err = db.register_table::<Customer>().unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("TableAlreadyExists") || msg.to_lowercase().contains("already"),
        "got {msg}",
    );
}

#[test]
fn caller_fetcher_that_returns_err_propagates() {
    // Build a caller that always errors to prove fetcher-error propagation.
    let mut db = Database::new();
    db.register_table::<Customer>().unwrap();

    let meta = RequirementMeta {
        row_table: <Customer as DbTable>::TABLE.into(),
        params: vec![RequirementParamDef {
            name: "owner_id".into(),
            data_type: DataType::I64,
        }],
    };
    let fetcher = Arc::new(|_args: Vec<Value>| -> FetcherFuture {
        Box::pin(async move { Err::<Vec<Vec<CellValue>>, String>("boom".into()) })
    });
    db.register_caller(Caller::new("customers::always_err", meta, fetcher));

    let err = run_err(
        &mut db,
        "SELECT customer.name FROM customers.always_err(1)",
    );
    let msg = format!("{err:?}");
    assert!(msg.contains("boom"), "error not propagated: {msg}");
    let _ = RequirementsResult::default; // keep the import live if future code uses it
}

#[test]
fn caller_fetcher_wrong_column_count_errors() {
    let mut db = Database::new();
    db.register_table::<Customer>().unwrap();

    let meta = RequirementMeta {
        row_table: <Customer as DbTable>::TABLE.into(),
        params: vec![RequirementParamDef {
            name: "owner_id".into(),
            data_type: DataType::I64,
        }],
    };
    // Customer schema has 3 columns; return 2-column rows to trigger a
    // column-count mismatch at Phase 0.
    let fetcher = Arc::new(|_args: Vec<Value>| -> FetcherFuture {
        Box::pin(async move {
            Ok(vec![vec![CellValue::I64(1), CellValue::Str("Alice".into())]])
        })
    });
    db.register_caller(Caller::new("customers::short_rows", meta, fetcher));

    let err = run_err(
        &mut db,
        "SELECT customer.name FROM customers.short_rows(1)",
    );
    let msg = format!("{err:?}");
    assert!(
        msg.to_lowercase().contains("column")
            || msg.to_lowercase().contains("mismatch")
            || msg.contains("ColumnCount"),
        "expected column-count error, got {msg}",
    );
}

// Sanity: the happy path still works in this module.
#[test]
fn happy_path_still_works_in_errors_suite() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.name FROM customers.by_owner(2)");
    assert_eq!(cols[0].len(), 1);
}
