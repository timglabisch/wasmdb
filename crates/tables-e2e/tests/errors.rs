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

use common::{check_plans, run, run_err, setup_db};

#[test]
fn unknown_caller_id_at_plan_time() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.does_not_exist(1)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(msg.contains("UnknownRequirement"), "got {msg}");
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::does_not_exist(1) row=customers
=== ExecutionPlan ===
error: UnknownRequirement(\"customers::does_not_exist\")
=== ReactivePlan ===
error: UnknownRequirement(\"customers::does_not_exist\")",
    );
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

    let sql = "SELECT customer.name FROM customers.always_err(1)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(msg.contains("boom"), "error not propagated: {msg}");
    let _ = RequirementsResult::default; // keep the import live if future code uses it
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::always_err(1) row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::always_err row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
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

    let sql = "SELECT customer.name FROM customers.short_rows(1)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(
        msg.to_lowercase().contains("column")
            || msg.to_lowercase().contains("mismatch")
            || msg.contains("ColumnCount"),
        "expected column-count error, got {msg}",
    );
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::short_rows(1) row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::short_rows row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

// Sanity: the happy path still works in this module.
#[test]
fn happy_path_still_works_in_errors_suite() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(2)";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0].len(), 1);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner(2) row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

// ── UUID cross-type negatives ───────────────────────────────────────

#[test]
fn caller_uuid_arg_against_i64_param_rejects_at_plan_time() {
    use tables_e2e::AppCtx;
    let mut db = setup_db(AppCtx::with_default_fixtures());
    db.register_caller(Caller::new(
        "customers::probe_int",
        RequirementMeta {
            row_table: "customer".into(),
            params: vec![RequirementParamDef { name: "id".into(), data_type: DataType::I64 }],
        },
        Arc::new(|_args| {
            Box::pin(async move { Ok(vec![]) }) as FetcherFuture
        }),
    ));
    let sql = "SELECT customer.name FROM customers.probe_int(\
        UUID '00000000-0000-0000-0000-000000000001'\
    )";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(msg.contains("CallerArgTypeMismatch"), "got {msg}");
}

#[test]
fn caller_int_arg_against_uuid_param_rejects_at_plan_time() {
    use tables_e2e::AppCtx;
    let mut db = setup_db(AppCtx::with_default_fixtures());
    db.register_caller(Caller::new(
        "customers::probe_uuid",
        RequirementMeta {
            row_table: "customer".into(),
            params: vec![RequirementParamDef { name: "id".into(), data_type: DataType::Uuid }],
        },
        Arc::new(|_args| {
            Box::pin(async move { Ok(vec![]) }) as FetcherFuture
        }),
    ));
    let sql = "SELECT customer.name FROM customers.probe_uuid(42)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(msg.contains("CallerArgTypeMismatch"), "got {msg}");
}

#[test]
fn cross_type_str_eq_uuid_returns_no_rows() {
    use tables_e2e::AppCtx;
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // customer.name is STRING; comparing to a UUID literal must compile but
    // match nothing — pinning behavior for cross-type equality.
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customer \
         WHERE customer.name = UUID '00000000-0000-0000-0000-000000000001'"
    );
    assert!(cols[0].is_empty(), "cross-type comparison must not match");
}

#[test]
fn invalid_uuid_literal_rejected_at_parse_time() {
    use tables_e2e::AppCtx;
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let err = run_err(&mut db, "SELECT customer.name FROM customer \
        WHERE customer.id = UUID 'definitely-not-a-uuid'");
    let msg = format!("{err:?}");
    assert!(msg.contains("invalid UUID"), "got {msg}");
}
