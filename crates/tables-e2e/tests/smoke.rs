//! Smoke test — proves the wiring works end-to-end: generated `DbTable`
//! schema + generated `DbCaller::call` + `Database::execute_async`.

mod common;

use common::{check_plans, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn select_name_from_by_owner_caller_returns_rows() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1)";
    let cols = run(&mut db, sql);

    assert_eq!(cols.len(), 1, "one projected column");
    let name_col = &cols[0];
    assert!(name_col.contains(&s("Alice")));
    assert!(name_col.contains(&s("Carol")));
    assert_eq!(name_col.len(), 2, "owner 1 has 2 customers");

    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner(1) row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
