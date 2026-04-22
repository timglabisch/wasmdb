//! SELECT projection through callers — column order, single-column,
//! multi-column, empty result shape.

mod common;

use common::{check_plans, i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn select_all_columns_via_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id, customer.name, customer.owner_id FROM customers.by_owner(1)";
    let cols = run(&mut db, sql);
    assert_eq!(cols.len(), 3);
    // Fixtures: (1, Alice, 1) and (3, Carol, 1)
    assert!(cols[0].contains(&i(1)) && cols[0].contains(&i(3)));
    assert!(cols[1].contains(&s("Alice")) && cols[1].contains(&s("Carol")));
    assert_eq!(cols[2], vec![i(1), i(1)]);
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
  Output [customer.id, customer.name, customer.owner_id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn select_single_column_via_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(2)";
    let cols = run(&mut db, sql);
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], vec![s("Bob")]);
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

#[test]
fn select_projection_reorder() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name, customer.id FROM customers.by_name('Bob')";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Bob")]);
    assert_eq!(cols[1], vec![i(2)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_name('Bob') row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_name row=customer args=[:__caller_0_arg_0]
  Output [customer.name, customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn select_from_caller_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(999)";
    let cols = run(&mut db, sql);
    assert_eq!(cols.len(), 1, "column shape preserved even on empty result");
    assert!(cols[0].is_empty());
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner(999) row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn select_string_pk_caller_returns_sku() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT product.sku, product.name FROM products.by_sku('widget')";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("widget")]);
    assert_eq!(cols[1], vec![s("Widget")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller products::by_sku('widget') row=products
=== ExecutionPlan ===
Select
  Scan table=product caller=products::by_sku row=product args=[:__caller_0_arg_0]
  Output [product.sku, product.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
