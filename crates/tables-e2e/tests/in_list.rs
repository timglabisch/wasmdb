//! IN (list) and subquery interactions with callers.

mod common;

use common::{check_plans, i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn in_literal_int_list_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1) \
         WHERE customer.id IN (1, 3)";
    let cols = run(&mut db, sql);
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Carol")));
    assert_eq!(cols[0].len(), 2);
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
    pre_filter: customer.id IN (1, 3)
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn in_literal_string_list_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.name IN ('Alice')";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![i(1)]);
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
    pre_filter: customer.name IN ('Alice')
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn in_empty_list_yields_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.id IN (999)";
    let cols = run(&mut db, sql);
    assert_eq!(cols.len(), 1);
    assert!(cols[0].is_empty());
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
    pre_filter: customer.id IN (999)
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn in_subquery_caller_feeds_another_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // Outer: invoices for customer ids that belong to owner 1 per the caller.
    // Inner subquery pulls ids via `customers.by_owner(1)`.
    let sql = "SELECT invoice.id FROM invoices.min_amount(0) \
         WHERE invoice.customer_id IN (\
             SELECT customer.id FROM customers.by_owner(1)\
         ) \
         ORDER BY invoice.id";
    let cols = run(&mut db, sql);
    // Owner 1 → Alice(1), Carol(3) → their invoices: 10, 11 (Alice) + 13 (Carol).
    assert_eq!(cols[0], vec![i(10), i(11), i(13)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (2 requirements)
  [0] Caller invoices::min_amount(0) row=invoices
  [1] Caller customers::by_owner(1) row=customers
=== ExecutionPlan ===
Materialize step=0 kind=List
  Scan table=customer caller=customers::by_owner row=customer args=[:__caller_0_arg_0]
  Output [customer.id]
Select
  Scan table=invoice caller=invoices::min_amount row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.customer_id IN $mat0
  OrderBy [invoice.id ASC]
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
