//! LIMIT over caller output — bare LIMIT, with ORDER BY, with placeholder.

mod common;

use std::collections::HashMap;

use common::{check_plans, i, run, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

#[test]
fn limit_basic() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) LIMIT 1";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0].len(), 1);
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
  Limit 1
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn limit_zero() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) LIMIT 0";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0].len(), 0);
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
  Limit 0
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn limit_larger_than_rows() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) LIMIT 99";
    let cols = run(&mut db, sql);
    // Owner 1 has Alice + Carol → 2 rows even though LIMIT says 99.
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
  Limit 99
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn limit_with_order_by() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT invoice.id FROM invoices.min_amount(0) \
         ORDER BY invoice.amount DESC LIMIT 2";
    let cols = run(&mut db, sql);
    // Top two by amount desc: 300 (id 13), 200 (id 11).
    assert_eq!(cols[0], vec![i(13), i(11)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::min_amount(0) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::min_amount row=invoice args=[:__caller_0_arg_0]
  OrderBy [invoice.amount DESC]
  Limit 2
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn limit_with_where_and_order_by() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT invoice.id FROM invoices.min_amount(0) \
         WHERE invoice.customer_id = 1 \
         ORDER BY invoice.amount ASC LIMIT 1";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![i(10)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::min_amount(0) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::min_amount row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.customer_id = 1
  OrderBy [invoice.amount ASC]
  Limit 1
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn limit_placeholder_via_user_param() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("n".into(), ParamValue::Int(1));
    let sql = "SELECT customer.id FROM customers.by_owner(1) LIMIT :n";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0].len(), 1);
    // Underlying ordering is fixture order; Alice (id=1) is first.
    assert_eq!(cols[0], vec![i(1)]);
    let _ = s; // keep helper available
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
  Limit :n
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
