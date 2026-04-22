//! WHERE predicates over caller output — equality, comparison, AND, OR,
//! combined, NULL handling.

mod common;

use common::{check_plans, i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn where_eq_int_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = 3";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Carol")]);
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
    pre_filter: customer.id = 3
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_eq_string_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) WHERE customer.name = 'Alice'";
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
    pre_filter: customer.name = 'Alice'
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_neq_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id != 1";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Carol")]);
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
    pre_filter: customer.id != 1
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_gt_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount > 100";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![i(11)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::by_customer row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.amount > 100
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_lt_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount < 200";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![i(10)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::by_customer row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.amount < 200
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_lte_gte_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql_lte = "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount <= 100";
    let cols_lte = run(&mut db, sql_lte);
    assert_eq!(cols_lte[0], vec![i(10)]);
    check_plans(
        &db,
        sql_lte,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::by_customer row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.amount <= 100
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );

    let sql_gte = "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount >= 200";
    let cols_gte = run(&mut db, sql_gte);
    assert_eq!(cols_gte[0], vec![i(11)]);
    check_plans(
        &db,
        sql_gte,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::by_customer row=invoice args=[:__caller_0_arg_0]
    pre_filter: invoice.amount >= 200
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_and_two_predicates() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1) \
         WHERE customer.id > 1 AND customer.name = 'Carol'";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Carol")]);
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
    pre_filter: (customer.id > 1 AND customer.name = 'Carol')
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_or_two_predicates() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.id = 1 OR customer.id = 3";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0].len(), 2);
    assert!(cols[0].contains(&i(1)));
    assert!(cols[0].contains(&i(3)));
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
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_combined_and_or() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // (id=1 AND name='Alice') OR id=3 → Alice + Carol.
    let sql = "SELECT customer.name FROM customers.by_owner(1) \
         WHERE (customer.id = 1 AND customer.name = 'Alice') OR customer.id = 3";
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
    pre_filter: ((customer.id = 1 AND customer.name = 'Alice') OR customer.id = 3)
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn where_no_match_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = 999";
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
    pre_filter: customer.id = 999
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
