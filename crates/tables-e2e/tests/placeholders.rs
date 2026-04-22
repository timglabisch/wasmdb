//! Prepared-style user placeholders, both inside caller arg lists and
//! inside WHERE predicates / LIMIT.

mod common;

use std::collections::HashMap;

use common::{check_plans, i, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

#[test]
fn placeholder_in_caller_arg() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(2));
    let sql = "SELECT customer.name FROM customers.by_owner(:uid)";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![s("Bob")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_owner(...)` arg 0: unsupported literal Placeholder(\\\"uid\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:uid]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn placeholder_in_where_over_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("cid".into(), ParamValue::Int(3));
    let sql = "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = :cid";
    let cols = run_with_params(&mut db, sql, p);
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
    pre_filter: customer.id = :cid
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn placeholder_in_both_caller_and_where() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("oid".into(), ParamValue::Int(1));
    p.insert("cid".into(), ParamValue::Int(1));
    let sql = "SELECT customer.name FROM customers.by_owner(:oid) WHERE customer.id = :cid";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![s("Alice")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_owner(...)` arg 0: unsupported literal Placeholder(\\\"oid\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:oid]
    pre_filter: customer.id = :cid
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn placeholder_reused_same_query_twice_with_different_params() {
    let mut db = setup_db(AppCtx::with_default_fixtures());

    let sql = "SELECT customer.name FROM customers.by_owner(:uid) ORDER BY customer.name";
    let mut p1 = HashMap::new();
    p1.insert("uid".into(), ParamValue::Int(1));
    let cols1 = run_with_params(&mut db, sql, p1);
    assert_eq!(cols1[0], vec![s("Alice"), s("Carol")]);

    let mut p2 = HashMap::new();
    p2.insert("uid".into(), ParamValue::Int(2));
    let cols2 = run_with_params(&mut db, sql, p2);
    assert_eq!(cols2[0], vec![s("Bob")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_owner(...)` arg 0: unsupported literal Placeholder(\\\"uid\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:uid]
  OrderBy [customer.name ASC]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn placeholder_limit_with_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(1));
    p.insert("n".into(), ParamValue::Int(1));
    let sql = "SELECT customer.id FROM customers.by_owner(:uid) ORDER BY customer.id LIMIT :n";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![i(1)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_owner(...)` arg 0: unsupported literal Placeholder(\\\"uid\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner row=customer args=[:uid]
  OrderBy [customer.id ASC]
  Limit :n
  Output [customer.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn placeholder_in_list_over_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("ids".into(), ParamValue::IntList(vec![1, 3]));
    let sql = "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id IN (:ids) ORDER BY customer.id";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![s("Alice"), s("Carol")]);
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
    pre_filter: customer.id IN (:ids)
  OrderBy [customer.id ASC]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
