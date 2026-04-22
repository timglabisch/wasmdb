//! Caller arg shapes at the Planner ↔ DbCaller boundary. Covers literal
//! ints, literal strings, NULL literals, user placeholders, multi-arg,
//! and the error paths for unknown/arity/type mismatches.

mod common;

use std::collections::HashMap;

use common::{check_plans, run, run_err, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

// ── Literal arg shapes ─────────────────────────────────────────────────

#[test]
fn literal_int_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner(2)";
    let cols = run(&mut db, sql);
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
fn literal_string_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_name('Alice')";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Alice")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_name('Alice') row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_name row=customer args=[:__caller_0_arg_0]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn multi_arg_literals() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.by_owner_and_name(1, 'Alice')";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Alice")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner_and_name(1, 'Alice') row=customers
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner_and_name row=customer args=[:__caller_0_arg_0, :__caller_0_arg_1]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

// ── User placeholders ──────────────────────────────────────────────────

#[test]
fn user_placeholder_int_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(1));
    let sql = "SELECT customer.name FROM customers.by_owner(:uid)";
    let cols = run_with_params(&mut db, sql, p);
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Carol")));
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
fn user_placeholder_string_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("n".into(), ParamValue::Text("Bob".into()));
    let sql = "SELECT customer.name FROM customers.by_name(:n)";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![s("Bob")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_name(...)` arg 0: unsupported literal Placeholder(\\\"n\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_name row=customer args=[:n]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn multi_arg_mixed_literal_and_placeholder() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("who".into(), ParamValue::Text("Carol".into()));
    let sql = "SELECT customer.name FROM customers.by_owner_and_name(1, :who)";
    let cols = run_with_params(&mut db, sql, p);
    assert_eq!(cols[0], vec![s("Carol")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
error: UnsupportedExpr(\"caller `customers.by_owner_and_name(...)` arg 1: unsupported literal Placeholder(\\\"who\\\")\")
=== ExecutionPlan ===
Select
  Scan table=customer caller=customers::by_owner_and_name row=customer args=[:__caller_0_arg_0, :who]
  Output [customer.name]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

// ── Error paths ────────────────────────────────────────────────────────

#[test]
fn unknown_caller_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT customer.name FROM customers.no_such_fn(1)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(
        msg.contains("UnknownRequirement") || msg.to_lowercase().contains("unknown"),
        "expected unknown-caller error, got {msg}",
    );
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::no_such_fn(1) row=customers
=== ExecutionPlan ===
error: UnknownRequirement(\"customers::no_such_fn\")
=== ReactivePlan ===
error: UnknownRequirement(\"customers::no_such_fn\")",
    );
}

#[test]
fn arity_mismatch_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // by_owner expects 1 arg; we pass 2.
    let sql = "SELECT customer.name FROM customers.by_owner(1, 2)";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(
        msg.contains("CallerArgCountMismatch"),
        "expected arg-count error, got {msg}",
    );
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner(1, 2) row=customers
=== ExecutionPlan ===
error: CallerArgCountMismatch { id: \"customers::by_owner\", expected: 1, got: 2 }
=== ReactivePlan ===
error: CallerArgCountMismatch { id: \"customers::by_owner\", expected: 1, got: 2 }",
    );
}

#[test]
fn type_mismatch_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // by_owner expects i64; we pass a string literal.
    let sql = "SELECT customer.name FROM customers.by_owner('nope')";
    let err = run_err(&mut db, sql);
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ArgTypeMismatch")
            || msg.to_lowercase().contains("type mismatch")
            || msg.to_lowercase().contains("expected"),
        "expected type-mismatch error, got {msg}",
    );
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller customers::by_owner('nope') row=customers
=== ExecutionPlan ===
error: CallerArgTypeMismatch { id: \"customers::by_owner\", arg_idx: 0, expected: \"I64\", got: \"String\" }
=== ReactivePlan ===
error: CallerArgTypeMismatch { id: \"customers::by_owner\", arg_idx: 0, expected: \"I64\", got: \"String\" }",
    );
}

// ── Caller with optional arg ───────────────────────────────────────────

#[test]
fn option_i64_arg_with_int_literal() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT product.sku FROM products.with_optional_price(100)";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("gadget")]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller products::with_optional_price(100) row=products
=== ExecutionPlan ===
Select
  Scan table=product caller=products::with_optional_price row=product args=[:__caller_0_arg_0]
  Output [product.sku]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn option_string_arg_with_text_literal() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let sql = "SELECT invoice.id FROM invoices.with_optional_note('rush')";
    let cols = run(&mut db, sql);
    // Only invoice 10 matches note='rush' exactly.
    assert_eq!(cols[0], vec![common::i(10)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::with_optional_note('rush') row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::with_optional_note row=invoice args=[:__caller_0_arg_0]
  Output [invoice.id]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
