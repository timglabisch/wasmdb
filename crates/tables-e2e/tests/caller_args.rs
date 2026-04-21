//! Caller arg shapes at the Planner ↔ DbCaller boundary. Covers literal
//! ints, literal strings, NULL literals, user placeholders, multi-arg,
//! and the error paths for unknown/arity/type mismatches.

mod common;

use std::collections::HashMap;

use common::{run, run_err, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

// ── Literal arg shapes ─────────────────────────────────────────────────

#[test]
fn literal_int_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.name FROM customers.by_owner(2)");
    assert_eq!(cols[0], vec![s("Bob")]);
}

#[test]
fn literal_string_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.name FROM customers.by_name('Alice')");
    assert_eq!(cols[0], vec![s("Alice")]);
}

#[test]
fn multi_arg_literals() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner_and_name(1, 'Alice')",
    );
    assert_eq!(cols[0], vec![s("Alice")]);
}

// ── User placeholders ──────────────────────────────────────────────────

#[test]
fn user_placeholder_int_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(1));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(:uid)",
        p,
    );
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Carol")));
}

#[test]
fn user_placeholder_string_arg_runs() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("n".into(), ParamValue::Text("Bob".into()));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_name(:n)",
        p,
    );
    assert_eq!(cols[0], vec![s("Bob")]);
}

#[test]
fn multi_arg_mixed_literal_and_placeholder() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("who".into(), ParamValue::Text("Carol".into()));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner_and_name(1, :who)",
        p,
    );
    assert_eq!(cols[0], vec![s("Carol")]);
}

// ── Error paths ────────────────────────────────────────────────────────

#[test]
fn unknown_caller_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let err = run_err(&mut db, "SELECT customer.name FROM customers.no_such_fn(1)");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("UnknownRequirement") || msg.to_lowercase().contains("unknown"),
        "expected unknown-caller error, got {msg}",
    );
}

#[test]
fn arity_mismatch_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // by_owner expects 1 arg; we pass 2.
    let err = run_err(&mut db, "SELECT customer.name FROM customers.by_owner(1, 2)");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("CallerArgCountMismatch"),
        "expected arg-count error, got {msg}",
    );
}

#[test]
fn type_mismatch_errors() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // by_owner expects i64; we pass a string literal.
    let err = run_err(&mut db, "SELECT customer.name FROM customers.by_owner('nope')");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ArgTypeMismatch")
            || msg.to_lowercase().contains("type mismatch")
            || msg.to_lowercase().contains("expected"),
        "expected type-mismatch error, got {msg}",
    );
}

// ── Caller with optional arg ───────────────────────────────────────────

#[test]
fn option_i64_arg_with_int_literal() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT product.sku FROM products.with_optional_price(100)",
    );
    assert_eq!(cols[0], vec![s("gadget")]);
}

#[test]
fn option_string_arg_with_text_literal() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.with_optional_note('rush')",
    );
    // Only invoice 10 matches note='rush' exactly.
    assert_eq!(cols[0], vec![common::i(10)]);
}
