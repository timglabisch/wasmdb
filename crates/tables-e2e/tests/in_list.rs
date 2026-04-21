//! IN (list) and subquery interactions with callers.

mod common;

use common::{i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn in_literal_int_list_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) \
         WHERE customer.id IN (1, 3)",
    );
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Carol")));
    assert_eq!(cols[0].len(), 2);
}

#[test]
fn in_literal_string_list_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.name IN ('Alice')",
    );
    assert_eq!(cols[0], vec![i(1)]);
}

#[test]
fn in_empty_list_yields_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.id IN (999)",
    );
    assert_eq!(cols.len(), 1);
    assert!(cols[0].is_empty());
}

#[test]
fn in_subquery_caller_feeds_another_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // Outer: invoices for customer ids that belong to owner 1 per the caller.
    // Inner subquery pulls ids via `customers.by_owner(1)`.
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) \
         WHERE invoice.customer_id IN (\
             SELECT customer.id FROM customers.by_owner(1)\
         ) \
         ORDER BY invoice.id",
    );
    // Owner 1 → Alice(1), Carol(3) → their invoices: 10, 11 (Alice) + 13 (Carol).
    assert_eq!(cols[0], vec![i(10), i(11), i(13)]);
}
