//! ORDER BY over caller output — ASC/DESC, multi-key, combined WHERE.

mod common;

use common::{i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn order_by_asc_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) ORDER BY customer.name ASC",
    );
    assert_eq!(cols[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn order_by_desc_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) ORDER BY customer.name DESC",
    );
    assert_eq!(cols[0], vec![s("Carol"), s("Alice")]);
}

#[test]
fn order_by_int_asc_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) ORDER BY invoice.amount ASC",
    );
    // Amounts sorted: 50, 100, 200, 300 → ids: 12, 10, 11, 13.
    assert_eq!(cols[0], vec![i(12), i(10), i(11), i(13)]);
}

#[test]
fn order_by_int_desc_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) ORDER BY invoice.amount DESC",
    );
    assert_eq!(cols[0], vec![i(13), i(11), i(10), i(12)]);
}

#[test]
fn order_by_multi_key() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.customer_id, invoice.amount \
         FROM invoices.min_amount(0) \
         ORDER BY invoice.customer_id ASC, invoice.amount DESC",
    );
    // customer 1: (200, 100), customer 2: (50), customer 3: (300)
    assert_eq!(
        cols[0],
        vec![i(1), i(1), i(2), i(3)],
    );
    assert_eq!(
        cols[1],
        vec![i(200), i(100), i(50), i(300)],
    );
}

#[test]
fn order_by_with_where() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) \
         WHERE invoice.amount >= 100 \
         ORDER BY invoice.amount DESC",
    );
    // amounts ≥100 sorted desc: 300, 200, 100 → ids 13, 11, 10.
    assert_eq!(cols[0], vec![i(13), i(11), i(10)]);
}
