//! WHERE predicates over caller output — equality, comparison, AND, OR,
//! combined, NULL handling.

mod common;

use common::{i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn where_eq_int_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = 3",
    );
    assert_eq!(cols[0], vec![s("Carol")]);
}

#[test]
fn where_eq_string_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(1) WHERE customer.name = 'Alice'",
    );
    assert_eq!(cols[0], vec![i(1)]);
}

#[test]
fn where_neq_on_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id != 1",
    );
    assert_eq!(cols[0], vec![s("Carol")]);
}

#[test]
fn where_gt_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount > 100",
    );
    assert_eq!(cols[0], vec![i(11)]);
}

#[test]
fn where_lt_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount < 200",
    );
    assert_eq!(cols[0], vec![i(10)]);
}

#[test]
fn where_lte_gte_on_caller_output() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols_lte = run(
        &mut db,
        "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount <= 100",
    );
    assert_eq!(cols_lte[0], vec![i(10)]);

    let cols_gte = run(
        &mut db,
        "SELECT invoice.id FROM invoices.by_customer(1) WHERE invoice.amount >= 200",
    );
    assert_eq!(cols_gte[0], vec![i(11)]);
}

#[test]
fn where_and_two_predicates() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) \
         WHERE customer.id > 1 AND customer.name = 'Carol'",
    );
    assert_eq!(cols[0], vec![s("Carol")]);
}

#[test]
fn where_or_two_predicates() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(1) \
         WHERE customer.id = 1 OR customer.id = 3",
    );
    assert_eq!(cols[0].len(), 2);
    assert!(cols[0].contains(&i(1)));
    assert!(cols[0].contains(&i(3)));
}

#[test]
fn where_combined_and_or() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // (id=1 AND name='Alice') OR id=3 → Alice + Carol.
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) \
         WHERE (customer.id = 1 AND customer.name = 'Alice') OR customer.id = 3",
    );
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Carol")));
    assert_eq!(cols[0].len(), 2);
}

#[test]
fn where_no_match_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = 999",
    );
    assert_eq!(cols.len(), 1);
    assert!(cols[0].is_empty());
}
