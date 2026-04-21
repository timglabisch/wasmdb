//! SELECT projection through callers — column order, single-column,
//! multi-column, empty result shape.

mod common;

use common::{i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn select_all_columns_via_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.id, customer.name, customer.owner_id FROM customers.by_owner(1)",
    );
    assert_eq!(cols.len(), 3);
    // Fixtures: (1, Alice, 1) and (3, Carol, 1)
    assert!(cols[0].contains(&i(1)) && cols[0].contains(&i(3)));
    assert!(cols[1].contains(&s("Alice")) && cols[1].contains(&s("Carol")));
    assert_eq!(cols[2], vec![i(1), i(1)]);
}

#[test]
fn select_single_column_via_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.name FROM customers.by_owner(2)");
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], vec![s("Bob")]);
}

#[test]
fn select_projection_reorder() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name, customer.id FROM customers.by_name('Bob')",
    );
    assert_eq!(cols[0], vec![s("Bob")]);
    assert_eq!(cols[1], vec![i(2)]);
}

#[test]
fn select_from_caller_empty_result() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(999)",
    );
    assert_eq!(cols.len(), 1, "column shape preserved even on empty result");
    assert!(cols[0].is_empty());
}

#[test]
fn select_string_pk_caller_returns_sku() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT product.sku, product.name FROM products.by_sku('widget')",
    );
    assert_eq!(cols[0], vec![s("widget")]);
    assert_eq!(cols[1], vec![s("Widget")]);
}
