//! LIMIT over caller output — bare LIMIT, with ORDER BY, with placeholder.

mod common;

use std::collections::HashMap;

use common::{i, run, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

#[test]
fn limit_basic() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.id FROM customers.by_owner(1) LIMIT 1");
    assert_eq!(cols[0].len(), 1);
}

#[test]
fn limit_zero() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.id FROM customers.by_owner(1) LIMIT 0");
    assert_eq!(cols[0].len(), 0);
}

#[test]
fn limit_larger_than_rows() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.id FROM customers.by_owner(1) LIMIT 99");
    // Owner 1 has Alice + Carol → 2 rows even though LIMIT says 99.
    assert_eq!(cols[0].len(), 2);
}

#[test]
fn limit_with_order_by() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) \
         ORDER BY invoice.amount DESC LIMIT 2",
    );
    // Top two by amount desc: 300 (id 13), 200 (id 11).
    assert_eq!(cols[0], vec![i(13), i(11)]);
}

#[test]
fn limit_with_where_and_order_by() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.min_amount(0) \
         WHERE invoice.customer_id = 1 \
         ORDER BY invoice.amount ASC LIMIT 1",
    );
    assert_eq!(cols[0], vec![i(10)]);
}

#[test]
fn limit_placeholder_via_user_param() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("n".into(), ParamValue::Int(1));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(1) LIMIT :n",
        p,
    );
    assert_eq!(cols[0].len(), 1);
    // Underlying ordering is fixture order; Alice (id=1) is first.
    assert_eq!(cols[0], vec![i(1)]);
    let _ = s; // keep helper available
}
