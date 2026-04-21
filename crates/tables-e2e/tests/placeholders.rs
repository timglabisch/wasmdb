//! Prepared-style user placeholders, both inside caller arg lists and
//! inside WHERE predicates / LIMIT.

mod common;

use std::collections::HashMap;

use common::{i, run_with_params, s, setup_db};
use sql_engine::execute::ParamValue;
use tables_e2e::AppCtx;

#[test]
fn placeholder_in_caller_arg() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(2));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(:uid)",
        p,
    );
    assert_eq!(cols[0], vec![s("Bob")]);
}

#[test]
fn placeholder_in_where_over_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("cid".into(), ParamValue::Int(3));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id = :cid",
        p,
    );
    assert_eq!(cols[0], vec![s("Carol")]);
}

#[test]
fn placeholder_in_both_caller_and_where() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("oid".into(), ParamValue::Int(1));
    p.insert("cid".into(), ParamValue::Int(1));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(:oid) WHERE customer.id = :cid",
        p,
    );
    assert_eq!(cols[0], vec![s("Alice")]);
}

#[test]
fn placeholder_reused_same_query_twice_with_different_params() {
    let mut db = setup_db(AppCtx::with_default_fixtures());

    let mut p1 = HashMap::new();
    p1.insert("uid".into(), ParamValue::Int(1));
    let cols1 = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(:uid) ORDER BY customer.name",
        p1,
    );
    assert_eq!(cols1[0], vec![s("Alice"), s("Carol")]);

    let mut p2 = HashMap::new();
    p2.insert("uid".into(), ParamValue::Int(2));
    let cols2 = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(:uid) ORDER BY customer.name",
        p2,
    );
    assert_eq!(cols2[0], vec![s("Bob")]);
}

#[test]
fn placeholder_limit_with_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("uid".into(), ParamValue::Int(1));
    p.insert("n".into(), ParamValue::Int(1));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.id FROM customers.by_owner(:uid) ORDER BY customer.id LIMIT :n",
        p,
    );
    assert_eq!(cols[0], vec![i(1)]);
}

#[test]
fn placeholder_in_list_over_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let mut p = HashMap::new();
    p.insert("ids".into(), ParamValue::IntList(vec![1, 3]));
    let cols = run_with_params(
        &mut db,
        "SELECT customer.name FROM customers.by_owner(1) WHERE customer.id IN (:ids) ORDER BY customer.id",
        p,
    );
    assert_eq!(cols[0], vec![s("Alice"), s("Carol")]);
}
