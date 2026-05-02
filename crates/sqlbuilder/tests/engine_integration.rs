//! Tests for the optional `sql-engine` feature impls (Uuid binding).

#![cfg(feature = "sql-engine")]

use sql_engine::storage::Uuid;
use sqlbuilder::{sql, Value};

#[test]
fn uuid_binds_as_uuid_value() {
    let id = Uuid([7; 16]);
    let r = sql!("DELETE FROM t WHERE id = {id}").render().unwrap();
    assert_eq!(r.params.len(), 1);
    match &r.params[0].1 {
        Value::Uuid(b) => assert_eq!(*b, [7; 16]),
        other => panic!("expected Uuid, got {other:?}"),
    }
}

#[test]
fn borrowed_uuid_via_destructure_works() {
    // Replicates the `&self` destructure case from real Command impls.
    struct S {
        id: Uuid,
    }
    let s = S { id: Uuid([3; 16]) };
    let r = &s;
    let S { id } = r; // id: &Uuid
    let rendered = sql!("UPDATE t SET v = 1 WHERE id = {id}").render().unwrap();
    match &rendered.params[0].1 {
        Value::Uuid(b) => assert_eq!(*b, [3; 16]),
        other => panic!("expected Uuid, got {other:?}"),
    }
}

#[test]
fn option_uuid_some_and_none() {
    let some: Option<Uuid> = Some(Uuid([1; 16]));
    let none: Option<Uuid> = None;
    let r1 = sql!("UPDATE t SET v = {v}", v = some).render().unwrap();
    let r2 = sql!("UPDATE t SET v = {v}", v = none).render().unwrap();
    assert!(matches!(r1.params[0].1, Value::Uuid(_)));
    assert!(matches!(r2.params[0].1, Value::Null));
}
