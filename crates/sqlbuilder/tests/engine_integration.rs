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

mod from_row_tests {
    use sql_engine::storage::{CellValue, Uuid};
    use sqlbuilder::{FromRow, FromCell};

    #[test]
    fn tuple_maps_columns_positionally() {
        let row = vec![
            CellValue::Str("hi".into()),
            CellValue::I64(42),
            CellValue::Null,
        ];
        let (s, n, opt): (String, i64, Option<i64>) = FromRow::from_row(row).unwrap();
        assert_eq!(s, "hi");
        assert_eq!(n, 42);
        assert_eq!(opt, None);
    }

    #[test]
    fn tuple_arity_mismatch_errors() {
        let row = vec![CellValue::I64(1)];
        let r: Result<(i64, i64), _> = FromRow::from_row(row);
        assert!(r.is_err());
    }

    #[test]
    fn cell_type_mismatch_errors() {
        let r: Result<i64, _> = FromCell::from_cell(CellValue::Str("nope".into()));
        assert!(r.is_err());
    }

    #[test]
    fn null_to_non_optional_errors() {
        let r: Result<String, _> = FromCell::from_cell(CellValue::Null);
        assert!(r.is_err());
    }

    #[derive(FromRow)]
    struct Hdr {
        name: String,
        count: i64,
        owner: Option<Uuid>,
    }

    #[test]
    fn derive_struct_maps_in_field_order() {
        let row = vec![
            CellValue::Str("alice".into()),
            CellValue::I64(7),
            CellValue::Uuid([9; 16]),
        ];
        let h: Hdr = FromRow::from_row(row).unwrap();
        assert_eq!(h.name, "alice");
        assert_eq!(h.count, 7);
        assert_eq!(h.owner, Some(Uuid([9; 16])));
    }

    #[test]
    fn derive_struct_propagates_null_to_option() {
        let row = vec![
            CellValue::Str("bob".into()),
            CellValue::I64(0),
            CellValue::Null,
        ];
        let h: Hdr = FromRow::from_row(row).unwrap();
        assert_eq!(h.owner, None);
    }
}
