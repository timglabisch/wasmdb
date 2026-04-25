//! Covers all four `FieldKind` variants through the generated `DbTable`
//! impls — `i64`, `String`, `Option<i64>`, `Option<String>` — both at
//! the schema level and across the `into_cells` round-trip.

mod common;

use sql_engine::schema::DataType;
use sql_engine::storage::CellValue;
use sql_engine::DbTable;
use tables_e2e::{contact_uuid, Contact, Customer, Invoice, Product};

#[test]
fn customer_schema_has_i64_pk_and_string_name() {
    assert_eq!(Customer::TABLE, "customer");
    let schema = Customer::schema();
    assert_eq!(schema.name, "customer");
    assert_eq!(schema.primary_key, vec![0]);
    let cols: Vec<(&str, DataType, bool)> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type, c.nullable))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", DataType::I64, false),
            ("name", DataType::String, false),
            ("owner_id", DataType::I64, false),
        ],
    );
}

#[test]
fn product_schema_has_string_pk_and_optional_i64() {
    assert_eq!(Product::TABLE, "product");
    let schema = Product::schema();
    assert_eq!(schema.primary_key, vec![0]);
    let cols: Vec<(&str, DataType, bool)> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type, c.nullable))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("sku", DataType::String, false),
            ("name", DataType::String, false),
            ("price", DataType::I64, true),
        ],
    );
}

#[test]
fn order_schema_has_optional_string() {
    assert_eq!(Invoice::TABLE, "invoice");
    let schema = Invoice::schema();
    assert_eq!(schema.primary_key, vec![0]);
    let cols: Vec<(&str, DataType, bool)> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type, c.nullable))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", DataType::I64, false),
            ("customer_id", DataType::I64, false),
            ("amount", DataType::I64, false),
            ("note", DataType::String, true),
        ],
    );
}

#[test]
fn customer_into_cells_preserves_order() {
    let c = Customer { id: 42, name: "Alice".into(), owner_id: 7 };
    let cells = c.into_cells();
    assert_eq!(
        cells,
        vec![
            CellValue::I64(42),
            CellValue::Str("Alice".into()),
            CellValue::I64(7),
        ],
    );
}

#[test]
fn product_into_cells_preserves_some_price() {
    let p = Product { sku: "x".into(), name: "X".into(), price: Some(99) };
    let cells = p.into_cells();
    assert_eq!(
        cells,
        vec![
            CellValue::Str("x".into()),
            CellValue::Str("X".into()),
            CellValue::I64(99),
        ],
    );
}

#[test]
fn product_into_cells_maps_none_price_to_null() {
    let p = Product { sku: "x".into(), name: "X".into(), price: None };
    let cells = p.into_cells();
    assert_eq!(cells[2], CellValue::Null);
}

#[test]
fn order_into_cells_maps_some_note() {
    let o = Invoice { id: 1, customer_id: 2, amount: 3, note: Some("hi".into()) };
    let cells = o.into_cells();
    assert_eq!(cells[3], CellValue::Str("hi".into()));
}

#[test]
fn order_into_cells_maps_none_note_to_null() {
    let o = Invoice { id: 1, customer_id: 2, amount: 3, note: None };
    let cells = o.into_cells();
    assert_eq!(cells[3], CellValue::Null);
}

// ── UUID-typed `#[row]` ──────────────────────────────────────────────────

#[test]
fn contact_schema_has_uuid_pk_and_optional_uuid() {
    assert_eq!(Contact::TABLE, "contact");
    let schema = Contact::schema();
    assert_eq!(schema.primary_key, vec![0]);
    let cols: Vec<(&str, DataType, bool)> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type, c.nullable))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", DataType::Uuid, false),
            ("name", DataType::String, false),
            ("external_id", DataType::Uuid, true),
        ],
    );
}

#[test]
fn contact_into_cells_unwraps_uuid_to_bytes() {
    let id = contact_uuid(7);
    let ext = contact_uuid(70);
    let c = Contact {
        id,
        name: "Alice".into(),
        external_id: Some(ext),
    };
    let cells = c.into_cells();
    assert_eq!(cells, vec![
        CellValue::Uuid(id.0),
        CellValue::Str("Alice".into()),
        CellValue::Uuid(ext.0),
    ]);
}

#[test]
fn contact_into_cells_maps_none_external_to_null() {
    let c = Contact {
        id: contact_uuid(1),
        name: "Alice".into(),
        external_id: None,
    };
    let cells = c.into_cells();
    assert_eq!(cells[2], CellValue::Null);
}

#[test]
fn contact_uuid_newtype_round_trips_via_cells() {
    // Build cells, push through Table, read back — preserves byte-identity.
    let bytes = contact_uuid(0xab);
    let cells = vec![
        CellValue::Uuid(bytes.0),
        CellValue::Str("X".into()),
        CellValue::Null,
    ];
    let mut t = sql_engine::storage::Table::new(Contact::schema());
    t.insert(&cells).unwrap();
    assert_eq!(t.get(0, 0), CellValue::Uuid(bytes.0));
}

#[test]
fn contact_uuid_pk_uniqueness_via_upsert() {
    let id = contact_uuid(1);
    let mut t = sql_engine::storage::Table::new(Contact::schema());
    t.upsert_by_pk(&[CellValue::Uuid(id.0), CellValue::Str("A".into()), CellValue::Null]).unwrap();
    t.upsert_by_pk(&[CellValue::Uuid(id.0), CellValue::Str("B".into()), CellValue::Null]).unwrap();
    assert_eq!(t.len(), 1);
    let live = t.row_ids().next().unwrap();
    assert_eq!(t.get(live, 1), CellValue::Str("B".into()));
}

