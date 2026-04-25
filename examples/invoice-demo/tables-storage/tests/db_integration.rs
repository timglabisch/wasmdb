//! Verify `tables-codegen` server-mode emits working `DbTable` +
//! `DbCaller` impls for the invoice-demo rows/queries.
//!
//! Scope: type-level + metadata correctness. We don't invoke
//! `DbCaller::call` because that would require a live MySQL pool;
//! the body of `call` is exercised end-to-end by
//! `crates/database/tests/traits_e2e.rs` against a hand-written impl.

use invoice_demo_tables_storage::__generated::customers::All;
use invoice_demo_tables_storage::Customer;
use sql_engine::schema::DataType;
use sql_engine::{DbCaller, DbTable};

#[test]
fn customer_dbtable_schema_matches_row() {
    assert_eq!(Customer::TABLE, "customers");

    let schema = Customer::schema();
    assert_eq!(schema.name, "customers");
    assert_eq!(schema.primary_key, vec![0]);
    assert_eq!(schema.columns.len(), 20);

    let first: Vec<(&str, DataType, bool)> = schema
        .columns
        .iter()
        .take(3)
        .map(|c| (c.name.as_str(), c.data_type, c.nullable))
        .collect();
    assert_eq!(
        first,
        vec![
            ("id", DataType::Uuid, false),
            ("name", DataType::String, false),
            ("email", DataType::String, false),
        ],
    );
}

#[test]
fn all_dbcaller_meta_points_at_customers() {
    assert_eq!(<All as DbCaller>::ID, "customers::all");

    let meta = <All as DbCaller>::meta();
    assert_eq!(meta.row_table, "customers");
    assert!(meta.params.is_empty());
}
