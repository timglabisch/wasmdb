//! Verify `tables-codegen` server-mode emits working `DbTable` +
//! `DbCaller` impls for the invoice-demo rows/queries.
//!
//! Scope: type-level + metadata correctness. We don't invoke
//! `DbCaller::call` because that would require a live MySQL pool;
//! the body of `call` is exercised end-to-end by
//! `crates/database/tests/traits_e2e.rs` against a hand-written impl.

use invoice_demo_tables_storage::__generated::customers::ByOwner;
use invoice_demo_tables_storage::Customer;
use sql_engine::schema::DataType;
use sql_engine::{DbCaller, DbTable};

#[test]
fn customer_dbtable_schema_matches_row() {
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
        ],
    );
}

#[test]
fn by_owner_dbcaller_meta_points_at_customer() {
    assert_eq!(<ByOwner as DbCaller>::ID, "customers::by_owner");

    let meta = <ByOwner as DbCaller>::meta();
    assert_eq!(meta.row_table, "customer");
    assert_eq!(meta.params.len(), 1);
    assert_eq!(meta.params[0].name, "owner_id");
    assert_eq!(meta.params[0].data_type, DataType::I64);
}
