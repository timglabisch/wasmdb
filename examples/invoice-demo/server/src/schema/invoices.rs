use std::collections::HashMap;

use sql_engine::schema::{IndexSchema, IndexType, TableSchema};
use super::cols::{i64_col, str_col, uuid_col};

/// Invoices + positions + payments — tied together by invoice_id FKs.
pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("invoices".into(), TableSchema {
        name: "invoices".into(),
        columns: vec![
            uuid_col("id"), uuid_col("customer_id"),
            str_col("number"), str_col("status"),
            str_col("date_issued"), str_col("date_due"), str_col("notes"),
            str_col("doc_type"),
            uuid_col("parent_id"),
            str_col("service_date"),
            i64_col("cash_allowance_pct"), i64_col("cash_allowance_days"), i64_col("discount_pct"),
            str_col("payment_method"),
            uuid_col("sepa_mandate_id"),
            str_col("currency"), str_col("language"),
            str_col("project_ref"), str_col("external_id"),
            str_col("billing_street"), str_col("billing_zip"), str_col("billing_city"), str_col("billing_country"),
            str_col("shipping_street"), str_col("shipping_zip"), str_col("shipping_city"), str_col("shipping_country"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });

    schemas.insert("positions".into(), TableSchema {
        name: "positions".into(),
        columns: vec![
            uuid_col("id"), uuid_col("invoice_id"), i64_col("position_nr"),
            str_col("description"),
            i64_col("quantity"), i64_col("unit_price"), i64_col("tax_rate"),
            uuid_col("product_id"),
            str_col("item_number"), str_col("unit"),
            i64_col("discount_pct"), i64_col("cost_price"),
            str_col("position_type"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });

    schemas.insert("payments".into(), TableSchema {
        name: "payments".into(),
        columns: vec![
            uuid_col("id"), uuid_col("invoice_id"),
            i64_col("amount"), str_col("paid_at"),
            str_col("method"), str_col("reference"), str_col("note"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });
}
