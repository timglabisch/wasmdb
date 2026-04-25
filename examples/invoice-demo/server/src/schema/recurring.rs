use std::collections::HashMap;

use sql_engine::schema::{IndexSchema, IndexType, TableSchema};
use super::cols::{i64_col, str_col, uuid_col};

pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("recurring_invoices".into(), TableSchema {
        name: "recurring_invoices".into(),
        columns: vec![
            uuid_col("id"), uuid_col("customer_id"),
            str_col("template_name"),
            str_col("interval_unit"), i64_col("interval_value"),
            str_col("next_run"), str_col("last_run"),
            i64_col("enabled"),
            str_col("status_template"), str_col("notes_template"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });

    schemas.insert("recurring_positions".into(), TableSchema {
        name: "recurring_positions".into(),
        columns: vec![
            uuid_col("id"), uuid_col("recurring_id"), i64_col("position_nr"),
            str_col("description"),
            i64_col("quantity"), i64_col("unit_price"), i64_col("tax_rate"),
            str_col("unit"), str_col("item_number"),
            i64_col("discount_pct"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });
}
