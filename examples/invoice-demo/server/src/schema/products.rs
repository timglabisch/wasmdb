use std::collections::HashMap;

use sql_engine::schema::TableSchema;
use super::cols::{i64_col, str_col, uuid_col};

pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("products".into(), TableSchema {
        name: "products".into(),
        columns: vec![
            uuid_col("id"),
            str_col("sku"), str_col("name"), str_col("description"),
            str_col("unit"),
            i64_col("unit_price"), i64_col("tax_rate"), i64_col("cost_price"),
            i64_col("active"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    });
}
