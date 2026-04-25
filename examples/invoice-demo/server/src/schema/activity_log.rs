use std::collections::HashMap;

use sql_engine::schema::TableSchema;
use super::cols::{str_col, uuid_col};

pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("activity_log".into(), TableSchema {
        name: "activity_log".into(),
        columns: vec![
            uuid_col("id"),
            str_col("timestamp"),
            str_col("entity_type"), uuid_col("entity_id"),
            str_col("action"), str_col("actor"), str_col("detail"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    });
}
