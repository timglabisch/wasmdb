use database::Database;
use sql_engine::schema::TableSchema;
use super::cols::{i64_col, str_col};

pub fn create(db: &mut Database) {
    db.create_table(TableSchema {
        name: "activity_log".into(),
        columns: vec![
            i64_col("id"),
            str_col("timestamp"),
            str_col("entity_type"), i64_col("entity_id"),
            str_col("action"), str_col("actor"), str_col("detail"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();
}
