use database::Database;
use sql_engine::schema::TableSchema;
use super::cols::{i64_col, str_col};

pub fn create(db: &mut Database) {
    db.create_table(TableSchema {
        name: "products".into(),
        columns: vec![
            i64_col("id"),
            str_col("sku"), str_col("name"), str_col("description"),
            str_col("unit"),
            i64_col("unit_price"), i64_col("tax_rate"), i64_col("cost_price"),
            i64_col("active"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();
}
