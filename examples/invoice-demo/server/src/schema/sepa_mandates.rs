use std::collections::HashMap;

use sql_engine::schema::{IndexSchema, IndexType, TableSchema};
use super::cols::{str_col, uuid_col};

pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("sepa_mandates".into(), TableSchema {
        name: "sepa_mandates".into(),
        columns: vec![
            uuid_col("id"), uuid_col("customer_id"),
            str_col("mandate_ref"),
            str_col("iban"), str_col("bic"),
            str_col("holder_name"),
            str_col("signed_at"),
            str_col("status"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });
}
