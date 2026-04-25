use std::collections::HashMap;

use sql_engine::schema::{IndexSchema, IndexType, TableSchema};
use super::cols::{i64_col, str_col, uuid_col};

pub fn register(schemas: &mut HashMap<String, TableSchema>) {
    schemas.insert("customers".into(), TableSchema {
        name: "customers".into(),
        columns: vec![
            uuid_col("id"),
            str_col("name"), str_col("email"), str_col("created_at"),
            str_col("company_type"), str_col("tax_id"), str_col("vat_id"),
            i64_col("payment_terms_days"), i64_col("default_discount_pct"),
            str_col("billing_street"), str_col("billing_zip"), str_col("billing_city"), str_col("billing_country"),
            str_col("shipping_street"), str_col("shipping_zip"), str_col("shipping_city"), str_col("shipping_country"),
            str_col("default_iban"), str_col("default_bic"), str_col("notes"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    });

    schemas.insert("contacts".into(), TableSchema {
        name: "contacts".into(),
        columns: vec![
            uuid_col("id"), uuid_col("customer_id"),
            str_col("name"), str_col("email"), str_col("phone"), str_col("role"),
            i64_col("is_primary"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    });
}
