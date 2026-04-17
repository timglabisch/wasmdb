#![allow(dead_code)]

use database::{Database, MutResult};
use sql_engine::storage::CellValue;

/// Extract entries from a MutResult::Mutation, filtering by table and weight.
pub fn zset_rows(result: &MutResult, table: &str, weight: i32) -> Vec<Vec<CellValue>> {
    match result {
        MutResult::Mutation(zset) => zset.entries.iter()
            .filter(|e| e.table == table && e.weight == weight)
            .map(|e| e.row.clone())
            .collect(),
        _ => panic!("expected Mutation"),
    }
}

pub fn make_db() -> Database {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64
        );
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        INSERT INTO users VALUES (3, 'Carol', 35)
    ").unwrap();
    db
}
