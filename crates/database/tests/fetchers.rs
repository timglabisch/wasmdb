//! Native fetcher registration on `Database` + dual sync/async API.
//!
//! Covers: async path resolves fetchers and returns their rows; sync path
//! errors with `RequiresAsync` for queries that reference a fetcher source;
//! fetcher sources inside subqueries are detected too.

use std::sync::Arc;

use database::{Caller, Database, DbError};
use sql_engine::execute::FetcherFuture;
use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};
use sql_engine::schema::DataType;
use sql_engine::storage::CellValue;

fn make_db() -> Database {
    let mut db = Database::new();
    db.execute_all(
        "CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64
        )",
    ).unwrap();
    db
}

fn register_users_by_owner(db: &mut Database) {
    db.register_caller(Caller::new(
        "users::by_owner",
        RequirementMeta {
            row_table: "users".into(),
            params: vec![RequirementParamDef {
                name: "owner_id".into(),
                data_type: DataType::I64,
            }],
        },
        Arc::new(|args: Vec<sql_parser::ast::Value>| {
            let owner = match args.first() {
                Some(sql_parser::ast::Value::Int(n)) => *n,
                _ => {
                    return Box::pin(async { Err("expected Int".into()) })
                        as FetcherFuture;
                }
            };
            Box::pin(async move {
                if owner == 1 {
                    Ok(vec![vec![
                        CellValue::I64(1),
                        CellValue::Str("Alice".into()),
                        CellValue::I64(30),
                    ]])
                } else {
                    Ok(vec![])
                }
            }) as FetcherFuture
        }),
    ));
}

#[test]
fn async_execute_resolves_fetcher() {
    let mut db = make_db();
    register_users_by_owner(&mut db);

    let result = pollster::block_on(
        db.execute_async("SELECT users.name FROM users.by_owner(1)"),
    )
    .expect("execute_async");
    assert_eq!(result[0], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn sync_execute_errors_on_fetcher_source() {
    let mut db = make_db();
    register_users_by_owner(&mut db);

    let err = db
        .execute("SELECT users.name FROM users.by_owner(1)")
        .unwrap_err();
    assert!(
        matches!(err, DbError::RequiresAsync),
        "expected RequiresAsync, got {err:?}",
    );
}

#[test]
fn sync_execute_errors_on_fetcher_inside_subquery() {
    // Fetcher-Source tief in `WHERE id IN (SELECT …)` muss den sync-Pfad
    // genauso blockieren wie eine Top-Level-Source — sonst würde die Engine
    // gegen eine leere `users`-Tabelle scannen.
    let mut db = make_db();
    register_users_by_owner(&mut db);

    let err = db
        .execute(
            "SELECT users.name FROM users \
             WHERE users.id IN (SELECT users.id FROM users.by_owner(1))",
        )
        .unwrap_err();
    assert!(
        matches!(err, DbError::RequiresAsync),
        "expected RequiresAsync, got {err:?}",
    );
}

#[test]
fn sync_execute_still_works_without_fetcher_source() {
    // Fetcher registriert, aber die konkrete Query nutzt ihn nicht →
    // sync-Pfad bleibt gangbar.
    let mut db = make_db();
    register_users_by_owner(&mut db);
    db.execute("INSERT INTO users VALUES (7, 'Zed', 50)").unwrap();

    let result = db
        .execute("SELECT users.name FROM users WHERE users.id = 7")
        .unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Zed".into())]);
}
