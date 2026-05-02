use database::{Database, MutResult};

#[test]
fn test_create_table_ddl() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL,
            amount I64
        )
    ").unwrap();
    assert!(db.table("orders").is_some());
    assert!(db.table("nonexistent").is_none());
}

#[test]
fn test_multi_statement_ddl() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL
        );
        CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL
        )
    ").unwrap();
    assert!(db.table("users").is_some());
    assert!(db.table("orders").is_some());
}

#[test]
fn test_duplicate_table_error() {
    let mut db = Database::new();
    db.execute("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY)").unwrap();
    let err = db.execute("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY)");
    assert!(err.is_err());
}

#[test]
fn test_execute_all_mixed() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE t (id I64 NOT NULL PRIMARY KEY, val STRING);
        INSERT INTO t VALUES (1, 'a');
        INSERT INTO t VALUES (2, 'b')
    ").unwrap();
    let result = db.execute("SELECT t.val FROM t").unwrap();
    assert_eq!(result[0].len(), 2);
}

#[test]
fn test_execute_mut_ddl() {
    let mut db = Database::new();
    let result = db.execute_mut("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY)").unwrap();
    assert!(matches!(result, MutResult::Ddl));
}
