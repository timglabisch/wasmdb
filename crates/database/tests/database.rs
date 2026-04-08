use database::Database;
use sql_engine::storage::CellValue;

fn make_db() -> Database {
    let mut db = Database::new();
    db.execute_ddl(
        "CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64
        );"
    ).unwrap();
    db.insert("users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
    db.insert("users", &[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
    db.insert("users", &[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
    db
}

#[test]
fn test_select_all() {
    let db = make_db();
    let result = db.execute("SELECT users.name, users.age FROM users").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].len(), 3);
}

#[test]
fn test_select_with_filter() {
    let db = make_db();
    let result = db.execute("SELECT users.name FROM users WHERE users.age > 28").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
}

#[test]
fn test_select_with_params() {
    use std::collections::HashMap;
    use sql_engine::execute::ParamValue;

    let db = make_db();
    let params = HashMap::from([("uid".into(), ParamValue::Int(2))]);
    let result = db.execute_with_params(
        "SELECT users.name FROM users WHERE users.id = :uid",
        params,
    ).unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Bob".into())]);
}

#[test]
fn test_create_table_ddl() {
    let mut db = Database::new();
    db.execute_ddl(
        "CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL,
            amount I64
        );"
    ).unwrap();
    assert!(db.table("orders").is_some());
    assert!(db.table("nonexistent").is_none());
}

#[test]
fn test_multi_statement_ddl() {
    let mut db = Database::new();
    db.execute_ddl(
        "CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL
        );
        CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL
        );"
    ).unwrap();
    assert!(db.table("users").is_some());
    assert!(db.table("orders").is_some());
}

#[test]
fn test_duplicate_table_error() {
    let mut db = Database::new();
    db.execute_ddl("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY);").unwrap();
    let err = db.execute_ddl("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY);");
    assert!(err.is_err());
}

#[test]
fn test_insert_unknown_table() {
    let mut db = Database::new();
    let err = db.insert("nope", &[CellValue::I64(1)]);
    assert!(err.is_err());
}

#[test]
fn test_join() {
    let mut db = make_db();
    db.execute_ddl(
        "CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL,
            amount I64 NOT NULL
        );"
    ).unwrap();
    db.insert("orders", &[CellValue::I64(10), CellValue::I64(1), CellValue::I64(100)]).unwrap();
    db.insert("orders", &[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]).unwrap();
    db.insert("orders", &[CellValue::I64(12), CellValue::I64(2), CellValue::I64(50)]).unwrap();

    let result = db.execute(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    assert_eq!(result[0].len(), 3); // Alice, Alice, Bob
}

#[test]
fn test_aggregate() {
    let db = make_db();
    let result = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(3)]);
}
