use database::Database;
use sql_engine::storage::CellValue;

mod common;
use common::make_db;

#[test]
fn test_select_all() {
    let mut db = make_db();
    let result = db.execute("SELECT users.name, users.age FROM users").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].len(), 3);
}

#[test]
fn test_select_with_filter() {
    let mut db = make_db();
    let result = db.execute("SELECT users.name FROM users WHERE users.age > 28").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
}

#[test]
fn test_select_with_params() {
    use std::collections::HashMap;
    use sql_engine::execute::ParamValue;

    let mut db = make_db();
    let params = HashMap::from([("uid".into(), ParamValue::Int(2))]);
    let result = db.execute_with_params(
        "SELECT users.name FROM users WHERE users.id = :uid",
        params,
    ).unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Bob".into())]);
}

#[test]
fn test_join() {
    let mut db = make_db();
    db.execute_all("
        CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL,
            amount I64 NOT NULL
        );
        INSERT INTO orders VALUES (10, 1, 100);
        INSERT INTO orders VALUES (11, 1, 200);
        INSERT INTO orders VALUES (12, 2, 50)
    ").unwrap();

    let result = db.execute(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    assert_eq!(result[0].len(), 3); // Alice, Alice, Bob
}

#[test]
fn test_aggregate() {
    let mut db = make_db();
    let result = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(3)]);
}

#[test]
fn test_orders_inner_join_query() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64 NOT NULL
        );
        CREATE TABLE orders (
            id I64 NOT NULL PRIMARY KEY,
            user_id I64 NOT NULL,
            amount I64 NOT NULL,
            status STRING NOT NULL
        );
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        INSERT INTO orders VALUES (100, 1, 5000, 'pending');
        INSERT INTO orders VALUES (101, 2, 3000, 'shipped')
    ").unwrap();

    let result = db.execute(
        "SELECT orders.id, orders.user_id, users.name, orders.amount, orders.status FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY orders.id"
    ).unwrap();

    assert_eq!(result.len(), 5, "should have 5 columns");
    assert_eq!(result[0].len(), 2, "should have 2 rows");
    assert_eq!(result[0][0], CellValue::I64(100));
    assert_eq!(result[1][0], CellValue::I64(1));
    assert_eq!(result[2][0], CellValue::Str("Alice".into()));
    assert_eq!(result[3][0], CellValue::I64(5000));
    assert_eq!(result[4][0], CellValue::Str("pending".into()));
}
