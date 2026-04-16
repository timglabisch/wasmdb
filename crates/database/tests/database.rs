use database::{Database, MutResult};
use sql_engine::storage::{CellValue, ZSet};

/// Extract entries from a MutResult::Mutation, filtering by table and weight.
fn zset_rows(result: &MutResult, table: &str, weight: i32) -> Vec<Vec<CellValue>> {
    match result {
        MutResult::Mutation(zset) => zset.entries.iter()
            .filter(|e| e.table == table && e.weight == weight)
            .map(|e| e.row.clone())
            .collect(),
        _ => panic!("expected Mutation"),
    }
}

fn make_db() -> Database {
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
fn test_insert_unknown_table() {
    let mut db = Database::new();
    let err = db.execute("INSERT INTO nope VALUES (1)");
    assert!(err.is_err());
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
fn test_insert_via_sql() {
    let mut db = make_db();
    db.execute("INSERT INTO users VALUES (4, 'Dave', 40)").unwrap();
    let result = db.execute("SELECT users.name FROM users WHERE users.id = 4").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Dave".into())]);
}

#[test]
fn test_insert_with_columns() {
    let mut db = make_db();
    db.execute("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40)").unwrap();
    let result = db.execute("SELECT users.name FROM users WHERE users.id = 4").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Dave".into())]);
}

#[test]
fn test_insert_multi_row() {
    let mut db = make_db();
    db.execute("INSERT INTO users VALUES (4, 'Dave', 40), (5, 'Eve', 28)").unwrap();
    let result = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(5)]);
}

#[test]
fn test_insert_with_null() {
    let mut db = make_db();
    db.execute("INSERT INTO users VALUES (4, 'Dave', NULL)").unwrap();
    let result = db.execute("SELECT users.age FROM users WHERE users.id = 4").unwrap();
    assert_eq!(result[0], vec![CellValue::Null]);
}

#[test]
fn test_insert_unknown_table_sql() {
    let mut db = make_db();
    let err = db.execute("INSERT INTO nonexistent VALUES (1)");
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

// ── DELETE tests ─────────────────────────────────────────────────────

#[test]
fn test_delete_all() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 3);
    let result = db.execute("SELECT users.id FROM users").unwrap();
    assert_eq!(result[0].len(), 0);
}

#[test]
fn test_delete_with_equality_filter() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users WHERE users.id = 2").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], CellValue::I64(2));
    assert_eq!(rows[0][1], CellValue::Str("Bob".into()));
    assert_eq!(rows[0][2], CellValue::I64(25));
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(2)]);
    // Bob should be gone
    let names = db.execute("SELECT users.name FROM users ORDER BY users.id").unwrap();
    assert_eq!(names[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
}

#[test]
fn test_delete_with_range_filter() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users WHERE users.age > 28").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 2); // Alice(30) and Carol(35)
    let names = db.execute("SELECT users.name FROM users").unwrap();
    assert_eq!(names[0], vec![CellValue::Str("Bob".into())]);
}

#[test]
fn test_delete_with_and_filter() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users WHERE users.age > 20 AND users.age < 30").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 1); // Bob(25)
    assert_eq!(rows[0][1], CellValue::Str("Bob".into()));
}

#[test]
fn test_delete_with_or_filter() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users WHERE users.id = 1 OR users.id = 3").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 2); // Alice and Carol
    let names = db.execute("SELECT users.name FROM users").unwrap();
    assert_eq!(names[0], vec![CellValue::Str("Bob".into())]);
}

#[test]
fn test_delete_no_match() {
    let mut db = make_db();
    let result = db.execute_mut("DELETE FROM users WHERE users.id = 999").unwrap();
    let rows = zset_rows(&result, "users", -1);
    assert_eq!(rows.len(), 0);
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(3)]);
}

#[test]
fn test_delete_unknown_table() {
    let mut db = make_db();
    let err = db.execute_mut("DELETE FROM nonexistent WHERE nonexistent.id = 1");
    assert!(err.is_err());
}

#[test]
fn test_delete_updates_indexes() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE t (
            id I64 NOT NULL PRIMARY KEY,
            val I64 NOT NULL,
            INDEX idx_val (val) USING BTREE
        );
        INSERT INTO t VALUES (1, 100);
        INSERT INTO t VALUES (2, 200);
        INSERT INTO t VALUES (3, 100)
    ").unwrap();

    db.execute_mut("DELETE FROM t WHERE t.id = 1").unwrap();

    // Index lookup for val=100 should only find row 3 now
    let result = db.execute("SELECT t.id FROM t WHERE t.val = 100").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(3)]);
}

// ── UPDATE tests ─────────────────────────────────────────────────────

#[test]
fn test_update_all_rows() {
    let mut db = make_db();
    let result = db.execute_mut("UPDATE users SET age = 99").unwrap();
    let old_rows = zset_rows(&result, "users", -1);
    let new_rows = zset_rows(&result, "users", 1);
    assert_eq!(old_rows.len(), 3);
    assert_eq!(new_rows.len(), 3);
    for old in &old_rows {
        assert_ne!(old[2], CellValue::I64(99)); // old age was not 99
    }
    for new in &new_rows {
        assert_eq!(new[2], CellValue::I64(99)); // new age is 99
    }
    let ages = db.execute("SELECT users.age FROM users").unwrap();
    assert_eq!(ages[0], vec![CellValue::I64(99), CellValue::I64(99), CellValue::I64(99)]);
}

#[test]
fn test_update_single_row() {
    let mut db = make_db();
    let result = db.execute_mut("UPDATE users SET age = 31 WHERE users.id = 1").unwrap();
    let old_rows = zset_rows(&result, "users", -1);
    let new_rows = zset_rows(&result, "users", 1);
    assert_eq!(old_rows.len(), 1);
    assert_eq!(new_rows.len(), 1);
    assert_eq!(old_rows[0][0], CellValue::I64(1));
    assert_eq!(old_rows[0][1], CellValue::Str("Alice".into()));
    assert_eq!(old_rows[0][2], CellValue::I64(30));
    assert_eq!(new_rows[0][0], CellValue::I64(1));
    assert_eq!(new_rows[0][1], CellValue::Str("Alice".into()));
    assert_eq!(new_rows[0][2], CellValue::I64(31));
    let result = db.execute("SELECT users.age FROM users WHERE users.id = 1").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(31)]);
}

#[test]
fn test_update_multiple_columns() {
    let mut db = make_db();
    let result = db.execute_mut("UPDATE users SET name = 'Alicia', age = 31 WHERE users.id = 1").unwrap();
    let old_rows = zset_rows(&result, "users", -1);
    let new_rows = zset_rows(&result, "users", 1);
    assert_eq!(old_rows.len(), 1);
    assert_eq!(new_rows.len(), 1);
    assert_eq!(old_rows[0][1], CellValue::Str("Alice".into()));
    assert_eq!(old_rows[0][2], CellValue::I64(30));
    assert_eq!(new_rows[0][1], CellValue::Str("Alicia".into()));
    assert_eq!(new_rows[0][2], CellValue::I64(31));
    let result = db.execute("SELECT users.name, users.age FROM users WHERE users.id = 1").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Alicia".into())]);
    assert_eq!(result[1], vec![CellValue::I64(31)]);
}

#[test]
fn test_update_no_match() {
    let mut db = make_db();
    let result = db.execute_mut("UPDATE users SET age = 99 WHERE users.id = 999").unwrap();
    let old_rows = zset_rows(&result, "users", -1);
    let new_rows = zset_rows(&result, "users", 1);
    assert_eq!(old_rows.len(), 0);
    assert_eq!(new_rows.len(), 0);
    // DB unchanged
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(3)]);
}

#[test]
fn test_update_unknown_table() {
    let mut db = make_db();
    let err = db.execute_mut("UPDATE nonexistent SET x = 1");
    assert!(err.is_err());
}

#[test]
fn test_update_unknown_column() {
    let mut db = make_db();
    let err = db.execute_mut("UPDATE users SET nonexistent = 1 WHERE users.id = 1");
    assert!(err.is_err());
}

#[test]
fn test_update_preserves_unset_columns() {
    let mut db = make_db();
    db.execute_mut("UPDATE users SET age = 50 WHERE users.id = 2").unwrap();
    let result = db.execute("SELECT users.id, users.name, users.age FROM users WHERE users.id = 2").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(2)]);
    assert_eq!(result[1], vec![CellValue::Str("Bob".into())]); // unchanged
    assert_eq!(result[2], vec![CellValue::I64(50)]); // updated
}

#[test]
fn test_update_updates_indexes() {
    let mut db = Database::new();
    db.execute_all("
        CREATE TABLE t (
            id I64 NOT NULL PRIMARY KEY,
            val I64 NOT NULL,
            INDEX idx_val (val) USING BTREE
        );
        INSERT INTO t VALUES (1, 100);
        INSERT INTO t VALUES (2, 200);
        INSERT INTO t VALUES (3, 100)
    ").unwrap();

    db.execute_mut("UPDATE t SET val = 300 WHERE t.id = 1").unwrap();

    // Old index value should not find row 1
    let result = db.execute("SELECT t.id FROM t WHERE t.val = 100").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(3)]);

    // New index value should find row 1
    let result = db.execute("SELECT t.id FROM t WHERE t.val = 300").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(1)]);
}

#[test]
fn test_update_with_and_filter() {
    let mut db = make_db();
    db.execute_mut("UPDATE users SET age = 99 WHERE users.age >= 30 AND users.age <= 35").unwrap();
    let result = db.execute("SELECT users.name, users.age FROM users ORDER BY users.id").unwrap();
    // Alice(30→99), Bob(25→25), Carol(35→99)
    assert_eq!(result[1], vec![CellValue::I64(99), CellValue::I64(25), CellValue::I64(99)]);
}

// ── execute_mut result type tests ────────────────────────────────────

#[test]
fn test_execute_mut_insert() {
    let mut db = make_db();
    let result = db.execute_mut("INSERT INTO users VALUES (4, 'Dave', 40)").unwrap();
    let rows = zset_rows(&result, "users", 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![CellValue::I64(4), CellValue::Str("Dave".into()), CellValue::I64(40)]);
}

#[test]
fn test_execute_mut_insert_multi_row() {
    let mut db = make_db();
    let result = db.execute_mut("INSERT INTO users VALUES (4, 'Dave', 40), (5, 'Eve', 28)").unwrap();
    let rows = zset_rows(&result, "users", 1);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], CellValue::I64(4));
    assert_eq!(rows[1][0], CellValue::I64(5));
}

#[test]
fn test_execute_mut_select() {
    let mut db = make_db();
    let result = db.execute_mut("SELECT users.name FROM users WHERE users.id = 1").unwrap();
    match result {
        MutResult::Rows(cols) => {
            assert_eq!(cols[0], vec![CellValue::Str("Alice".into())]);
        }
        _ => panic!("expected Rows"),
    }
}

#[test]
fn test_execute_mut_ddl() {
    let mut db = Database::new();
    let result = db.execute_mut("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY)").unwrap();
    assert!(matches!(result, MutResult::Ddl));
}

// ── Integration / lifecycle tests ────────────────────────────────────

#[test]
fn test_insert_delete_select_lifecycle() {
    let mut db = make_db();
    // Insert a new user
    db.execute_mut("INSERT INTO users VALUES (4, 'Dave', 40)").unwrap();
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(4)]);

    // Delete the new user
    let deleted = db.execute_mut("DELETE FROM users WHERE users.id = 4").unwrap();
    let rows = zset_rows(&deleted, "users", -1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], CellValue::Str("Dave".into()));

    // Verify gone
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(3)]);
}

#[test]
fn test_insert_update_select_lifecycle() {
    let mut db = make_db();
    db.execute_mut("INSERT INTO users VALUES (4, 'Dave', 40)").unwrap();

    let updated = db.execute_mut("UPDATE users SET name = 'David', age = 41 WHERE users.id = 4").unwrap();
    let old_rows = zset_rows(&updated, "users", -1);
    let new_rows = zset_rows(&updated, "users", 1);
    assert_eq!(old_rows.len(), 1);
    assert_eq!(new_rows.len(), 1);
    assert_eq!(old_rows[0][1], CellValue::Str("Dave".into()));
    assert_eq!(new_rows[0][1], CellValue::Str("David".into()));
    assert_eq!(old_rows[0][2], CellValue::I64(40));
    assert_eq!(new_rows[0][2], CellValue::I64(41));

    let result = db.execute("SELECT users.name, users.age FROM users WHERE users.id = 4").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("David".into())]);
    assert_eq!(result[1], vec![CellValue::I64(41)]);
}

#[test]
fn test_delete_all_then_empty() {
    let mut db = make_db();
    db.execute_mut("DELETE FROM users").unwrap();
    let result = db.execute("SELECT users.id FROM users").unwrap();
    assert_eq!(result[0].len(), 0);
}

#[test]
fn test_mixed_update_delete() {
    let mut db = make_db();
    // Update Alice's age
    db.execute_mut("UPDATE users SET age = 31 WHERE users.id = 1").unwrap();
    // Delete Bob
    db.execute_mut("DELETE FROM users WHERE users.id = 2").unwrap();
    // Update Carol's name
    db.execute_mut("UPDATE users SET name = 'Caroline' WHERE users.id = 3").unwrap();

    let result = db.execute("SELECT users.id, users.name, users.age FROM users ORDER BY users.id").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(1), CellValue::I64(3)]);
    assert_eq!(result[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Caroline".into())]);
    assert_eq!(result[2], vec![CellValue::I64(31), CellValue::I64(35)]);
}

#[test]
fn test_delete_then_reinsert_same_id() {
    let mut db = make_db();
    db.execute_mut("DELETE FROM users WHERE users.id = 1").unwrap();
    db.execute_mut("INSERT INTO users VALUES (1, 'Alicia', 28)").unwrap();

    let result = db.execute("SELECT users.name, users.age FROM users WHERE users.id = 1").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Alicia".into())]);
    assert_eq!(result[1], vec![CellValue::I64(28)]);
}

#[test]
fn test_update_then_delete() {
    let mut db = make_db();
    let updated = db.execute_mut("UPDATE users SET age = 99 WHERE users.id = 1").unwrap();
    let old_rows = zset_rows(&updated, "users", -1);
    let new_rows = zset_rows(&updated, "users", 1);
    assert_eq!(old_rows[0][2], CellValue::I64(30)); // old age
    assert_eq!(new_rows[0][2], CellValue::I64(99)); // new age

    let deleted = db.execute_mut("DELETE FROM users WHERE users.id = 1").unwrap();
    let rows = zset_rows(&deleted, "users", -1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], CellValue::I64(99)); // deletes the updated row

    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(2)]);
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

    // This is the exact query from OrdersPanel
    let result = db.execute(
        "SELECT orders.id, orders.user_id, users.name, orders.amount, orders.status FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY orders.id"
    ).unwrap();

    eprintln!("columns: {}", result.len());
    for (i, col) in result.iter().enumerate() {
        eprintln!("  col[{}]: {:?}", i, col);
    }

    assert_eq!(result.len(), 5, "should have 5 columns");
    assert_eq!(result[0].len(), 2, "should have 2 rows");
    // First row: order 100, user_id 1, Alice, 5000, pending
    assert_eq!(result[0][0], CellValue::I64(100));
    assert_eq!(result[1][0], CellValue::I64(1));
    assert_eq!(result[2][0], CellValue::Str("Alice".into()));
    assert_eq!(result[3][0], CellValue::I64(5000));
    assert_eq!(result[4][0], CellValue::Str("pending".into()));
}
