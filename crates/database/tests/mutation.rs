use database::{Database, MutResult};
use sql_engine::storage::CellValue;

mod common;
use common::{make_db, zset_rows};

// ── INSERT tests ─────────────────────────────────────────────────────

#[test]
fn test_insert_unknown_table() {
    let mut db = Database::new();
    let err = db.execute("INSERT INTO nope VALUES (1)");
    assert!(err.is_err());
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
        assert_ne!(old[2], CellValue::I64(99));
    }
    for new in &new_rows {
        assert_eq!(new[2], CellValue::I64(99));
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
    assert_eq!(result[1], vec![CellValue::Str("Bob".into())]);
    assert_eq!(result[2], vec![CellValue::I64(50)]);
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

    let result = db.execute("SELECT t.id FROM t WHERE t.val = 100").unwrap();
    assert_eq!(result[0], vec![CellValue::I64(3)]);

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
