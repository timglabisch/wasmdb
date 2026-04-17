use sql_engine::storage::CellValue;

mod common;
use common::{make_db, zset_rows};

#[test]
fn test_insert_delete_select_lifecycle() {
    let mut db = make_db();
    db.execute_mut("INSERT INTO users VALUES (4, 'Dave', 40)").unwrap();
    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(4)]);

    let deleted = db.execute_mut("DELETE FROM users WHERE users.id = 4").unwrap();
    let rows = zset_rows(&deleted, "users", -1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], CellValue::Str("Dave".into()));

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
    db.execute_mut("UPDATE users SET age = 31 WHERE users.id = 1").unwrap();
    db.execute_mut("DELETE FROM users WHERE users.id = 2").unwrap();
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
    assert_eq!(old_rows[0][2], CellValue::I64(30));
    assert_eq!(new_rows[0][2], CellValue::I64(99));

    let deleted = db.execute_mut("DELETE FROM users WHERE users.id = 1").unwrap();
    let rows = zset_rows(&deleted, "users", -1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], CellValue::I64(99));

    let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
    assert_eq!(count[0], vec![CellValue::I64(2)]);
}
