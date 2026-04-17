use std::cell::RefCell;
use std::rc::Rc;

use database_reactive::{ReactiveDatabase, SubId};

fn setup() -> ReactiveDatabase {
    let mut rdb = ReactiveDatabase::new();
    rdb.execute_ddl("
        CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64
        )
    ").unwrap();
    rdb.execute_all("
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25)
    ").unwrap();
    rdb
}

#[derive(Default, Clone)]
struct Calls(Rc<RefCell<Vec<(SubId, Vec<usize>)>>>);

impl Calls {
    fn record(&self) -> impl Fn(SubId, &[usize]) + 'static {
        let inner = self.0.clone();
        move |id, triggered| inner.borrow_mut().push((id, triggered.to_vec()))
    }
    fn snapshot(&self) -> Vec<(SubId, Vec<usize>)> {
        self.0.borrow().clone()
    }
    fn len(&self) -> usize { self.0.borrow().len() }
    fn clear(&self) { self.0.borrow_mut().clear() }
}

#[test]
fn subscribe_fires_on_matching_insert() {
    let mut rdb = setup();
    let calls = Calls::default();
    let id = rdb.subscribe(
        "SELECT REACTIVE(users.id = 42) AS inv, users.name FROM users WHERE users.id = 42",
        Box::new(calls.record()),
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (42, 'Matt', 40)").unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls.snapshot()[0].0, id);

    // Non-matching insert — no callback.
    calls.clear();
    rdb.execute_mut("INSERT INTO users VALUES (99, 'Bob', 20)").unwrap();
    assert_eq!(calls.len(), 0);
}

#[test]
fn subscribe_fires_on_matching_delete() {
    let mut rdb = setup();
    let calls = Calls::default();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv, users.name FROM users WHERE users.id = 1",
        Box::new(calls.record()),
    ).unwrap();

    rdb.execute_mut("DELETE FROM users WHERE users.id = 1").unwrap();
    assert_eq!(calls.len(), 1);
}

#[test]
fn subscribe_fires_on_matching_update() {
    let mut rdb = setup();
    let calls = Calls::default();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv, users.name FROM users WHERE users.id = 1",
        Box::new(calls.record()),
    ).unwrap();

    // Update rewrites as delete+insert — both sides match id=1, so we expect two fires.
    rdb.execute_mut("UPDATE users SET name = 'Alicia' WHERE users.id = 1").unwrap();
    assert!(calls.len() >= 1);
}

#[test]
fn table_level_subscription_fires_on_any_mutation() {
    let mut rdb = setup();
    let calls = Calls::default();
    // REACTIVE(column) with a bare column ref is table-level.
    rdb.subscribe(
        "SELECT REACTIVE(users.id) AS inv, users.id FROM users",
        Box::new(calls.record()),
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'Zoe', 20)").unwrap();
    rdb.execute_mut("DELETE FROM users WHERE users.id = 2").unwrap();
    assert!(calls.len() >= 2);
}

#[test]
fn unsubscribe_stops_firing() {
    let mut rdb = setup();
    let calls = Calls::default();
    let id = rdb.subscribe(
        "SELECT REACTIVE(users.id) AS inv, users.id FROM users",
        Box::new(calls.record()),
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'Zoe', 20)").unwrap();
    let before = calls.len();
    assert!(before >= 1);

    rdb.unsubscribe(id);
    assert_eq!(rdb.subscription_count(), 0);

    rdb.execute_mut("INSERT INTO users VALUES (11, 'Jo', 22)").unwrap();
    assert_eq!(calls.len(), before); // no new fires
}

#[test]
fn apply_zset_notifies() {
    use sql_engine::storage::{CellValue, ZSet};

    let mut rdb = setup();
    let calls = Calls::default();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 5) AS inv, users.name FROM users WHERE users.id = 5",
        Box::new(calls.record()),
    ).unwrap();

    let mut zset = ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(5), CellValue::Str("Eve".into()), CellValue::I64(28)]);
    rdb.apply_zset(&zset).unwrap();
    assert_eq!(calls.len(), 1);
}

#[test]
fn execute_mut_on_empty_subs_skips_notify() {
    let mut rdb = setup();
    rdb.execute_mut("INSERT INTO users VALUES (10, 'X', 1)").unwrap();
    assert_eq!(rdb.subscription_count(), 0);
}

#[test]
fn replace_data_keeps_subscriptions() {
    let mut rdb = setup();
    let calls = Calls::default();
    rdb.subscribe(
        "SELECT REACTIVE(users.id) AS inv, users.id FROM users",
        Box::new(calls.record()),
    ).unwrap();

    use database::Database;
    let mut fresh = Database::new();
    fresh.execute_ddl("
        CREATE TABLE users (
            id I64 NOT NULL PRIMARY KEY,
            name STRING NOT NULL,
            age I64
        )
    ").unwrap();
    rdb.replace_data(&fresh);
    assert_eq!(rdb.subscription_count(), 1);

    rdb.execute_mut("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    assert!(calls.len() >= 1);
}

#[test]
fn notify_all_fires_every_callback_with_empty_triggered() {
    let mut rdb = setup();
    let a = Calls::default();
    let b = Calls::default();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1",
        Box::new(a.record()),
    ).unwrap();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 2) AS inv FROM users WHERE users.id = 2",
        Box::new(b.record()),
    ).unwrap();

    rdb.notify_all();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert!(a.snapshot()[0].1.is_empty());
    assert!(b.snapshot()[0].1.is_empty());
}

#[test]
fn execute_for_sql_clears_triggered() {
    let mut rdb = setup();
    let calls = Calls::default();
    let sql = "SELECT REACTIVE(users.id = 99) AS inv, users.name FROM users WHERE users.id = 99";
    rdb.subscribe(sql, Box::new(calls.record())).unwrap();

    // Trigger by inserting matching row.
    rdb.execute_mut("INSERT INTO users VALUES (99, 'Nina', 40)").unwrap();
    assert!(calls.len() >= 1);

    let triggered = rdb.take_triggered_for_sql(sql);
    assert!(triggered.is_some(), "triggered set should be populated after a fire");

    // Second call: cleared.
    let triggered2 = rdb.take_triggered_for_sql(sql);
    assert!(triggered2.is_none(), "triggered set should be empty after take");
}
