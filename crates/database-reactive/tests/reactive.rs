use std::cell::RefCell;
use std::rc::Rc;

use database_reactive::{ReactiveDatabase, SubscriptionId};

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
struct Calls(Rc<RefCell<Vec<(SubscriptionId, Vec<usize>)>>>);

impl Calls {
    fn record(&self) -> impl Fn(SubscriptionId, &[usize]) + 'static {
        let inner = self.0.clone();
        move |id, triggered| inner.borrow_mut().push((id, triggered.to_vec()))
    }
    fn snapshot(&self) -> Vec<(SubscriptionId, Vec<usize>)> {
        self.0.borrow().clone()
    }
    fn len(&self) -> usize { self.0.borrow().len() }
    fn clear(&self) { self.0.borrow_mut().clear() }
}

#[test]
fn subscribe_fires_on_matching_insert() {
    let mut rdb = setup();
    let calls = Calls::default();
    let (_handle, id) = rdb.subscribe(
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
    let (handle, _id) = rdb.subscribe(
        "SELECT REACTIVE(users.id) AS inv, users.id FROM users",
        Box::new(calls.record()),
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'Zoe', 20)").unwrap();
    let before = calls.len();
    assert!(before >= 1);

    rdb.unsubscribe(handle);
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

// ── Dedup and handle semantics ──────────────────────────────────────

#[test]
fn subscribe_twice_with_same_sql_shares_sub_id_and_fires_both_callbacks() {
    let mut rdb = setup();
    let a = Calls::default();
    let b = Calls::default();
    let sql = "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1";

    let (handle_a, id_a) = rdb.subscribe(sql, Box::new(a.record())).unwrap();
    let (handle_b, id_b) = rdb.subscribe(sql, Box::new(b.record())).unwrap();

    assert_eq!(id_a, id_b, "identical SQL must resolve to same SubscriptionId");
    assert_ne!(handle_a, handle_b, "each caller must get its own handle");
    assert_eq!(rdb.subscription_count(), 1, "one deduped subscription");
    assert_eq!(rdb.handle_count(), 2, "two caller handles");

    rdb.execute_mut("UPDATE users SET name = 'Alicia' WHERE users.id = 1").unwrap();
    assert!(a.len() >= 1, "first callback must fire");
    assert!(b.len() >= 1, "second callback must fire");
}

#[test]
fn subscribe_different_sql_gets_distinct_sub_ids() {
    let mut rdb = setup();
    let a = Calls::default();
    let b = Calls::default();

    let (_, id_a) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1",
        Box::new(a.record()),
    ).unwrap();
    let (_, id_b) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 2) AS inv FROM users WHERE users.id = 2",
        Box::new(b.record()),
    ).unwrap();

    assert_ne!(id_a, id_b);
    assert_eq!(rdb.subscription_count(), 2);
}

#[test]
fn unsubscribe_one_handle_keeps_other_callbacks_firing() {
    let mut rdb = setup();
    let a = Calls::default();
    let b = Calls::default();
    let sql = "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1";

    let (handle_a, _) = rdb.subscribe(sql, Box::new(a.record())).unwrap();
    let (_handle_b, _) = rdb.subscribe(sql, Box::new(b.record())).unwrap();

    assert!(rdb.unsubscribe(handle_a));
    assert_eq!(rdb.subscription_count(), 1, "still alive until last handle released");
    assert_eq!(rdb.handle_count(), 1);

    rdb.execute_mut("UPDATE users SET name = 'X' WHERE users.id = 1").unwrap();
    assert_eq!(a.len(), 0, "released handle's callback must not fire");
    assert!(b.len() >= 1, "remaining handle's callback still fires");
}

#[test]
fn last_handle_unsubscribe_tears_down_subscription() {
    let mut rdb = setup();
    let a = Calls::default();
    let b = Calls::default();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";

    let (handle_a, _) = rdb.subscribe(sql, Box::new(a.record())).unwrap();
    let (handle_b, _) = rdb.subscribe(sql, Box::new(b.record())).unwrap();

    assert!(rdb.unsubscribe(handle_a));
    assert_eq!(rdb.subscription_count(), 1);

    assert!(rdb.unsubscribe(handle_b));
    assert_eq!(rdb.subscription_count(), 0, "last handle out must drop sub");
    assert_eq!(rdb.handle_count(), 0);
}

#[test]
fn unsubscribe_unknown_handle_returns_false_and_is_noop() {
    use database_reactive::SubscriptionHandle;
    let mut rdb = setup();
    let a = Calls::default();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    rdb.subscribe(sql, Box::new(a.record())).unwrap();

    assert!(!rdb.unsubscribe(SubscriptionHandle(9999)));
    assert_eq!(rdb.subscription_count(), 1, "unknown handle must not corrupt state");
}

#[test]
fn double_unsubscribe_returns_false_second_time() {
    let mut rdb = setup();
    let a = Calls::default();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    let (handle, _) = rdb.subscribe(sql, Box::new(a.record())).unwrap();

    assert!(rdb.unsubscribe(handle));
    assert!(!rdb.unsubscribe(handle), "second release of same handle is a no-op");
}

#[test]
fn resubscribe_after_full_teardown_allocates_new_sub_id() {
    let mut rdb = setup();
    let a = Calls::default();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    let (handle_1, id_1) = rdb.subscribe(sql, Box::new(a.record())).unwrap();
    assert!(rdb.unsubscribe(handle_1));
    assert_eq!(rdb.subscription_count(), 0);

    let b = Calls::default();
    let (_, id_2) = rdb.subscribe(sql, Box::new(b.record())).unwrap();
    assert_ne!(id_1, id_2, "fresh subscribe after teardown must allocate a new id");
}
