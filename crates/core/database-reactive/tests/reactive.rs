use std::cell::Cell;
use std::rc::Rc;

use database_reactive::{DirtyNotification, ReactiveDatabase, SubscriptionId};

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

/// Drain all pending notifications into a Vec. Equivalent to the JS
/// `while (n = next_dirty()) { ... }` pull loop.
fn drain(rdb: &mut ReactiveDatabase) -> Vec<DirtyNotification> {
    let mut out = Vec::new();
    while let Some(n) = rdb.next_dirty() {
        out.push(n);
    }
    out
}

#[test]
fn subscribe_fires_on_matching_insert() {
    let mut rdb = setup();
    let (_handle, id) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 42) AS inv, users.name FROM users WHERE users.id = 42",
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (42, 'Matt', 40)").unwrap();
    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sub_id, id);

    // Non-matching insert — nothing to drain.
    rdb.execute_mut("INSERT INTO users VALUES (99, 'Bob', 20)").unwrap();
    assert!(drain(&mut rdb).is_empty());
}

#[test]
fn subscribe_fires_on_matching_delete() {
    let mut rdb = setup();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv, users.name FROM users WHERE users.id = 1",
    ).unwrap();

    rdb.execute_mut("DELETE FROM users WHERE users.id = 1").unwrap();
    assert_eq!(drain(&mut rdb).len(), 1);
}

#[test]
fn subscribe_fires_on_matching_update() {
    let mut rdb = setup();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv, users.name FROM users WHERE users.id = 1",
    ).unwrap();

    // Update rewrites as delete+insert — both sides match id=1, but after drain
    // snapshot we see the sub exactly once.
    rdb.execute_mut("UPDATE users SET name = 'Alicia' WHERE users.id = 1").unwrap();
    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1, "single sub deduped across delete+insert of one update");
}

#[test]
fn table_level_subscription_fires_on_any_mutation() {
    let mut rdb = setup();
    rdb.subscribe("SELECT REACTIVE(users.id) AS inv, users.id FROM users").unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'Zoe', 20)").unwrap();
    rdb.execute_mut("DELETE FROM users WHERE users.id = 2").unwrap();
    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1, "two notifies into one sub → one drain entry");
}

#[test]
fn unsubscribe_stops_firing() {
    let mut rdb = setup();
    let (handle, _id) = rdb.subscribe(
        "SELECT REACTIVE(users.id) AS inv, users.id FROM users",
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'Zoe', 20)").unwrap();
    assert!(!drain(&mut rdb).is_empty());

    rdb.unsubscribe(handle);
    assert_eq!(rdb.subscription_count(), 0);

    rdb.execute_mut("INSERT INTO users VALUES (11, 'Jo', 22)").unwrap();
    assert!(drain(&mut rdb).is_empty(), "no marks after unsubscribe");
}

#[test]
fn apply_zset_notifies() {
    use sql_engine::storage::{CellValue, ZSet};

    let mut rdb = setup();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 5) AS inv, users.name FROM users WHERE users.id = 5",
    ).unwrap();

    let mut zset = ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(5), CellValue::Str("Eve".into()), CellValue::I64(28)]);
    rdb.apply_zset(&zset).unwrap();
    assert_eq!(drain(&mut rdb).len(), 1);
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
    rdb.subscribe("SELECT REACTIVE(users.id) AS inv, users.id FROM users").unwrap();

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
    assert!(!drain(&mut rdb).is_empty());
}

#[test]
fn notify_all_marks_every_sub_with_empty_triggered() {
    let mut rdb = setup();
    let (_, id_a) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1",
    ).unwrap();
    let (_, id_b) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 2) AS inv FROM users WHERE users.id = 2",
    ).unwrap();

    rdb.notify_all();
    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 2);
    for n in &batch {
        assert!(n.triggered.is_empty(), "notify_all carries no precise triggered");
        assert!(n.sub_id == id_a || n.sub_id == id_b);
    }
}

// ── Wake / drain semantics ──────────────────────────────────────────

#[test]
fn wake_fires_once_per_empty_to_nonempty_transition() {
    let mut rdb = setup();
    rdb.subscribe("SELECT REACTIVE(users.id) AS inv FROM users").unwrap();

    let count = Rc::new(Cell::new(0u32));
    let c = count.clone();
    rdb.on_dirty(Box::new(move || c.set(c.get() + 1)));

    rdb.execute_mut("INSERT INTO users VALUES (10, 'X', 1)").unwrap();
    rdb.execute_mut("INSERT INTO users VALUES (11, 'Y', 2)").unwrap();
    assert_eq!(count.get(), 1, "wake fires exactly once while dirty stays non-empty");

    // Drain → dirty becomes empty again.
    while rdb.next_dirty().is_some() {}

    rdb.execute_mut("INSERT INTO users VALUES (12, 'Z', 3)").unwrap();
    assert_eq!(count.get(), 2, "post-drain mark re-arms the edge trigger");
}

#[test]
fn wake_not_fired_if_no_subs_affected() {
    let mut rdb = setup();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 999) AS inv FROM users WHERE users.id = 999",
    ).unwrap();

    let count = Rc::new(Cell::new(0u32));
    let c = count.clone();
    rdb.on_dirty(Box::new(move || c.set(c.get() + 1)));

    // Insert that doesn't match the subscription → dirty stays empty → no wake.
    rdb.execute_mut("INSERT INTO users VALUES (10, 'X', 1)").unwrap();
    assert_eq!(count.get(), 0);
}

#[test]
fn mutation_during_drain_survives_to_next_cycle() {
    let mut rdb = setup();
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 10) AS inv FROM users WHERE users.id = 10",
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'X', 1)").unwrap();
    // First drain cycle: take the snapshot, leave buffer empty.
    let first = rdb.next_dirty();
    assert!(first.is_some());

    // Between next_dirty calls a new mutation fires — goes into the now-empty
    // DirtySet, not into the snapshot being drained.
    rdb.execute_mut("UPDATE users SET name = 'Renamed' WHERE users.id = 10").unwrap();

    // Finish the first cycle: buffer is empty but dirty now has the new mark,
    // so next_dirty starts a *new* cycle and hands it back.
    let second = rdb.next_dirty();
    assert!(second.is_some(), "new mark from between-drain mutation surfaces");

    // Nothing left.
    assert!(rdb.next_dirty().is_none());
}

#[test]
fn idempotent_next_dirty_when_empty() {
    let mut rdb = setup();
    rdb.subscribe("SELECT REACTIVE(users.id) AS inv FROM users").unwrap();
    assert!(rdb.next_dirty().is_none());
    assert!(rdb.next_dirty().is_none());
}

#[test]
fn drain_deduplicates_within_a_cycle() {
    let mut rdb = setup();
    rdb.subscribe("SELECT REACTIVE(users.id) AS inv FROM users").unwrap();

    // Many notifies, same sub hit every time.
    for i in 0..20 {
        rdb.execute_mut(&format!("INSERT INTO users VALUES ({}, 'N', 0)", 100 + i)).unwrap();
    }

    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1, "N notifies on one sub → one drain entry");
}

#[test]
fn triggered_accumulates_across_notifies() {
    let mut rdb = setup();
    // Two REACTIVE conditions so we can observe accumulation across two inserts.
    rdb.subscribe(
        "SELECT REACTIVE(users.id = 10) AS a, REACTIVE(users.id = 20) AS b, users.name \
         FROM users WHERE users.id = 10 OR users.id = 20",
    ).unwrap();

    rdb.execute_mut("INSERT INTO users VALUES (10, 'X', 1)").unwrap();
    rdb.execute_mut("INSERT INTO users VALUES (20, 'Y', 1)").unwrap();

    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1);
    let mut trig = batch[0].triggered.clone();
    trig.sort();
    assert!(trig.len() >= 2, "accumulated triggered must hold both condition indices, got {trig:?}");
}

// ── Dedup and handle semantics ──────────────────────────────────────

#[test]
fn subscribe_twice_with_same_sql_shares_sub_id() {
    let mut rdb = setup();
    let sql = "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1";

    let (handle_a, id_a) = rdb.subscribe(sql).unwrap();
    let (handle_b, id_b) = rdb.subscribe(sql).unwrap();

    assert_eq!(id_a, id_b, "identical SQL must resolve to same SubscriptionId");
    assert_ne!(handle_a, handle_b, "each caller must get its own handle");
    assert_eq!(rdb.subscription_count(), 1, "one deduped subscription");
    assert_eq!(rdb.handle_count(), 2, "two caller handles");

    rdb.execute_mut("UPDATE users SET name = 'Alicia' WHERE users.id = 1").unwrap();
    let batch = drain(&mut rdb);
    assert_eq!(batch.len(), 1, "drain yields the shared sub exactly once");
    assert_eq!(batch[0].sub_id, id_a);
}

#[test]
fn subscribe_different_sql_gets_distinct_sub_ids() {
    let mut rdb = setup();

    let (_, id_a) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1",
    ).unwrap();
    let (_, id_b) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 2) AS inv FROM users WHERE users.id = 2",
    ).unwrap();

    assert_ne!(id_a, id_b);
    assert_eq!(rdb.subscription_count(), 2);
}

#[test]
fn unsubscribe_one_handle_keeps_sub_alive() {
    let mut rdb = setup();
    let sql = "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1";

    let (handle_a, _) = rdb.subscribe(sql).unwrap();
    let (_handle_b, _) = rdb.subscribe(sql).unwrap();

    assert!(rdb.unsubscribe(handle_a));
    assert_eq!(rdb.subscription_count(), 1, "still alive until last handle released");
    assert_eq!(rdb.handle_count(), 1);

    rdb.execute_mut("UPDATE users SET name = 'X' WHERE users.id = 1").unwrap();
    assert!(!drain(&mut rdb).is_empty(), "surviving handle still drives drain");
}

#[test]
fn last_handle_unsubscribe_tears_down_subscription() {
    let mut rdb = setup();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";

    let (handle_a, _) = rdb.subscribe(sql).unwrap();
    let (handle_b, _) = rdb.subscribe(sql).unwrap();

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
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    rdb.subscribe(sql).unwrap();

    assert!(!rdb.unsubscribe(SubscriptionHandle(9999)));
    assert_eq!(rdb.subscription_count(), 1, "unknown handle must not corrupt state");
}

#[test]
fn double_unsubscribe_returns_false_second_time() {
    let mut rdb = setup();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    let (handle, _) = rdb.subscribe(sql).unwrap();

    assert!(rdb.unsubscribe(handle));
    assert!(!rdb.unsubscribe(handle), "second release of same handle is a no-op");
}

#[test]
fn resubscribe_after_full_teardown_allocates_new_sub_id() {
    let mut rdb = setup();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    let (handle_1, id_1) = rdb.subscribe(sql).unwrap();
    assert!(rdb.unsubscribe(handle_1));
    assert_eq!(rdb.subscription_count(), 0);

    let (_, id_2) = rdb.subscribe(sql).unwrap();
    assert_ne!(id_1, id_2, "fresh subscribe after teardown must allocate a new id");
    let _: SubscriptionId = id_2; // type assertion
}

#[test]
fn unsubscribe_mid_drain_skips_stale_entries() {
    let mut rdb = setup();
    let (h1, id1) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 1) AS inv FROM users WHERE users.id = 1",
    ).unwrap();
    let (_h2, id2) = rdb.subscribe(
        "SELECT REACTIVE(users.id = 2) AS inv FROM users WHERE users.id = 2",
    ).unwrap();

    rdb.execute_mut("UPDATE users SET name = 'A' WHERE users.id = 1").unwrap();
    rdb.execute_mut("UPDATE users SET name = 'B' WHERE users.id = 2").unwrap();

    // Unsubscribe sub1 before starting the drain — its entry is in the dirty-set
    // but will be skipped.
    assert!(rdb.unsubscribe(h1));

    let batch = drain(&mut rdb);
    assert!(
        batch.iter().all(|n| n.sub_id != id1),
        "stale sub1 must not surface after unsubscribe"
    );
    assert!(
        batch.iter().any(|n| n.sub_id == id2),
        "sub2 must still surface"
    );
}
