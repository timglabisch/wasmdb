//! Runtime proof that the demo's projection actually works: `PostEntry`
//! commands append to `ledger_log`, and `BalanceFold` — driven by the
//! real `ProjectionEngine` at the derive chokepoint — maintains the
//! derived `balance` table. Covers the signed fold and partition
//! isolation, the demo-specific behavior. (The committed-frontier being
//! result-invariant is a framework property, exhaustively proven in
//! `tables-e2e`'s `projection_fold_incremental` and the engine kernel
//! tests; no need to re-derive it here.)

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::ProjectionEngine;
use projection_demo_domain::ledger::balance::Balance;
use projection_demo_domain::ledger::balance_fold::BalanceFold;
use projection_demo_domain::ledger::command::post_entry::PostEntry;
use projection_demo_domain::ledger::ledger_log::LedgerLog;
use sql_engine::storage::{CellValue, Uuid};
use sync::command::Command;

/// Deterministic command id from a small counter — no RNG in fixtures.
fn id(n: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    Uuid::from_bytes(bytes)
}

fn setup() -> (Database, ProjectionEngine) {
    let mut db = Database::new();
    db.register_table::<LedgerLog>().unwrap();
    db.register_table::<Balance>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(BalanceFold::default())).unwrap();
    (db, engine)
}

/// Append one entry (as the optimistic command does) and run the engine.
fn post(db: &mut Database, engine: &mut ProjectionEngine, n: u8, account: &str, cents: i64) {
    let cmd = PostEntry { id: id(n), account: account.to_string(), amount_cents: cents };
    let batch = cmd.execute_optimistic(db).expect("append");
    let mut host = DatabaseHost::new(db);
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
}

/// Read `(balance_cents, entries)` for an account from the derived table.
fn balance_of(db: &Database, account: &str) -> Option<(i64, i64)> {
    let t = db.table("balance").unwrap();
    t.row_ids().find_map(|r| {
        match t.get(r, 0) {
            CellValue::Str(a) if a == account => {
                let cents = match t.get(r, 1) {
                    CellValue::I64(v) => v,
                    other => panic!("balance_cents not I64: {other:?}"),
                };
                let entries = match t.get(r, 2) {
                    CellValue::I64(v) => v,
                    other => panic!("entries not I64: {other:?}"),
                };
                Some((cents, entries))
            }
            _ => None,
        }
    })
}

#[test]
fn fold_produces_signed_running_balance_per_account() {
    let (mut db, mut engine) = setup();

    post(&mut db, &mut engine, 1, "alice", 5000); // deposit €50.00
    post(&mut db, &mut engine, 2, "alice", -1250); // withdraw €12.50
    post(&mut db, &mut engine, 3, "bob", 10000); // deposit €100.00

    // Signed fold, and alice/bob are independent partitions.
    assert_eq!(balance_of(&db, "alice"), Some((3750, 2)));
    assert_eq!(balance_of(&db, "bob"), Some((10000, 1)));
}

#[test]
fn later_entries_update_the_existing_balance_row() {
    let (mut db, mut engine) = setup();
    post(&mut db, &mut engine, 1, "carol", 4000);
    assert_eq!(balance_of(&db, "carol"), Some((4000, 1)));

    // A second entry folds onto the first — one row in `balance`, not two.
    post(&mut db, &mut engine, 2, "carol", -1000);
    assert_eq!(balance_of(&db, "carol"), Some((3000, 2)));
    let rows = db.table("balance").unwrap().row_ids().count();
    assert_eq!(rows, 1, "balance stays keyed by account");
}
