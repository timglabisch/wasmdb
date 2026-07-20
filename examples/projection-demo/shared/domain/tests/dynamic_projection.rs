//! Host proof of the demand-driven `activity` template (design §12) against
//! the real `ReactiveDatabase`: activate materializes exactly the named
//! account (the 10k-scenario in miniature — `balance` still materializes
//! everything by data presence, the contrast is the point), source rows
//! keep the instance in sync, deactivate retracts, and a
//! `replace_data`-rebuild re-materializes active instances.

use database::Database;
use database_projection::ProjectionEngine;
use database_reactive::ReactiveDatabase;
use projection_demo_domain::ledger::account_activity::AccountActivity;
use projection_demo_domain::ledger::activity_fold::{ActivityFold, ACTIVITY_PROJECTION_ID};
use projection_demo_domain::ledger::balance::Balance;
use projection_demo_domain::ledger::balance_fold::BalanceFold;
use projection_demo_domain::ledger::ledger_log::{EntryPosted, LedgerLog};
use projection_demo_domain::ServerLog;
use rpc_command::payload_json;
use sql_engine::storage::{CellValue, Uuid, ZSet};
use sql_engine::DbTable;
use tables::ROOT_PARENT;

fn cid(n: u8) -> Uuid {
    let mut b = [0u8; 16];
    b[15] = n;
    Uuid(b)
}

/// A committed `ledger_log` row (server-linked, no drift).
fn committed_row(account: &str, command_id: Uuid, parent: Uuid, cents: i64) -> ZSet {
    let cells = LedgerLog {
        command_id,
        account: account.into(),
        client_parent_id: parent,
        server_parent_id: Some(parent),
        payload: payload_json(&EntryPosted { amount_cents: cents }).unwrap(),
    }
    .into_cells();
    let mut z = ZSet::new();
    z.insert(LedgerLog::TABLE.into(), cells);
    z
}

fn fresh_db() -> Database {
    let mut db = Database::new();
    db.register_table::<LedgerLog>().unwrap();
    db.register_table::<Balance>().unwrap();
    db.register_table::<AccountActivity>().unwrap();
    db
}

fn setup() -> ReactiveDatabase {
    let mut rdb = ReactiveDatabase::from_database(fresh_db());
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(BalanceFold::default())).unwrap();
    engine.register_dynamic(Box::new(ActivityFold::default())).unwrap();
    rdb.install_projections(engine);
    rdb
}

/// Seed three accounts: alice (+5000, −1250), bob (+10000), carol
/// (+3000, −500) — each a committed chain from ROOT.
fn seed(rdb: &mut ReactiveDatabase) {
    rdb.apply_zset(&committed_row("alice", cid(1), ROOT_PARENT, 5000)).unwrap();
    rdb.apply_zset(&committed_row("alice", cid(2), cid(1), -1250)).unwrap();
    rdb.apply_zset(&committed_row("bob", cid(3), ROOT_PARENT, 10000)).unwrap();
    rdb.apply_zset(&committed_row("carol", cid(4), ROOT_PARENT, 3000)).unwrap();
    rdb.apply_zset(&committed_row("carol", cid(5), cid(4), -500)).unwrap();
}

fn carol_name() -> Vec<CellValue> {
    vec![CellValue::Str("account".into()), CellValue::Str("carol".into())]
}

fn activity_rows(rdb: &ReactiveDatabase) -> Vec<Vec<CellValue>> {
    let t = rdb.db().table(AccountActivity::TABLE).unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<Vec<CellValue>> = t
        .row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect();
    rows.sort();
    rows
}

fn activity_row(account: &str, deposits: i64, withdrawals: i64, largest: i64) -> Vec<CellValue> {
    vec![
        CellValue::Str(account.into()),
        CellValue::I64(deposits),
        CellValue::I64(withdrawals),
        CellValue::I64(largest),
    ]
}

#[test]
fn activate_materializes_only_the_named_account() {
    let mut rdb = setup();
    seed(&mut rdb);

    // Static path (data presence): every seeded account has a balance row.
    assert_eq!(rdb.db().table(Balance::TABLE).unwrap().row_ids().count(), 3);
    // Dynamic path (demand): nothing before activate.
    assert!(activity_rows(&rdb).is_empty());

    rdb.activate_projection(ACTIVITY_PROJECTION_ID, carol_name()).unwrap();

    // Exactly carol — alice/bob are NOT materialized.
    assert_eq!(activity_rows(&rdb), vec![activity_row("carol", 1, 1, 3000)]);
    assert!(rdb.take_projection_events().is_empty());
}

#[test]
fn foreign_rows_advance_the_active_instance() {
    let mut rdb = setup();
    seed(&mut rdb);
    rdb.activate_projection(ACTIVITY_PROJECTION_ID, carol_name()).unwrap();

    // Another writer advances carol out-of-band; applying the fetched rows
    // (the bootstrap/repair path) must flow into the active instance.
    let mut server = ServerLog::new();
    let injected = server.foreign_write("carol", 3);
    let fetched = server.fetch(&injected);
    rdb.apply_zset(&fetched).unwrap();

    // Burst pattern [1500, −400, 900]: deposits 1+2, withdrawals 1+1,
    // largest stays 3000.
    assert_eq!(activity_rows(&rdb), vec![activity_row("carol", 3, 2, 3000)]);
}

#[test]
fn deactivate_empties_the_table() {
    let mut rdb = setup();
    seed(&mut rdb);
    rdb.activate_projection(ACTIVITY_PROJECTION_ID, carol_name()).unwrap();
    assert_eq!(activity_rows(&rdb).len(), 1);

    rdb.deactivate_projection(ACTIVITY_PROJECTION_ID, &carol_name()).unwrap();
    assert!(activity_rows(&rdb).is_empty());

    // The source data is untouched — only the demand materialization died.
    assert_eq!(rdb.db().table(Balance::TABLE).unwrap().row_ids().count(), 3);
}

#[test]
fn replace_data_rebuild_rematerializes_the_instance() {
    let mut rdb = setup();
    seed(&mut rdb);
    rdb.activate_projection(ACTIVITY_PROJECTION_ID, carol_name()).unwrap();

    // Wholesale rebuild (the sync-client reconcile path): carol's history
    // is different in the replacement snapshot.
    let mut other = fresh_db();
    other
        .apply_zset(&committed_row("carol", cid(9), ROOT_PARENT, 7000))
        .unwrap();
    rdb.replace_data(&other);
    rdb.notify_all();

    // The activation survived; the render reflects the new source data.
    assert_eq!(activity_rows(&rdb), vec![activity_row("carol", 1, 0, 7000)]);
    assert!(rdb.take_projection_events().is_empty());

    // And it is still live.
    rdb.apply_zset(&committed_row("carol", cid(10), cid(9), -200)).unwrap();
    assert_eq!(activity_rows(&rdb), vec![activity_row("carol", 1, 1, 7000)]);
}
