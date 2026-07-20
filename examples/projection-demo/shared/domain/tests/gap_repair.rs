//! Runtime proof of commit-chain-v2 gap-repair (design §11.4), end to end
//! on the host: the demo server's [`ServerLog`] holds a committed chain the
//! client never fetched, the client's own confirmed row points at an
//! unknown `server_parent_id`, and the real repair core
//! (`sync_client::repair::missing_parents`) drives a backward refetch until
//! the chain is contiguous from ROOT and `BalanceFold` folds the full
//! history.
//!
//! This is the host stand-in for the wasm `repair_chain` loop: over the
//! network it POSTs the missing PKs and applies the `FetchRowsResponse`;
//! here it calls `ServerLog::fetch` and applies directly. Same logic, same
//! convergence, no browser.

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::ProjectionEngine;
use projection_demo_domain::ledger::balance::Balance;
use projection_demo_domain::ledger::balance_fold::BalanceFold;
use projection_demo_domain::ledger::ledger_log::{EntryPosted, LedgerLog};
use projection_demo_domain::ServerLog;
use rpc_command::payload_json;
use sql_engine::storage::{CellValue, Uuid, ZSet};
use sql_engine::DbTable;
use sync_client::repair::{missing_parents, unknown_ids};

const ROOT: Uuid = Uuid([0u8; 16]);

/// Server/carol-seed id (`0xca…`) — the "other writer" the client backfills.
fn sid(n: u8) -> Uuid {
    let mut b = [0u8; 16];
    b[0] = 0xca;
    b[15] = n;
    Uuid(b)
}

/// A client-generated command id.
fn cid(n: u8) -> Uuid {
    let mut b = [0u8; 16];
    b[15] = n;
    Uuid(b)
}

fn setup() -> (Database, ProjectionEngine) {
    let mut db = Database::new();
    db.register_table::<LedgerLog>().unwrap();
    db.register_table::<Balance>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(BalanceFold::default())).unwrap();
    (db, engine)
}

/// Insert every row of `zset` and run one derive pass — the host analog of
/// the reactive database's `apply_zset` (write, then re-fold).
fn apply_rows(db: &mut Database, engine: &mut ProjectionEngine, zset: &ZSet) {
    for e in &zset.entries {
        db.insert(&e.table, &e.row).expect("insert log row");
    }
    let mut host = DatabaseHost::new(db);
    let outcome = engine.derive(zset, &mut host);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
}

fn balance_of(db: &Database, account: &str) -> Option<(i64, i64)> {
    let t = db.table("balance").unwrap();
    t.row_ids().find_map(|r| match t.get(r, 0) {
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
    })
}

/// A committed `ledger_log` row as the client holds it after confirm.
fn committed_row(command_id: Uuid, client_parent: Uuid, server_parent: Uuid, cents: i64) -> ZSet {
    let cells = LedgerLog {
        command_id,
        account: "carol".into(),
        client_parent_id: client_parent,
        server_parent_id: Some(server_parent),
        payload: payload_json(&EntryPosted { amount_cents: cents }).unwrap(),
    }
    .into_cells();
    let mut z = ZSet::new();
    z.insert(LedgerLog::TABLE.into(), cells);
    z
}

#[test]
fn gap_repair_backfills_ancestors_and_the_fold_recovers() {
    // Server: carol already has a committed chain (ROOT → sid1 → sid2) that
    // no client holds.
    let mut server = ServerLog::new();
    server.seed_chain("carol", &[(sid(1), 3000), (sid(2), -500)]);

    // Client: fresh DB. It holds ONE carol row — already confirmed, but the
    // server linked it after sid2, a head the client never fetched. Its
    // `client_parent_id` is still the optimistic ROOT (it assumed it was
    // first): committed, with drift, and dangling off a missing ancestor.
    let (mut db, mut engine) = setup();
    apply_rows(&mut db, &mut engine, &committed_row(cid(100), ROOT, sid(2), 1000));

    // Before repair the committed chain is broken: cid100 is unreachable
    // from ROOT, so the balance is wrong — only the client's own entry.
    assert_eq!(balance_of(&db, "carol"), Some((1000, 1)));
    assert_eq!(missing_parents(&db, "ledger_log"), vec![sid(2)]);

    // Backward refetch — exactly what `repair_chain` drives over the wire.
    let mut rounds = 0;
    loop {
        let missing = missing_parents(&db, "ledger_log");
        if missing.is_empty() {
            break;
        }
        let fetched = server.fetch(&missing);
        assert!(!fetched.is_empty(), "server holds the gap ancestors");
        apply_rows(&mut db, &mut engine, &fetched);
        rounds += 1;
        assert!(rounds <= 8, "repair must converge");
    }

    // After repair: ROOT → sid1 → sid2 → cid100 is contiguous and the fold
    // includes carol's full history (3000 − 500 + 1000 over 3 entries).
    assert_eq!(missing_parents(&db, "ledger_log"), Vec::<Uuid>::new());
    assert_eq!(balance_of(&db, "carol"), Some((3500, 3)));
    assert_eq!(rounds, 2, "backward walk: fetch sid2, then sid1");
}

/// Mirror the demo server's opening seed: alice/bob ordinary chains plus
/// carol's "other writer" history.
fn seed_all(server: &mut ServerLog) {
    let id = |n: u8| {
        let mut b = [0u8; 16];
        b[15] = n;
        Uuid(b)
    };
    server.seed_chain("alice", &[(id(0xa1), 5000), (id(0xa2), -1250)]);
    server.seed_chain("bob", &[(id(0xb1), 10000), (id(0xb2), -2000), (id(0xb3), 750)]);
    server.seed_chain("carol", &[(sid(1), 3000), (sid(2), -500)]);
}

/// Host stand-in for the wasm `bootstrap` loop: fetch the heads this client
/// doesn't hold, apply them, then walk every chain to ROOT. Returns the
/// number of backward-walk rounds.
fn bootstrap(db: &mut Database, engine: &mut ProjectionEngine, server: &ServerLog) -> usize {
    let fresh = unknown_ids(db, "ledger_log", &server.heads());
    if !fresh.is_empty() {
        apply_rows(db, engine, &server.fetch(&fresh));
    }
    let mut rounds = 0;
    loop {
        let missing = missing_parents(db, "ledger_log");
        if missing.is_empty() {
            break;
        }
        apply_rows(db, engine, &server.fetch(&missing));
        rounds += 1;
        assert!(rounds <= 16, "bootstrap must converge");
    }
    rounds
}

#[test]
fn bootstrap_from_empty_reconstructs_every_balance() {
    // The server owns all opening state; the client's wasm memory is empty
    // (a fresh load / new tab). Bootstrap must rebuild every balance from
    // the server alone — this is what makes a page reload non-destructive.
    let mut server = ServerLog::new();
    seed_all(&mut server);

    let (mut db, mut engine) = setup();
    assert_eq!(missing_parents(&db, "ledger_log"), Vec::<Uuid>::new());
    assert_eq!(server.heads().len(), 3, "one head per partition");

    bootstrap(&mut db, &mut engine, &server);

    // Whole committed history reconstructed from the server, every fold correct.
    assert_eq!(missing_parents(&db, "ledger_log"), Vec::<Uuid>::new());
    assert_eq!(balance_of(&db, "alice"), Some((3750, 2)));
    assert_eq!(balance_of(&db, "bob"), Some((8750, 3)));
    assert_eq!(balance_of(&db, "carol"), Some((2500, 2)));
}

#[test]
fn foreign_write_then_rebootstrap_pulls_new_rows_without_double_counting() {
    let mut server = ServerLog::new();
    seed_all(&mut server);
    let (mut db, mut engine) = setup();
    bootstrap(&mut db, &mut engine, &server);
    assert_eq!(balance_of(&db, "carol"), Some((2500, 2)));

    // Another writer advances carol out-of-band (the `/foreign-write` button).
    let injected = server.foreign_write("carol", 3);
    assert_eq!(injected.len(), 3);

    // Re-bootstrap: only carol's new head is unknown. alice/bob heads are
    // unchanged and already held — `unknown_ids` filters them out, so we
    // never re-apply and double-count them.
    let fresh = unknown_ids(&db, "ledger_log", &server.heads());
    assert_eq!(
        fresh,
        vec![*injected.last().unwrap()],
        "only the fresh carol head is fetched"
    );

    let rounds = bootstrap(&mut db, &mut engine, &server);

    // Burst pattern [1500, −400, 900] nets +2000 over 3 entries; alice/bob
    // untouched. The walk fetches the two older burst rows after the head.
    assert_eq!(balance_of(&db, "carol"), Some((4500, 5)));
    assert_eq!(balance_of(&db, "alice"), Some((3750, 2)));
    assert_eq!(balance_of(&db, "bob"), Some((8750, 3)));
    assert_eq!(rounds, 2, "backward walk: fetch the 2 older burst rows");
}

#[test]
fn seeded_chain_is_fetchable_by_pk_and_links_to_root() {
    let mut server = ServerLog::new();
    server.seed_chain("carol", &[(sid(1), 3000), (sid(2), -500)]);

    // Unknown ids are simply absent; known ones come back as +1 rows.
    assert!(server.fetch(&[cid(200)]).is_empty());
    let head = server.fetch(&[sid(2)]);
    assert_eq!(head.entries.len(), 1);

    // The head row links back to sid1 via `server_parent_id`; sid1 links to
    // ROOT — a chain the client can walk to the root.
    let sp_idx = LedgerLog::schema()
        .columns
        .iter()
        .position(|c| c.name == "server_parent_id")
        .unwrap();
    assert_eq!(head.entries[0].row[sp_idx], CellValue::from(sid(1)));
    let first = server.fetch(&[sid(1)]);
    assert_eq!(first.entries[0].row[sp_idx], CellValue::from(ROOT));
}
