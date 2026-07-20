//! Hook tests: projections wired into `ReactiveDatabase`.
//!
//! Verifies the design-doc guarantees at the reactive layer: same-batch
//! atomicity (one notification cycle carries source AND derived changes),
//! the reject flow (invert of source rows re-derives), the ownership
//! guard on `apply_zset`, derivation on the SQL mutation path, initial
//! materialization on install, and reset-on-replace.

use database::{Database, DbError};
use database_projection::{
    Inputs, PartitionedSource, OutputRow, Projection, ProjectionEngine, ProjectionSpec, ReadCtx,
};
use database_reactive::{ProjectionEvent, ReactiveDatabase};
use sql_engine::storage::{CellValue, ZSet};

fn i64v(v: i64) -> CellValue {
    CellValue::I64(v)
}

const DDL: &str = "CREATE TABLE events (
        command_id I64 NOT NULL PRIMARY KEY,
        doc_id I64 NOT NULL,
        seq I64 NOT NULL,
        val I64 NOT NULL,
        INDEX idx_doc (doc_id)
    );
    CREATE TABLE totals (
        doc_id I64 NOT NULL PRIMARY KEY,
        amount I64 NOT NULL
    );";

/// events(command_id, doc_id, seq, val) partitioned by doc_id → totals(doc_id, amount).
struct Totals;

impl Projection for Totals {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "totals".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 1 }],
            reads: vec![],
            outputs: vec!["totals".into()],
        }
    }

    fn project(
        &self,
        partition: &CellValue,
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
    ) -> Result<Vec<OutputRow>, String> {
        let mut sum = 0i64;
        for row in inputs.rows("events") {
            let CellValue::I64(v) = row[3] else {
                return Err("val must be I64".into());
            };
            sum += v;
        }
        Ok(vec![("totals".to_string(), vec![partition.clone(), i64v(sum)])])
    }
}

fn setup() -> ReactiveDatabase {
    let mut db = Database::new();
    db.execute_ddl(DDL).unwrap();
    let mut rdb = ReactiveDatabase::from_database(db);
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    rdb.install_projections(engine);
    rdb
}

fn event_zset(cmd: i64, doc: i64, seq: i64, val: i64) -> ZSet {
    let mut z = ZSet::new();
    z.insert("events".into(), vec![i64v(cmd), i64v(doc), i64v(seq), i64v(val)]);
    z
}

fn totals_rows(rdb: &ReactiveDatabase) -> Vec<Vec<CellValue>> {
    let t = rdb.db().table("totals").unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<Vec<CellValue>> = t
        .row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect();
    rows.sort();
    rows
}

#[test]
fn apply_zset_derives_and_notifies_in_one_cycle() {
    let mut rdb = setup();
    // Subscribe ONLY to the derived table — the external batch touches
    // `events` alone, so a mark on this subscription proves the combined
    // (source + derived) zset reached the subscribers.
    let (_handle, sub_id) = rdb
        .subscribe("SELECT REACTIVE(totals.doc_id) AS inv, totals.amount FROM totals")
        .unwrap();

    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();

    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(10)]]);
    assert!(rdb.take_projection_events().is_empty());

    // Exactly one drain cycle, containing the totals subscription once.
    let n = rdb.next_dirty().expect("totals subscription must be dirty");
    assert_eq!(n.sub_id, sub_id);
    assert!(rdb.next_dirty().is_none(), "one consistent cycle, no second wave");
}

#[test]
fn reject_invert_rederives_to_base() {
    let mut rdb = setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    rdb.apply_zset(&event_zset(101, 1, 1, 20)).unwrap();
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(30)]]);

    // Reject: the sync layer inverts the pending +row. The projection must
    // re-derive to the base state in the same batch.
    let mut invert = ZSet::new();
    invert.delete("events".into(), vec![i64v(101), i64v(1), i64v(1), i64v(20)]);
    rdb.apply_zset(&invert).unwrap();

    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(10)]]);

    // Key death: invert the remaining event → output fully cleared.
    let mut invert = ZSet::new();
    invert.delete("events".into(), vec![i64v(100), i64v(1), i64v(0), i64v(10)]);
    rdb.apply_zset(&invert).unwrap();
    assert!(totals_rows(&rdb).is_empty());
    assert!(rdb.take_projection_events().is_empty());
}

#[test]
fn external_write_to_owned_table_is_rejected_before_apply() {
    let mut rdb = setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();

    let mut sneaky = ZSet::new();
    sneaky.insert("totals".into(), vec![i64v(1), i64v(999)]);
    let err = rdb.apply_zset(&sneaky).unwrap_err();
    match err {
        DbError::OwnedByProjection { table, owner } => {
            assert_eq!(table, "totals");
            assert_eq!(owner, "totals");
        }
        other => panic!("expected OwnedByProjection, got {other:?}"),
    }
    // Nothing was applied.
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(10)]]);
}

#[test]
fn sql_mutation_path_derives_too() {
    let mut rdb = setup();
    rdb.execute_mut(
        "INSERT INTO events (command_id, doc_id, seq, val) VALUES (100, 1, 0, 42)",
    )
    .unwrap();
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(42)]]);
}

#[test]
fn install_materializes_existing_source_rows() {
    let mut db = Database::new();
    db.execute_ddl(DDL).unwrap();
    db.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    db.apply_zset(&event_zset(101, 2, 0, 5)).unwrap();

    let mut rdb = ReactiveDatabase::from_database(db);
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    rdb.install_projections(engine);

    assert_eq!(
        totals_rows(&rdb),
        vec![vec![i64v(1), i64v(10)], vec![i64v(2), i64v(5)]]
    );
    assert!(rdb.take_projection_events().is_empty());
}

/// Poisoned projection for the failure tests: val == 13 makes project()
/// fail with a stable message.
struct Superstitious;

impl Projection for Superstitious {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "totals".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 1 }],
            reads: vec![],
            outputs: vec!["totals".into()],
        }
    }
    fn project(
        &self,
        partition: &CellValue,
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
    ) -> Result<Vec<OutputRow>, String> {
        let mut sum = 0i64;
        for row in inputs.rows("events") {
            let CellValue::I64(v) = row[3] else {
                return Err("val must be I64".into());
            };
            if v == 13 {
                return Err("unlucky".into());
            }
            sum += v;
        }
        Ok(vec![("totals".to_string(), vec![partition.clone(), i64v(sum)])])
    }
}

fn superstitious_rdb() -> ReactiveDatabase {
    let mut db = Database::new();
    db.execute_ddl(DDL).unwrap();
    let mut rdb = ReactiveDatabase::from_database(db);
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Superstitious)).unwrap();
    rdb.install_projections(engine);
    rdb
}

/// A failed partition pins a failure; a later successful re-derivation
/// of the SAME partition surfaces as a recovery — and only then.
#[test]
fn failure_then_success_yields_recovery() {
    let mut rdb = superstitious_rdb();

    // Healthy derive: no events (never-failed partitions are not tracked as
    // recovered).
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    assert!(rdb.take_projection_events().is_empty());

    // Poison: partition 1 fails, previous output stays.
    rdb.apply_zset(&event_zset(101, 1, 1, 13)).unwrap();
    let events = rdb.take_projection_events();
    let [ProjectionEvent::Failed(f)] = events.as_slice() else {
        panic!("expected exactly one Failed event, got {events:?}");
    };
    assert_eq!(f.projection, "totals");
    assert_eq!(f.partition.as_deref(), Some("1"));
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(10)]]);

    // Repeat failure with an unchanged message records no new event —
    // the pin is already in place, and native embedders that never drain
    // must not accumulate one event per pass.
    rdb.apply_zset(&event_zset(102, 1, 2, 13)).unwrap();
    assert!(rdb.take_projection_events().is_empty());

    // Cure: remove the poisoned rows → partition re-derives → recovery reported.
    let mut invert = ZSet::new();
    invert.delete("events".into(), vec![i64v(101), i64v(1), i64v(1), i64v(13)]);
    invert.delete("events".into(), vec![i64v(102), i64v(1), i64v(2), i64v(13)]);
    rdb.apply_zset(&invert).unwrap();
    let events = rdb.take_projection_events();
    let [ProjectionEvent::Recovered { projection, partition }] = events.as_slice() else {
        panic!("expected exactly one Recovered event, got {events:?}");
    };
    assert_eq!(projection, "totals");
    assert_eq!(partition, "1");
    // Recovery is reported once, not on every later success.
    rdb.apply_zset(&event_zset(103, 1, 1, 5)).unwrap();
    assert!(rdb.take_projection_events().is_empty());
}

/// Multiple derive passes between two drains: the drained events keep
/// derive order, so a fail-then-cure sequence reads Failed → Recovered
/// and the consumer's last-event-wins application lands on healthy.
/// (This is the reconcile case: invert + apply run back to back before
/// the wasm layer drains once.)
#[test]
fn events_keep_order_across_passes_between_drains() {
    let mut rdb = superstitious_rdb();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    assert!(rdb.take_projection_events().is_empty());

    // Pass 1: poison. Pass 2: cure. NO drain in between.
    rdb.apply_zset(&event_zset(101, 1, 1, 13)).unwrap();
    let mut invert = ZSet::new();
    invert.delete("events".into(), vec![i64v(101), i64v(1), i64v(1), i64v(13)]);
    rdb.apply_zset(&invert).unwrap();

    let events = rdb.take_projection_events();
    let [ProjectionEvent::Failed(f), ProjectionEvent::Recovered { projection, partition }] =
        events.as_slice()
    else {
        panic!("expected Failed then Recovered, got {events:?}");
    };
    assert_eq!(f.partition.as_deref(), Some("1"));
    assert_eq!(projection, "totals");
    assert_eq!(partition, "1");
}

#[test]
fn replace_data_resets_and_rederives() {
    let mut rdb = setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(10)]]);

    // Snapshot with entirely different contents — including garbage in the
    // owned table that must be swept away.
    let mut other = Database::new();
    other.execute_ddl(DDL).unwrap();
    other.apply_zset(&event_zset(500, 7, 0, 3)).unwrap();
    let mut garbage = ZSet::new();
    garbage.insert("totals".into(), vec![i64v(9), i64v(9)]);
    other.apply_zset(&garbage).unwrap();

    rdb.replace_data(&other);

    assert_eq!(totals_rows(&rdb), vec![vec![i64v(7), i64v(3)]]);
    assert!(rdb.take_projection_events().is_empty());
}
