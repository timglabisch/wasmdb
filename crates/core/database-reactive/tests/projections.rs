//! Hook tests: projections wired into `ReactiveDatabase`.
//!
//! Verifies the design-doc guarantees at the reactive layer: same-batch
//! atomicity (one notification cycle carries source AND derived changes),
//! the reject flow (invert of source rows re-derives), the ownership
//! guard on `apply_zset`, derivation on the SQL mutation path, initial
//! materialization on install, and reset-on-replace.

use database::{Database, DbError};
use database_projection::{
    FoldCache, Inputs, OutputRow, PartitionedSource, Projection, ProjectionEngine,
    ProjectionSpec, ReadCtx,
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
        _cache: &mut FoldCache,
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
        _cache: &mut FoldCache,
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

// ── Dynamic instances (§12): activate/deactivate through the reactive DB ─

use database_projection::{DynamicProjection, DynamicSpec, FootprintSource};

const DDL_DYN: &str = "CREATE TABLE events (
        command_id I64 NOT NULL PRIMARY KEY,
        doc_id I64 NOT NULL,
        seq I64 NOT NULL,
        val I64 NOT NULL,
        INDEX idx_doc (doc_id)
    );
    CREATE TABLE totals (
        doc_id I64 NOT NULL PRIMARY KEY,
        amount I64 NOT NULL
    );
    CREATE TABLE doc_activity (
        doc_id I64 NOT NULL PRIMARY KEY,
        entries I64 NOT NULL,
        amount I64 NOT NULL
    );";

/// events(command_id, doc_id, seq, val) → doc_activity(doc_id, entries,
/// amount), demand-activated per document. Name = [Str("doc"), I64(<id>)]:
/// component 0 is a namespace discriminator, component 1 binds doc_id.
struct DocActivity;

impl DynamicProjection for DocActivity {
    fn spec(&self) -> DynamicSpec {
        DynamicSpec {
            id: "doc_activity".into(),
            sources: vec![FootprintSource { table: "events".into(), bind: vec![(1, 1)] }],
            reads: vec![],
            outputs: vec!["doc_activity".into()],
        }
    }

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let rows = inputs.rows("events");
        let mut sum = 0i64;
        for row in rows {
            let CellValue::I64(v) = row[3] else {
                return Err("val must be I64".into());
            };
            sum += v;
        }
        Ok(vec![(
            "doc_activity".to_string(),
            vec![name[1].clone(), i64v(rows.len() as i64), i64v(sum)],
        )])
    }
}

fn dynamic_setup() -> ReactiveDatabase {
    let mut db = Database::new();
    db.execute_ddl(DDL_DYN).unwrap();
    let mut rdb = ReactiveDatabase::from_database(db);
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    engine.register_dynamic(Box::new(DocActivity)).unwrap();
    rdb.install_projections(engine);
    rdb
}

fn doc_name(doc: i64) -> Vec<CellValue> {
    vec![CellValue::Str("doc".into()), i64v(doc)]
}

fn activity_rows(rdb: &ReactiveDatabase) -> Vec<Vec<CellValue>> {
    let t = rdb.db().table("doc_activity").unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<Vec<CellValue>> = t
        .row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect();
    rows.sort();
    rows
}

#[test]
fn activate_materializes_and_notifies_output_subscribers() {
    let mut rdb = dynamic_setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    rdb.apply_zset(&event_zset(101, 2, 0, 5)).unwrap();

    let (_handle, sub_id) = rdb
        .subscribe("SELECT REACTIVE(doc_activity.doc_id) AS inv, doc_activity.amount FROM doc_activity")
        .unwrap();

    rdb.activate_projection("doc_activity", doc_name(1)).unwrap();

    // Exactly the named instance materialized; doc 2 stays out.
    assert_eq!(activity_rows(&rdb), vec![vec![i64v(1), i64v(1), i64v(10)]]);
    assert!(rdb.take_projection_events().is_empty());

    // The activation delta reached the output-table subscription.
    let n = rdb.next_dirty().expect("subscription must be dirty after activate");
    assert_eq!(n.sub_id, sub_id);
    assert!(rdb.next_dirty().is_none());
}

#[test]
fn source_mutation_updates_instance_in_one_notification() {
    let mut rdb = dynamic_setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    rdb.activate_projection("doc_activity", doc_name(1)).unwrap();
    while rdb.next_dirty().is_some() {}

    let (_handle, sub_id) = rdb
        .subscribe("SELECT REACTIVE(doc_activity.doc_id) AS inv, doc_activity.amount FROM doc_activity")
        .unwrap();

    rdb.execute_mut("INSERT INTO events (command_id, doc_id, seq, val) VALUES (101, 1, 1, 32)")
        .unwrap();

    // Source and derived instance are consistent within ONE cycle.
    assert_eq!(activity_rows(&rdb), vec![vec![i64v(1), i64v(2), i64v(42)]]);
    let n = rdb.next_dirty().expect("one notification");
    assert_eq!(n.sub_id, sub_id);
    assert!(rdb.next_dirty().is_none(), "no second wave");

    // Static projection derived in the same pass, untouched by dynamics.
    assert_eq!(totals_rows(&rdb), vec![vec![i64v(1), i64v(42)]]);
}

#[test]
fn deactivate_retracts_and_notifies() {
    let mut rdb = dynamic_setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    rdb.activate_projection("doc_activity", doc_name(1)).unwrap();
    while rdb.next_dirty().is_some() {}

    let (_handle, sub_id) = rdb
        .subscribe("SELECT REACTIVE(doc_activity.doc_id) AS inv, doc_activity.amount FROM doc_activity")
        .unwrap();

    rdb.deactivate_projection("doc_activity", &doc_name(1)).unwrap();

    assert!(activity_rows(&rdb).is_empty());
    let n = rdb.next_dirty().expect("retraction must notify");
    assert_eq!(n.sub_id, sub_id);

    // Unknown instance is an embedder error.
    assert!(rdb.deactivate_projection("doc_activity", &doc_name(1)).is_err());
}

#[test]
fn replace_data_rematerializes_active_instances() {
    let mut rdb = dynamic_setup();
    rdb.apply_zset(&event_zset(100, 1, 0, 10)).unwrap();
    rdb.activate_projection("doc_activity", doc_name(1)).unwrap();
    assert_eq!(activity_rows(&rdb), vec![vec![i64v(1), i64v(1), i64v(10)]]);

    // Wholesale replacement with different contents for doc 1.
    let mut other = Database::new();
    other.execute_ddl(DDL_DYN).unwrap();
    other.apply_zset(&event_zset(500, 1, 0, 7)).unwrap();
    other.apply_zset(&event_zset(501, 1, 1, 3)).unwrap();
    rdb.replace_data(&other);
    rdb.notify_all();

    // The activation survived the swap; the render reflects the new data.
    assert_eq!(activity_rows(&rdb), vec![vec![i64v(1), i64v(2), i64v(10)]]);
    assert!(rdb.take_projection_events().is_empty());

    // And it is still live: a new event keeps flowing in.
    rdb.execute_mut("INSERT INTO events (command_id, doc_id, seq, val) VALUES (502, 1, 2, 5)")
        .unwrap();
    assert_eq!(activity_rows(&rdb), vec![vec![i64v(1), i64v(3), i64v(15)]]);
}
