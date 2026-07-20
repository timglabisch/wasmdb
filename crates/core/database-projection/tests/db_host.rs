//! Adapter tests: the engine against a real `Database` through
//! `DatabaseHost` — PK-upsert replacement pairs, index-backed key reads,
//! tolerant full-row deletes.

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::{
    FoldCache, Inputs, OutputRow, PartitionedSource, Projection, ProjectionEngine,
    ProjectionSpec, ReadCtx,
};
use sql_engine::storage::{CellValue, ZSet};

fn i64v(v: i64) -> CellValue {
    CellValue::I64(v)
}

fn setup_db() -> Database {
    let mut db = Database::new();
    db.execute_ddl(
        "CREATE TABLE events (
            command_id I64 NOT NULL PRIMARY KEY,
            doc_id I64 NOT NULL,
            seq I64 NOT NULL,
            val I64 NOT NULL,
            INDEX idx_doc (doc_id)
        );
        CREATE TABLE totals (
            doc_id I64 NOT NULL PRIMARY KEY,
            amount I64 NOT NULL
        );",
    )
    .unwrap();
    db
}

/// events(command_id, doc_id, seq, val) keyed by doc_id → totals(doc_id, sum).
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
        key: &CellValue,
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
        Ok(vec![("totals".to_string(), vec![key.clone(), i64v(sum)])])
    }
}

fn event_row(cmd: i64, doc: i64, seq: i64, val: i64) -> Vec<CellValue> {
    vec![i64v(cmd), i64v(doc), i64v(seq), i64v(val)]
}

fn table_rows(db: &Database, name: &str) -> Vec<Vec<CellValue>> {
    let t = db.table(name).unwrap();
    let ncols = t.schema.columns.len();
    t.row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect()
}

#[test]
fn pk_upsert_replacement_pair_updates_in_place() {
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    let mut db = setup_db();

    // Two events for doc 1.
    let mut batch = ZSet::new();
    batch.insert("events".into(), event_row(100, 1, 0, 10));
    batch.insert("events".into(), event_row(101, 1, 1, 20));
    db.apply_zset(&batch).unwrap();
    let outcome = engine.derive(&batch, &mut DatabaseHost::new(&mut db));
    assert!(outcome.failures.is_empty());
    assert_eq!(table_rows(&db, "totals"), vec![vec![i64v(1), i64v(30)]]);

    // Third event — the totals row is REPLACED via PK upsert; exactly one
    // live row must remain.
    let mut batch = ZSet::new();
    batch.insert("events".into(), event_row(102, 1, 2, 5));
    db.apply_zset(&batch).unwrap();
    let outcome = engine.derive(&batch, &mut DatabaseHost::new(&mut db));
    assert!(outcome.failures.is_empty());
    assert_eq!(table_rows(&db, "totals"), vec![vec![i64v(1), i64v(35)]]);
}

#[test]
fn indexed_key_read_and_reject_invert() {
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    let mut db = setup_db();

    let mut batch = ZSet::new();
    batch.insert("events".into(), event_row(100, 1, 0, 10));
    batch.insert("events".into(), event_row(200, 2, 0, 7));
    db.apply_zset(&batch).unwrap();
    engine.derive(&batch, &mut DatabaseHost::new(&mut db));

    let mut totals = table_rows(&db, "totals");
    totals.sort();
    assert_eq!(totals, vec![vec![i64v(1), i64v(10)], vec![i64v(2), i64v(7)]]);

    // Reject: invert doc 1's event — its totals row must vanish, doc 2's
    // must stay.
    let mut invert = ZSet::new();
    invert.delete("events".into(), event_row(100, 1, 0, 10));
    db.apply_zset(&invert).unwrap();
    let outcome = engine.derive(&invert, &mut DatabaseHost::new(&mut db));
    assert!(outcome.failures.is_empty());
    assert_eq!(table_rows(&db, "totals"), vec![vec![i64v(2), i64v(7)]]);
}
