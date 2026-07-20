//! Proves the §9.3 incremental EXECUTION of the fold shim: already
//! folded committed rows are not re-applied (memoized committed-prefix
//! snapshot), read-dirty only re-renders, pendings refold from the
//! snapshot, and a backfill behind the committed frontier falls back to
//! fold-from-zero. The apply counters make execution (not just results)
//! observable; each test owns its projection type and counter because
//! tests run in parallel within one binary.

use std::sync::atomic::{AtomicUsize, Ordering};

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::{Out, ProjectionEngine, RenderCtx};
use rpc_command::rpc_command;
use sql_engine::storage::{CellValue, ZSet};
use sql_engine::DbTable;
use tables::ProjectionLog;
use tables_storage::{projection, projection_row, row};

#[projection_row]
pub struct DraftLog {
    pub command_id: i64,
    pub doc_id: i64,
}

/// The event carried in a `DraftLog` row's payload. A plain `#[rpc_command]`
/// (a serializable request shape); the fixture builds log rows directly.
#[rpc_command]
pub struct SetLinePrice {
    pub id: i64,
    pub doc_id: i64,
    pub price_cents: i64,
}

#[row]
pub struct Total {
    #[pk]
    pub doc_id: i64,
    pub amount: i64,
}

#[row]
pub struct Label {
    #[pk]
    pub doc_id: i64,
    pub label: String,
}

#[row]
pub struct TotalLabeled {
    #[pk]
    pub doc_id: i64,
    pub amount: i64,
    pub label: String,
}

fn log_row(command_id: i64, seq: i64, committed: i64, price_cents: i64) -> DraftLog {
    let payload =
        rpc_command::payload_json(&SetLinePrice { id: command_id, doc_id: 1, price_cents })
            .unwrap();
    DraftLog { command_id, doc_id: 1, seq, committed, payload }
}

fn insert<R: DbTable>(db: &mut Database, row: R) -> ZSet {
    let cells = row.into_cells();
    db.insert(R::TABLE, &cells).unwrap();
    let mut zset = ZSet::new();
    zset.insert(R::TABLE.into(), cells);
    zset
}

fn derive(db: &mut Database, engine: &mut ProjectionEngine, batch: &ZSet) {
    let mut host = DatabaseHost::new(db);
    let outcome = engine.derive(batch, &mut host);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
}

fn single_row(db: &Database, table: &str) -> Vec<CellValue> {
    let t = db.table(table).unwrap();
    let ncols = t.schema.columns.len();
    let rows: Vec<Vec<CellValue>> = t
        .row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect();
    assert_eq!(rows.len(), 1, "expected exactly one row in {table}");
    rows.into_iter().next().unwrap()
}

// ── 1: committed rows fold only once ─────────────────────────────────

static APPLIES_A: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Clone)]
pub struct FoldA {
    doc_id: i64,
    amount: i64,
}

#[projection(outputs(Total))]
impl FoldA {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        APPLIES_A.fetch_add(1, Ordering::SeqCst);
        let cmd: SetLinePrice = row.decode()?;
        self.doc_id = row.doc_id;
        self.amount += cmd.price_cents;
        Ok(())
    }

    fn render(&self, _ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(Total { doc_id: self.doc_id, amount: self.amount });
        Ok(())
    }
}

#[test]
fn committed_rows_fold_only_once() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<Total>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FoldA::default())).unwrap();

    let mut batch = ZSet::new();
    for row in [log_row(100, 0, 1, 10), log_row(101, 1, 1, 20), log_row(102, 2, 1, 30)] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_A.load(Ordering::SeqCst), 3);
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(60));

    let batch = insert(&mut db, log_row(103, 3, 1, 5));
    derive(&mut db, &mut engine, &batch);
    assert_eq!(
        APPLIES_A.load(Ordering::SeqCst),
        4,
        "only the NEW committed row folds — the prefix comes from the memo"
    );
    assert_eq!(
        single_row(&db, "total")[1],
        CellValue::I64(65),
        "memoized state carried the earlier rows"
    );
}

// ── 2: read-dirty re-renders without refolding ───────────────────────

static APPLIES_B: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Clone)]
pub struct FoldB {
    doc_id: i64,
    amount: i64,
}

#[projection(outputs(TotalLabeled), reads(Label))]
impl FoldB {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        APPLIES_B.fetch_add(1, Ordering::SeqCst);
        let cmd: SetLinePrice = row.decode()?;
        self.doc_id = row.doc_id;
        self.amount += cmd.price_cents;
        Ok(())
    }

    fn render(&self, ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        let label = ctx
            .all::<Label>()?
            .into_iter()
            .find(|l| l.doc_id == self.doc_id)
            .map(|l| l.label)
            .unwrap_or_default();
        out.emit(TotalLabeled { doc_id: self.doc_id, amount: self.amount, label });
        Ok(())
    }
}

#[test]
fn read_change_rerenders_without_refolding() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<Label>().unwrap();
    db.register_table::<TotalLabeled>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FoldB::default())).unwrap();

    let mut batch = ZSet::new();
    for row in [log_row(100, 0, 1, 10), log_row(101, 1, 1, 20)] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_B.load(Ordering::SeqCst), 2);

    // Label arrives: no source change — render only, zero apply calls.
    let batch = insert(&mut db, Label { doc_id: 1, label: "ACME".into() });
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_B.load(Ordering::SeqCst), 2, "read-dirty must not refold");
    let row = single_row(&db, "total_labeled");
    assert_eq!(row[1], CellValue::I64(30));
    assert_eq!(row[2], CellValue::Str("ACME".into()), "but it DID re-render");
}

// ── 3: pendings refold from the committed snapshot ───────────────────

static APPLIES_C: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Clone)]
pub struct FoldC {
    doc_id: i64,
    amount: i64,
}

#[projection(outputs(Total))]
impl FoldC {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        APPLIES_C.fetch_add(1, Ordering::SeqCst);
        let cmd: SetLinePrice = row.decode()?;
        self.doc_id = row.doc_id;
        self.amount += cmd.price_cents;
        Ok(())
    }

    fn render(&self, _ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(Total { doc_id: self.doc_id, amount: self.amount });
        Ok(())
    }
}

#[test]
fn pendings_refold_from_the_committed_snapshot() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<Total>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FoldC::default())).unwrap();

    let mut batch = ZSet::new();
    for row in [log_row(100, 0, 1, 10), log_row(101, 1, 1, 20), log_row(102, 0, 0, 1)] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_C.load(Ordering::SeqCst), 3);
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(31));

    // A second pending: the committed prefix is skipped, but pendings are
    // never memoized — BOTH refold on top of the snapshot.
    let batch = insert(&mut db, log_row(103, 1, 0, 2));
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_C.load(Ordering::SeqCst), 5, "2 committed skipped, 2 pendings folded");
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(33));
}

// ── 4: backfill behind the frontier invalidates the memo ─────────────

static APPLIES_D: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Clone)]
pub struct FoldD {
    doc_id: i64,
    amount: i64,
}

#[projection(outputs(Total))]
impl FoldD {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        APPLIES_D.fetch_add(1, Ordering::SeqCst);
        let cmd: SetLinePrice = row.decode()?;
        self.doc_id = row.doc_id;
        self.amount += cmd.price_cents;
        Ok(())
    }

    fn render(&self, _ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(Total { doc_id: self.doc_id, amount: self.amount });
        Ok(())
    }
}

#[test]
fn backfill_behind_the_frontier_folds_from_zero() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<Total>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FoldD::default())).unwrap();

    let mut batch = ZSet::new();
    for row in [log_row(100, 2, 1, 10), log_row(101, 5, 1, 20)] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_D.load(Ordering::SeqCst), 2);

    // seq 3 lands BEHIND the folded frontier (tail backfill): the seq
    // list [2,5] is no prefix of [2,3,5] — fold from zero, once.
    let batch = insert(&mut db, log_row(102, 3, 1, 100));
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_D.load(Ordering::SeqCst), 5, "full refold after backfill");
    assert_eq!(
        single_row(&db, "total")[1],
        CellValue::I64(130),
        "fold order includes the backfilled row"
    );
}
