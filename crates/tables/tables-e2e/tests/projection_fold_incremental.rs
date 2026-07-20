//! Proves the §9.3/§11 incremental EXECUTION of the fold shim: already
//! folded committed rows are not re-applied (memoized committed-prefix
//! snapshot keyed by the server-chain id list), read-dirty only
//! re-renders, pendings refold from the snapshot, and a server reorder of
//! the committed chain (the id list stops extending the memo) falls back
//! to fold-from-zero. The apply counters make execution (not just results)
//! observable; each test owns its projection type and counter because
//! tests run in parallel within one binary.

use std::sync::atomic::{AtomicUsize, Ordering};

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::{Out, ProjectionEngine, RenderCtx};
use rpc_command::rpc_command;
use sql_engine::storage::{CellValue, Uuid, ZSet};
use sql_engine::DbTable;
use tables::{ProjectionLog, ROOT_PARENT};
use tables_storage::{projection, projection_row, row};

#[projection_row]
pub struct DraftLog {
    pub command_id: Uuid,
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

fn uuid(n: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    Uuid(bytes)
}

fn payload(n: u8, price_cents: i64) -> String {
    rpc_command::payload_json(&SetLinePrice { id: n as i64, doc_id: 1, price_cents }).unwrap()
}

/// A committed row (partition `doc_id = 1`): the server linked it after
/// `parent`. No drift, so the client link mirrors the server link.
fn committed_row(n: u8, parent: Uuid, price_cents: i64) -> DraftLog {
    DraftLog {
        command_id: uuid(n),
        doc_id: 1,
        client_parent_id: parent,
        server_parent_id: Some(parent),
        payload: payload(n, price_cents),
    }
}

/// A pending row: off-chain (`server_parent_id = None`), on the client's
/// optimistic chain after `client_parent`.
fn pending_row(n: u8, client_parent: Uuid, price_cents: i64) -> DraftLog {
    DraftLog {
        command_id: uuid(n),
        doc_id: 1,
        client_parent_id: client_parent,
        server_parent_id: None,
        payload: payload(n, price_cents),
    }
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
    for row in [
        committed_row(100, ROOT_PARENT, 10),
        committed_row(101, uuid(100), 20),
        committed_row(102, uuid(101), 30),
    ] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_A.load(Ordering::SeqCst), 3);
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(60));

    let batch = insert(&mut db, committed_row(103, uuid(102), 5));
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
    for row in [committed_row(100, ROOT_PARENT, 10), committed_row(101, uuid(100), 20)] {
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
    for row in [
        committed_row(100, ROOT_PARENT, 10),
        committed_row(101, uuid(100), 20),
        pending_row(102, uuid(101), 1), // pending after the committed tail
    ] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_C.load(Ordering::SeqCst), 3);
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(31));

    // A second pending: the committed prefix is skipped, but pendings are
    // never memoized — BOTH refold on top of the snapshot.
    let batch = insert(&mut db, pending_row(103, uuid(102), 2));
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_C.load(Ordering::SeqCst), 5, "2 committed skipped, 2 pendings folded");
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(33));
}

// ── 4: a server reorder of the committed chain folds from zero ────────

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
fn reorder_of_the_committed_chain_folds_from_zero() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<Total>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FoldD::default())).unwrap();

    let mut batch = ZSet::new();
    for row in [committed_row(100, ROOT_PARENT, 10), committed_row(101, uuid(100), 20)] {
        batch.extend(insert(&mut db, row));
    }
    derive(&mut db, &mut engine, &batch);
    assert_eq!(APPLIES_D.load(Ordering::SeqCst), 2);
    assert_eq!(single_row(&db, "total")[1], CellValue::I64(30));

    // The server re-links the committed chain, inserting 102 BETWEEN 100 and
    // 101: 100 → 102 → 101. The reconcile deletes the old 101 (linked after
    // 100) and re-inserts it linked after 102 (a drift: its client link
    // stays 100). The committed id list becomes [100, 102, 101] — no longer
    // a prefix of the memoized [100, 101] — so the shim folds from zero.
    let old_101 = committed_row(101, uuid(100), 20);
    let relinked_101 = DraftLog {
        command_id: uuid(101),
        doc_id: 1,
        client_parent_id: uuid(100), // client still believes it follows 100
        server_parent_id: Some(uuid(102)),
        payload: payload(101, 20),
    };
    let mut reorder = ZSet::new();
    reorder.delete(DraftLog::TABLE.into(), old_101.into_cells());
    reorder.insert(DraftLog::TABLE.into(), committed_row(102, uuid(100), 100).into_cells());
    reorder.insert(DraftLog::TABLE.into(), relinked_101.into_cells());
    db.apply_zset(&reorder).unwrap();
    derive(&mut db, &mut engine, &reorder);

    assert_eq!(APPLIES_D.load(Ordering::SeqCst), 5, "full refold after the reorder");
    assert_eq!(
        single_row(&db, "total")[1],
        CellValue::I64(130),
        "fold order is the re-linked server chain 100 → 102 → 101"
    );
}
