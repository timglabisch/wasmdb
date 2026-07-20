//! E2E of the `#[projection]` fold contract (§9.4): the impl target IS
//! the fold state; `apply` folds ONE typed log row at a time — the shim
//! feeds the partition's rows in fold order (committed by seq, then
//! pendings) into a `Default::default()` value — and `render` emits the
//! outputs once. Executed on the recompute node: the contract is M6b's,
//! the execution strategy is fold-from-zero.

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::{Out, Projection, ProjectionEngine, RenderCtx};
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

#[rpc_command(append_to = DraftLog)]
pub struct SetLinePrice {
    pub id: i64,
    #[partition]
    pub doc_id: i64,
    pub price_cents: i64,
}

#[row]
pub struct CustomerLabel {
    #[pk]
    pub doc_id: i64,
    pub label: String,
}

#[row]
pub struct DraftTotal {
    #[pk]
    pub doc_id: i64,
    pub amount: i64,
    /// Command ids in the order `apply` saw them — makes the fold order
    /// observable in the output.
    pub fold_trace: String,
    pub label: String,
}

#[row]
pub struct DraftCount {
    #[pk]
    pub doc_id: i64,
    pub events: i64,
}

/// The projection IS its fold state — the impl target carries the
/// accumulator fields, one value per partition.
#[derive(Default, Clone)]
pub struct DraftFold {
    doc_id: i64,
    amount: i64,
    trace: Vec<i64>,
}

#[projection(outputs(DraftTotal, DraftCount), reads(CustomerLabel))]
impl DraftFold {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        let cmd: SetLinePrice = row.decode()?;
        self.doc_id = row.doc_id;
        self.amount += cmd.price_cents;
        self.trace.push(row.command_id);
        Ok(())
    }

    fn render(&self, ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        let label = ctx
            .all::<CustomerLabel>()?
            .into_iter()
            .find(|l| l.doc_id == self.doc_id)
            .map(|l| l.label)
            .unwrap_or_default();
        out.emit(DraftTotal {
            doc_id: self.doc_id,
            amount: self.amount,
            fold_trace: self
                .trace
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(","),
            label,
        });
        out.emit(DraftCount { doc_id: self.doc_id, events: self.trace.len() as i64 });
        Ok(())
    }
}

fn log_row(command_id: i64, doc_id: i64, seq: i64, committed: i64, price_cents: i64) -> DraftLog {
    let payload =
        rpc_command::payload_json(&SetLinePrice { id: command_id, doc_id, price_cents }).unwrap();
    DraftLog { command_id, doc_id, seq, committed, payload }
}

fn setup() -> (Database, ProjectionEngine) {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();
    db.register_table::<CustomerLabel>().unwrap();
    db.register_table::<DraftTotal>().unwrap();
    db.register_table::<DraftCount>().unwrap();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(DraftFold::default())).unwrap();
    (db, engine)
}

fn insert<R: DbTable>(db: &mut Database, row: R) -> ZSet {
    let cells = row.into_cells();
    db.insert(R::TABLE, &cells).unwrap();
    let mut zset = ZSet::new();
    zset.insert(R::TABLE.into(), cells);
    zset
}

fn rows_of(db: &Database, table: &str) -> Vec<Vec<CellValue>> {
    let t = db.table(table).unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<Vec<CellValue>> = t
        .row_ids()
        .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
        .collect();
    rows.sort();
    rows
}

#[test]
fn spec_reflects_the_fold_form() {
    let spec = DraftFold::default().spec();
    assert_eq!(spec.id, "draft_fold");
    assert_eq!(spec.sources.len(), 1, "fold form has exactly one source");
    assert_eq!(spec.sources[0].table, "draft_log");
    assert_eq!(spec.sources[0].partition_column, 1, "doc_id — via PARTITION_COLUMN");
    assert_eq!(spec.reads, vec!["customer_label".to_string()]);
    assert_eq!(
        spec.outputs,
        vec!["draft_total".to_string(), "draft_count".to_string()]
    );
}

#[test]
fn shim_feeds_rows_in_fold_order() {
    let (mut db, mut engine) = setup();

    // Arrival order scrambled: pendings land first, committed rows carry
    // higher seqs — apply must still see committed-by-seq, then pendings.
    let mut batch = ZSet::new();
    for row in [
        log_row(103, 1, 0, 0, 7),  // pending, provisional seq 0
        log_row(101, 1, 5, 1, 30), // committed, seq 5
        log_row(104, 1, 1, 0, 9),  // pending, provisional seq 1
        log_row(100, 1, 2, 1, 10), // committed, seq 2
    ] {
        batch.extend(insert(&mut db, row));
    }
    let mut host = DatabaseHost::new(&mut db);
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);

    assert_eq!(
        rows_of(&db, "draft_total"),
        vec![vec![
            CellValue::I64(1),
            CellValue::I64(56),
            CellValue::Str("100,101,103,104".into()),
            CellValue::Str(String::new()),
        ]]
    );
    // Second output table, same render pass.
    assert_eq!(
        rows_of(&db, "draft_count"),
        vec![vec![CellValue::I64(1), CellValue::I64(4)]]
    );
}

#[test]
fn apply_error_pins_the_partition_and_keeps_the_last_render() {
    let (mut db, mut engine) = setup();

    let batch = insert(&mut db, log_row(100, 1, 0, 1, 10));
    let mut host = DatabaseHost::new(&mut db);
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());
    let before = rows_of(&db, "draft_total");

    // An undecodable payload fails the WHOLE partition (fold, not row
    // isolation) — decode policy beyond `?` is product code.
    let bad = DraftLog {
        command_id: 101,
        doc_id: 1,
        seq: 1,
        committed: 1,
        payload: "not json".into(),
    };
    let batch = insert(&mut db, bad);
    let mut host = DatabaseHost::new(&mut db);
    let outcome = engine.derive(&batch, &mut host);
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(outcome.failures[0].projection, "draft_fold");
    assert_eq!(outcome.failures[0].partition.as_deref(), Some("1"));
    assert!(
        outcome.failures[0].message.contains("SetLinePrice"),
        "decode error names the type: {}",
        outcome.failures[0].message
    );

    assert_eq!(rows_of(&db, "draft_total"), before, "failed partition keeps its last render");
}

#[test]
fn read_change_rerenders_the_partition() {
    let (mut db, mut engine) = setup();

    let batch = insert(&mut db, log_row(100, 1, 0, 1, 10));
    let mut host = DatabaseHost::new(&mut db);
    engine.derive(&batch, &mut host);
    assert_eq!(
        rows_of(&db, "draft_total"),
        vec![vec![
            CellValue::I64(1),
            CellValue::I64(10),
            CellValue::Str("100".into()),
            CellValue::Str(String::new()),
        ]]
    );

    // Label arrives through the read table — no new events, new render.
    let batch = insert(&mut db, CustomerLabel { doc_id: 1, label: "ACME".into() });
    let mut host = DatabaseHost::new(&mut db);
    engine.derive(&batch, &mut host);
    assert_eq!(
        rows_of(&db, "draft_total"),
        vec![vec![
            CellValue::I64(1),
            CellValue::I64(10),
            CellValue::Str("100".into()),
            CellValue::Str("ACME".into()),
        ]]
    );
}
