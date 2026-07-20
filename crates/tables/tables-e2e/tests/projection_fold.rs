//! E2E of the `#[projection]` fold contract (§9.4): the impl target IS
//! the fold state; `apply` folds ONE typed log row at a time — the shim
//! feeds the partition's rows in fold order (the committed server-parent
//! chain, then the pending client-parent tail — §11.3) into a
//! `Default::default()` value — and `render` emits the outputs once.
//! Executed on the recompute node: the contract is M6b's, the execution
//! strategy is fold-from-zero.

use database::Database;
use database_projection::db_host::DatabaseHost;
use database_projection::{Out, Projection, ProjectionEngine, RenderCtx};
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
/// (a serializable request shape) — the log rows here are built directly by
/// the test fixture. Its `id` (a small int) doubles as the fold-order trace.
#[rpc_command]
pub struct SetLinePrice {
    pub id: i64,
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
    /// Payload ids in the order `apply` saw them — makes the fold order
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
        self.trace.push(cmd.id);
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

fn uuid(n: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    Uuid(bytes)
}

fn payload(n: u8, doc_id: i64, price_cents: i64) -> String {
    rpc_command::payload_json(&SetLinePrice { id: n as i64, doc_id, price_cents }).unwrap()
}

/// A committed row: the server has linked it after `parent`. No drift, so
/// the client link mirrors the server link.
fn committed_row(n: u8, doc_id: i64, parent: Uuid, price_cents: i64) -> DraftLog {
    DraftLog {
        command_id: uuid(n),
        doc_id,
        client_parent_id: parent,
        server_parent_id: Some(parent),
        payload: payload(n, doc_id, price_cents),
    }
}

/// A pending row: off-chain (`server_parent_id = None`), linked into the
/// client's optimistic chain after `client_parent`.
fn pending_row(n: u8, doc_id: i64, client_parent: Uuid, price_cents: i64) -> DraftLog {
    DraftLog {
        command_id: uuid(n),
        doc_id,
        client_parent_id: client_parent,
        server_parent_id: None,
        payload: payload(n, doc_id, price_cents),
    }
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

    // Arrival order scrambled: the pending tail lands first — apply must
    // still see the committed server chain (100 → 101), then the pending
    // client tail (101 → 103 → 104).
    let mut batch = ZSet::new();
    for row in [
        pending_row(103, 1, uuid(101), 7),        // pending after committed tail
        committed_row(101, 1, uuid(100), 30),     // committed 2nd
        pending_row(104, 1, uuid(103), 9),         // pending after 103
        committed_row(100, 1, ROOT_PARENT, 10),   // committed 1st
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

    let batch = insert(&mut db, committed_row(100, 1, ROOT_PARENT, 10));
    let mut host = DatabaseHost::new(&mut db);
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());
    let before = rows_of(&db, "draft_total");

    // An undecodable payload fails the WHOLE partition (fold, not row
    // isolation) — decode policy beyond `?` is product code.
    let bad = DraftLog {
        command_id: uuid(101),
        doc_id: 1,
        client_parent_id: uuid(100),
        server_parent_id: Some(uuid(100)),
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

    let batch = insert(&mut db, committed_row(100, 1, ROOT_PARENT, 10));
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
