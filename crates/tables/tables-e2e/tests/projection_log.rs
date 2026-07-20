//! E2E of `#[projection_row]` (M6a/§9.6): the struct declares only
//! `command_id` and the partition column (inferred — no attribute
//! argument); the macro appends `seq`, `committed` and `payload`,
//! expands to a full `#[row]` and implements `tables::ProjectionLog`.
//! A hand-written append (`sync::append::{next_seq, append_row}` +
//! `rpc_command::payload_json`) fills exactly the generated shape.

use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::CellValue;
use sql_engine::DbTable;
use sync::append::{append_row, next_seq};
use tables::ProjectionLog;
use tables_storage::{projection, projection_row, row};

#[projection_row]
pub struct DraftLog {
    pub command_id: i64,
    pub doc_id: i64,
}

/// The event carried in a `DraftLog` row's payload. A plain `#[rpc_command]`
/// (a serializable request shape) — the log rows are appended by hand below.
#[rpc_command]
pub struct SetLinePrice {
    pub id: i64,
    pub doc_id: i64,
    pub price_cents: i64,
}

/// Append one event to `DraftLog` by hand — the pattern that replaced the
/// generated `append_to` impl: provisional per-partition `seq`,
/// `committed = 0` (off-chain), payload = the event's RPC form.
fn append(db: &mut Database, command_id: i64, doc_id: i64, price_cents: i64) {
    let partition = CellValue::from(doc_id);
    let seq = next_seq::<DraftLog>(db, DraftLog::PARTITION_COLUMN, &partition).unwrap();
    let payload =
        rpc_command::payload_json(&SetLinePrice { id: command_id, doc_id, price_cents }).unwrap();
    append_row(db, DraftLog { command_id, doc_id, seq, committed: 0, payload }).unwrap();
}

#[test]
fn log_form_generates_the_full_row_shape() {
    let schema = DraftLog::schema();
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["command_id", "doc_id", "seq", "committed", "payload"]
    );
    assert_eq!(schema.primary_key, vec![0]);
    assert_eq!(DraftLog::TABLE, "draft_log");
}

#[test]
fn log_form_infers_the_partition_column() {
    assert_eq!(DraftLog::PARTITION_COLUMN, "doc_id");
}

#[test]
fn log_row_roundtrips_through_cells() {
    let row = DraftLog {
        command_id: 7,
        doc_id: 1,
        seq: 3,
        committed: 1,
        payload: "{}".into(),
    };
    let cells = row.into_cells();
    let back = DraftLog::from_cells(&cells).unwrap();
    assert_eq!(back.command_id, 7);
    assert_eq!(back.doc_id, 1);
    assert_eq!(back.seq, 3);
    assert_eq!(back.committed, 1);
    assert_eq!(back.payload, "{}");
}

#[test]
fn hand_written_append_fills_the_generated_shape() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();

    append(&mut db, 100, 1, 1500);
    append(&mut db, 101, 1, 900);

    let t = db.table(DraftLog::TABLE).unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<DraftLog> = t
        .row_ids()
        .map(|r| {
            let cells: Vec<CellValue> = (0..ncols).map(|c| t.get(r, c)).collect();
            DraftLog::from_cells(&cells).unwrap()
        })
        .collect();
    rows.sort_by_key(|r| r.seq);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].command_id, 100);
    assert_eq!(rows[0].seq, 0);
    assert_eq!(rows[0].committed, 0, "optimistic rows are off-chain");
    assert_eq!(rows[1].seq, 1, "provisional seq counts per partition");

    // Payload is the command's own wire form — deserializable back.
    let back: SetLinePrice = serde_json::from_str(&rows[1].payload).unwrap();
    assert_eq!(back.id, 101);
    assert_eq!(back.price_cents, 900);
}

// ── Generated fold helpers (§9.6) ────────────────────────────────────

fn log_row(command_id: i64, seq: i64, committed: i64, payload: &str) -> DraftLog {
    DraftLog { command_id, doc_id: 1, seq, committed, payload: payload.into() }
}

#[test]
fn decode_roundtrips_the_payload_and_names_the_type_on_error() {
    let cmd = SetLinePrice { id: 100, doc_id: 1, price_cents: 1500 };
    let payload = rpc_command::payload_json(&cmd).unwrap();
    let row = log_row(100, 0, 0, &payload);

    let back: SetLinePrice = row.decode().unwrap();
    assert_eq!(back.price_cents, 1500);

    let err = log_row(101, 1, 0, "not json").decode::<SetLinePrice>().unwrap_err();
    assert!(err.contains("SetLinePrice"), "error names the type: {err}");
}

#[test]
fn is_committed_reads_the_convention_column() {
    assert!(!log_row(1, 0, 0, "{}").is_committed());
    assert!(log_row(2, 0, 1, "{}").is_committed());
}

#[test]
fn in_fold_order_sorts_committed_by_seq_then_pendings() {
    // Arrival order scrambled: a pending with LOW provisional seq must
    // still fold after every committed row (§9.3 fold order).
    let rows = vec![
        log_row(103, 0, 0, "{}"), // pending, provisional seq 0
        log_row(101, 5, 1, "{}"), // committed, seq 5
        log_row(104, 1, 0, "{}"), // pending, provisional seq 1
        log_row(100, 2, 1, "{}"), // committed, seq 2
    ];
    let ordered: Vec<i64> = DraftLog::in_fold_order(&rows)
        .into_iter()
        .map(|r| r.command_id)
        .collect();
    assert_eq!(ordered, vec![100, 101, 103, 104]);
}

// ── Impl-form partition inference from the log source (§9.6) ─────────

#[derive(Default, Clone)]
pub struct DraftLogCount {
    doc_id: i64,
    events: i64,
}

#[projection(outputs(DraftCount))]
impl DraftLogCount {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        self.doc_id = row.doc_id;
        self.events += 1;
        Ok(())
    }

    fn render(
        &self,
        _ctx: &database_projection::RenderCtx<'_>,
        out: &mut database_projection::Out,
    ) -> Result<(), String> {
        out.emit(DraftCount { doc_id: self.doc_id, events: self.events });
        Ok(())
    }
}

#[row]
pub struct DraftCount {
    #[pk]
    pub doc_id: i64,
    pub events: i64,
}

#[test]
fn impl_form_infers_partition_from_the_log_source() {
    use database_projection::Projection;
    let spec = DraftLogCount::default().spec();
    assert_eq!(spec.sources.len(), 1);
    assert_eq!(spec.sources[0].table, "draft_log");
    assert_eq!(spec.sources[0].partition_column, 1, "doc_id — via PARTITION_COLUMN");
}
