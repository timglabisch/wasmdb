//! E2E of `#[projection_row]` (M6a/§9.6/§11): the struct declares only
//! `command_id` (Uuid) and the partition column (inferred — no attribute
//! argument); the macro appends the two-parent-link bookkeeping columns
//! `client_parent_id`, `server_parent_id` and `payload`, expands to a full
//! `#[row]` and implements `tables::ProjectionLog`. A hand-written append
//! (`sync::append::{client_head, append_row}` + `rpc_command::payload_json`)
//! fills exactly the generated shape.

use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::{CellValue, Uuid};
use sql_engine::DbTable;
use sync::append::{append_row, client_head};
use tables::{ProjectionLog, ROOT_PARENT};
use tables_storage::{projection, projection_row, row};

#[projection_row]
pub struct DraftLog {
    pub command_id: Uuid,
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

/// Deterministic command id from a small counter — no RNG in fixtures.
fn uuid(n: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    Uuid(bytes)
}

/// Append one event to `DraftLog` by hand — the pattern that replaced the
/// generated `append_to` impl: `client_parent_id` = the partition's current
/// chain head, `server_parent_id = None` (off-chain until the server links
/// it), payload = the event's RPC form.
fn append(db: &mut Database, n: u8, doc_id: i64, price_cents: i64) {
    let partition = CellValue::from(doc_id);
    let client_parent_id =
        client_head::<DraftLog>(db, DraftLog::PARTITION_COLUMN, &partition).unwrap();
    let payload =
        rpc_command::payload_json(&SetLinePrice { id: n as i64, doc_id, price_cents }).unwrap();
    append_row(
        db,
        DraftLog { command_id: uuid(n), doc_id, client_parent_id, server_parent_id: None, payload },
    )
    .unwrap();
}

#[test]
fn log_form_generates_the_full_row_shape() {
    let schema = DraftLog::schema();
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["command_id", "doc_id", "client_parent_id", "server_parent_id", "payload"]
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
    // A committed row: `server_parent_id = Some(..)`.
    let row = DraftLog {
        command_id: uuid(7),
        doc_id: 1,
        client_parent_id: uuid(3),
        server_parent_id: Some(uuid(3)),
        payload: "{}".into(),
    };
    let back = DraftLog::from_cells(&row.clone().into_cells()).unwrap();
    assert_eq!(back.command_id, uuid(7));
    assert_eq!(back.client_parent_id, uuid(3));
    assert_eq!(back.server_parent_id, Some(uuid(3)));
    assert_eq!(back.payload, "{}");

    // A pending row: `server_parent_id = None` maps to a NULL cell and back.
    let pending = DraftLog {
        command_id: uuid(8),
        doc_id: 1,
        client_parent_id: uuid(7),
        server_parent_id: None,
        payload: "{}".into(),
    };
    let cells = pending.into_cells();
    assert_eq!(cells[3], CellValue::Null, "None server parent is a NULL cell");
    assert_eq!(DraftLog::from_cells(&cells).unwrap().server_parent_id, None);
}

#[test]
fn hand_written_append_fills_the_generated_shape() {
    let mut db = Database::new();
    db.register_table::<DraftLog>().unwrap();

    append(&mut db, 100, 1, 1500);
    append(&mut db, 101, 1, 900);

    let t = db.table(DraftLog::TABLE).unwrap();
    let ncols = t.schema.columns.len();
    let rows: Vec<DraftLog> = t
        .row_ids()
        .map(|r| {
            let cells: Vec<CellValue> = (0..ncols).map(|c| t.get(r, c)).collect();
            DraftLog::from_cells(&cells).unwrap()
        })
        .collect();
    assert_eq!(rows.len(), 2);

    let first = rows.iter().find(|r| r.command_id == uuid(100)).unwrap();
    let second = rows.iter().find(|r| r.command_id == uuid(101)).unwrap();

    // The first event opens the partition's chain (parent = ROOT); the
    // second links to the first — the client chain, built by `client_head`.
    assert_eq!(first.client_parent_id, ROOT_PARENT);
    assert_eq!(second.client_parent_id, uuid(100), "chained onto the head");
    assert_eq!(first.server_parent_id, None, "optimistic rows are off-chain");
    assert_eq!(second.server_parent_id, None);

    // Payload is the command's own wire form — deserializable back.
    let back: SetLinePrice = second.decode().unwrap();
    assert_eq!(back.id, 101);
    assert_eq!(back.price_cents, 900);
}

// ── Generated fold helpers (§9.6/§11) ────────────────────────────────

fn log_row(command_id: Uuid, client_parent: Uuid, server_parent: Option<Uuid>, payload: &str) -> DraftLog {
    DraftLog { command_id, doc_id: 1, client_parent_id: client_parent, server_parent_id: server_parent, payload: payload.into() }
}

#[test]
fn decode_roundtrips_the_payload_and_names_the_type_on_error() {
    let cmd = SetLinePrice { id: 100, doc_id: 1, price_cents: 1500 };
    let payload = rpc_command::payload_json(&cmd).unwrap();
    let row = log_row(uuid(100), ROOT_PARENT, None, &payload);

    let back: SetLinePrice = row.decode().unwrap();
    assert_eq!(back.price_cents, 1500);

    let err = log_row(uuid(101), ROOT_PARENT, None, "not json")
        .decode::<SetLinePrice>()
        .unwrap_err();
    assert!(err.contains("SetLinePrice"), "error names the type: {err}");
}

#[test]
fn is_committed_reads_the_server_parent_link() {
    // No server link yet → pending; a server link (even to ROOT) → committed.
    assert!(!log_row(uuid(1), ROOT_PARENT, None, "{}").is_committed());
    assert!(log_row(uuid(2), ROOT_PARENT, Some(ROOT_PARENT), "{}").is_committed());
}

#[test]
fn in_fold_order_walks_the_chain() {
    // Arrival order scrambled: the committed server chain must come first
    // (ROOT → 100 → 101), then the pending client tail (101 → 103 → 104).
    let rows = vec![
        log_row(uuid(103), uuid(101), None, "{}"),                 // pending after tail
        log_row(uuid(101), uuid(100), Some(uuid(100)), "{}"),      // committed 2nd
        log_row(uuid(104), uuid(103), None, "{}"),                 // pending after 103
        log_row(uuid(100), ROOT_PARENT, Some(ROOT_PARENT), "{}"),  // committed 1st
    ];
    let ordered: Vec<Uuid> = DraftLog::in_fold_order(&rows)
        .into_iter()
        .map(|r| r.command_id)
        .collect();
    assert_eq!(ordered, vec![uuid(100), uuid(101), uuid(103), uuid(104)]);
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
