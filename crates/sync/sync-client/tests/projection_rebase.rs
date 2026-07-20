//! Acceptance test for the projections design (M4 criterion from
//! docs/wasmdb-projections-design.md): the REBASE case.
//!
//! Events live as rows in a normal log table; a command's optimistic
//! projection is nothing but the append of its own row. Derived tables
//! are maintained by the projection engine at the notify chokepoint.
//! Because every command is its own disjoint row, the sync layer's
//! existing invert-based reconcile is correct again — and a reject while
//! FOREIGN events are interleaved lands on "base + foreign events,
//! without mine", a state that never existed before. That is the case
//! the old hand-projection contract structurally could not reach.

use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use database_projection::{
    Inputs, PartitionedSource, OutputRow, Projection, ProjectionEngine, ProjectionSpec, ReadCtx,
};
use sql_engine::storage::{CellValue, ZSet};
use sync::command::{Command, CommandError};
use sync::protocol::{CommandResponse, Verdict};
use sync_client::SyncClient;

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

/// The event-append command: its optimistic effect IS the log row.
/// No hand-written derivation — the projection owns `totals`.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct AppendEvent {
    command_id: i64,
    doc_id: i64,
    seq: i64,
    val: i64,
}

impl AppendEvent {
    fn row(&self) -> Vec<CellValue> {
        vec![i64v(self.command_id), i64v(self.doc_id), i64v(self.seq), i64v(self.val)]
    }
}

impl Command for AppendEvent {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let row = self.row();
        db.insert("events", &row)
            .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
        let mut zset = ZSet::new();
        zset.insert("events".into(), row);
        Ok(zset)
    }
}

/// events keyed by doc_id → totals(doc_id, amount).
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

fn setup_client() -> SyncClient<AppendEvent> {
    let mut db = Database::new();
    db.execute_ddl(DDL).unwrap();
    let mut client = SyncClient::new(db);
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Totals)).unwrap();
    client.db_mut().install_projections(engine);
    client
}

/// A foreign event arriving over the wire (tail fetch / another client).
fn foreign_event(command_id: i64, doc_id: i64, seq: i64, val: i64) -> ZSet {
    let mut zset = ZSet::new();
    zset.insert("events".into(), vec![i64v(command_id), i64v(doc_id), i64v(seq), i64v(val)]);
    zset
}

fn total_for(client: &SyncClient<AppendEvent>, doc: i64) -> Option<i64> {
    let t = client.db().db().table("totals").unwrap();
    t.row_ids().find_map(|r| {
        if t.get(r, 0) == i64v(doc) {
            match t.get(r, 1) {
                CellValue::I64(v) => Some(v),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// THE acceptance case: reject while a foreign event is interleaved.
/// Target state after reject: base + foreign, WITHOUT the own pending —
/// a state that never existed before the reject.
#[test]
fn reject_with_interleaved_foreign_event_rebases() {
    let mut client = setup_client();
    let stream = client.create_stream();

    // 1. Base arrives (tail fetch): one confirmed event, total = 10.
    client.db_mut().apply_zset(&foreign_event(100, 1, 0, 10)).unwrap();
    assert_eq!(total_for(&client, 1), Some(10));

    // 2. Own optimistic edit: pending event, total jumps to 30.
    let request = client
        .execute(stream, AppendEvent { command_id: 101, doc_id: 1, seq: 1, val: 20 })
        .unwrap();
    assert_eq!(total_for(&client, 1), Some(30));
    // The command's client_zset is exactly its own log row — nothing else.
    assert_eq!(request.client_zset.entries.len(), 1);
    assert_eq!(request.client_zset.entries[0].table, "events");

    // 3. Foreign event lands WHILE ours is pending: total = 35.
    client.db_mut().apply_zset(&foreign_event(102, 1, 2, 5)).unwrap();
    assert_eq!(total_for(&client, 1), Some(35));

    // 4. Server rejects our pending command. The sync layer inverts the
    //    pending log row (disjoint — invert is safe again); the projection
    //    re-derives in the same batch.
    client
        .receive_response(CommandResponse {
            stream_id: stream,
            seq_no: request.seq_no,
            verdict: Verdict::Rejected { reason: "stale".into() },
        })
        .unwrap();

    // Base + foreign, without mine: 10 + 5.
    assert_eq!(total_for(&client, 1), Some(15));
    assert!(client.db_mut().take_projection_events().is_empty());
}

#[test]
fn confirm_echo_is_net_zero_on_derived_state() {
    let mut client = setup_client();
    let stream = client.create_stream();

    let request = client
        .execute(stream, AppendEvent { command_id: 100, doc_id: 1, seq: 0, val: 42 })
        .unwrap();
    assert_eq!(total_for(&client, 1), Some(42));

    // Confirm with the unchanged echo: invert(optimistic) + apply(echo)
    // must be net zero — on the log row AND on the derived table.
    client
        .receive_response(CommandResponse {
            stream_id: stream,
            seq_no: request.seq_no,
            verdict: Verdict::Confirmed { server_zset: request.client_zset.clone() },
        })
        .unwrap();

    assert_eq!(total_for(&client, 1), Some(42));
    let events = client.db().db().table("events").unwrap();
    assert_eq!(events.len(), 1);
    assert!(client.db_mut().take_projection_events().is_empty());
}

#[test]
fn reject_only_touches_own_document() {
    let mut client = setup_client();

    // Doc 2 has confirmed state.
    client.db_mut().apply_zset(&foreign_event(200, 2, 0, 7)).unwrap();

    // Own pending edit on doc 1, own stream (stream per document).
    let stream = client.create_stream();
    let request = client
        .execute(stream, AppendEvent { command_id: 101, doc_id: 1, seq: 0, val: 20 })
        .unwrap();
    assert_eq!(total_for(&client, 1), Some(20));
    assert_eq!(total_for(&client, 2), Some(7));

    client
        .receive_response(CommandResponse {
            stream_id: stream,
            seq_no: request.seq_no,
            verdict: Verdict::Rejected { reason: "nope".into() },
        })
        .unwrap();

    // Doc 1's derived state is fully gone (key death), doc 2 untouched.
    assert_eq!(total_for(&client, 1), None);
    assert_eq!(total_for(&client, 2), Some(7));
}
