//! `#[rpc_command(append_to = <LogRow>)]` with a `#[partition]` field
//! marker — the generated `execute_optimistic` appends exactly one log
//! row: command_id from the command's `id`, provisional seq = max(seq of
//! the partition) + 1, committed = 0, payload = the command's JSON wire
//! form.

use database::Database;
use rpc_command::rpc_command;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sql_engine::DbTable;
use sync::command::Command;

/// Log row in the conventional shape (hand-implemented `DbTable` so this
/// test doesn't depend on the tables macros).
#[derive(Debug, Clone)]
pub struct DraftEventRow {
    pub command_id: i64,
    pub doc_id: i64,
    pub seq: i64,
    pub committed: i64,
    pub payload: String,
}

impl DbTable for DraftEventRow {
    const TABLE: &'static str = "draft_event_row";

    fn schema() -> TableSchema {
        let col = |name: &str, data_type| ColumnSchema {
            name: name.into(),
            data_type,
            nullable: false,
        };
        TableSchema {
            name: Self::TABLE.into(),
            columns: vec![
                col("command_id", DataType::I64),
                col("doc_id", DataType::I64),
                col("seq", DataType::I64),
                col("committed", DataType::I64),
                col("payload", DataType::String),
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn into_cells(self) -> Vec<CellValue> {
        vec![
            CellValue::I64(self.command_id),
            CellValue::I64(self.doc_id),
            CellValue::I64(self.seq),
            CellValue::I64(self.committed),
            CellValue::Str(self.payload),
        ]
    }

    fn from_cells(cells: &[CellValue]) -> Result<Self, String> {
        let int = |i: usize| match cells.get(i) {
            Some(CellValue::I64(v)) => Ok(*v),
            other => Err(format!("cell {i}: expected I64, got {other:?}")),
        };
        let text = |i: usize| match cells.get(i) {
            Some(CellValue::Str(v)) => Ok(v.clone()),
            other => Err(format!("cell {i}: expected Str, got {other:?}")),
        };
        Ok(Self {
            command_id: int(0)?,
            doc_id: int(1)?,
            seq: int(2)?,
            committed: int(3)?,
            payload: text(4)?,
        })
    }
}

#[rpc_command(append_to = DraftEventRow)]
pub struct SetLinePrice {
    pub id: i64,
    #[partition]
    pub doc_id: i64,
    pub price_cents: i64,
}

fn setup_db() -> Database {
    let mut db = Database::new();
    db.register_table::<DraftEventRow>().unwrap();
    db
}

fn log_rows(db: &Database) -> Vec<DraftEventRow> {
    let t = db.table(DraftEventRow::TABLE).unwrap();
    let ncols = t.schema.columns.len();
    let mut rows: Vec<DraftEventRow> = t
        .row_ids()
        .map(|r| {
            let cells: Vec<CellValue> = (0..ncols).map(|c| t.get(r, c)).collect();
            DraftEventRow::from_cells(&cells).unwrap()
        })
        .collect();
    rows.sort_by_key(|r| (r.doc_id, r.seq));
    rows
}

#[test]
fn append_emits_exactly_one_uncommitted_log_row() {
    let mut db = setup_db();
    let cmd = SetLinePrice { id: 100, doc_id: 1, price_cents: 1500 };

    let zset = cmd.execute_optimistic(&mut db).unwrap();

    assert_eq!(zset.entries.len(), 1);
    assert_eq!(zset.entries[0].table, DraftEventRow::TABLE);
    assert_eq!(zset.entries[0].weight, 1);

    let rows = log_rows(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].command_id, 100);
    assert_eq!(rows[0].doc_id, 1);
    assert_eq!(rows[0].seq, 0);
    assert_eq!(rows[0].committed, 0);

    // Payload is the command's own wire form — deserializable back.
    let back: SetLinePrice = serde_json::from_str(&rows[0].payload).unwrap();
    assert_eq!(back.id, 100);
    assert_eq!(back.price_cents, 1500);
}

#[test]
fn provisional_seq_counts_per_partition() {
    let mut db = setup_db();
    SetLinePrice { id: 100, doc_id: 1, price_cents: 10 }
        .execute_optimistic(&mut db)
        .unwrap();
    SetLinePrice { id: 101, doc_id: 1, price_cents: 20 }
        .execute_optimistic(&mut db)
        .unwrap();
    // Another document starts its own sequence.
    SetLinePrice { id: 102, doc_id: 2, price_cents: 30 }
        .execute_optimistic(&mut db)
        .unwrap();

    let rows = log_rows(&db);
    let seqs: Vec<(i64, i64)> = rows.iter().map(|r| (r.doc_id, r.seq)).collect();
    assert_eq!(seqs, vec![(1, 0), (1, 1), (2, 0)]);
}
