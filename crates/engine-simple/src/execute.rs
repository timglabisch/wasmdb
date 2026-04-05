pub mod aggregate;
pub mod filter_batch;
pub mod filter_row;
pub mod join;
pub mod materialize;
pub mod pipeline;
pub mod project;
pub mod rowset;
pub mod scan;
pub mod sort;

use crate::storage::CellValue;
use query_engine::ast::Value;

pub use materialize::execute_plan;
pub use pipeline::execute;
pub use rowset::{RowSet, NULL_ROW};

pub type Column = Vec<CellValue>;
pub type Columns = Vec<Column>; // columns[col_idx][row_idx]

// ── Execution context ─────────────────────────────────────────────────────

/// Traces a single operation during query execution.
#[derive(Debug, Clone)]
pub enum TraceEvent {
    FullScan { table: String, rows: usize },
    IndexScan { table: String, index_columns: Vec<usize>, prefix_len: usize, rows: usize },
    Filter { rows_in: usize, rows_out: usize },
    Join { rows_out: usize },
    Aggregate { groups: usize },
    Sort { rows: usize },
    Project { columns: usize, rows: usize },
    Materialize { step: usize, rows: usize },
}

/// Threaded through all execution functions.
/// Collects trace events so callers can inspect the exact execution path.
pub struct ExecutionContext {
    pub trace: Vec<TraceEvent>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self { trace: Vec::new() }
    }
}

// ── Error ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ExecuteError {
    TableNotFound(String),
    MaterializeError(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::TableNotFound(t) => write!(f, "table not found: {t}"),
            ExecuteError::MaterializeError(msg) => write!(f, "subquery materialization error: {msg}"),
        }
    }
}

impl std::error::Error for ExecuteError {}

// ── Value conversion ──────────────────────────────────────────────────────

pub fn value_to_cell(v: &Value) -> CellValue {
    match v {
        Value::Int(n) => CellValue::I64(*n),
        Value::Text(s) => CellValue::Str(s.clone()),
        Value::Null => CellValue::Null,
        Value::Bool(b) => CellValue::I64(if *b { 1 } else { 0 }),
        Value::Float(f) => CellValue::I64(*f as i64),
    }
}

fn cell_to_value(cell: &CellValue) -> Value {
    match cell {
        CellValue::I64(n) => Value::Int(*n),
        CellValue::Str(s) => Value::Text(s.clone()),
        CellValue::Null => Value::Null,
    }
}
