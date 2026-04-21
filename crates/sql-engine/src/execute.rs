pub mod aggregate;
pub mod bind;
pub mod context;
pub mod filter_batch;
pub mod filter_row;
pub mod join;
pub mod materialize;
pub mod pipeline;
pub mod project;
pub mod requirement;
pub mod rowset;
pub mod scan;
pub mod sort;

use std::collections::HashMap;

use crate::storage::CellValue;
use sql_parser::ast::Value;

// ── Prepared statement parameters ────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ParamValue {
    Int(i64),
    Text(String),
    Null,
    IntList(Vec<i64>),
    TextList(Vec<String>),
}

pub type Params = HashMap<String, ParamValue>;

pub type Column = Vec<CellValue>;
pub type Columns = Vec<Column>; // columns[col_idx][row_idx]

// ── Re-exports ────────────────────────────────────────────────────────────

pub use bind::{resolve_filter, resolve_params, resolve_value};
pub use context::{ExecutionContext, ScanMethod, Span, SpanOperation};
pub use materialize::execute_plan;
pub use pipeline::execute;
pub use requirement::{
    execute_and_resolve_requirements, resolve_requirements,
    AsyncFetcherFn, FetcherFuture, FetcherRuntime, RequirementKey, RequirementsResult,
};
pub use rowset::{RowSet, NULL_ROW};

// ── Error ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ExecuteError {
    TableNotFound(String),
    MaterializeError(String),
    BindError(String),
    NotImplemented(String),
    /// Caller wasn't registered, args didn't resolve, closure returned an
    /// error, or a PK the caller produced isn't present in `row_table`.
    CallerError(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::TableNotFound(t) => write!(f, "table not found: {t}"),
            ExecuteError::MaterializeError(msg) => write!(f, "subquery materialization error: {msg}"),
            ExecuteError::BindError(msg) => write!(f, "bind error: {msg}"),
            ExecuteError::NotImplemented(msg) => write!(f, "not implemented: {msg}"),
            ExecuteError::CallerError(msg) => write!(f, "caller error: {msg}"),
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
        Value::Placeholder(name) => panic!("unresolved placeholder :{name} — must bind before execution"),
    }
}

pub(crate) fn cell_to_value(cell: &CellValue) -> Value {
    match cell {
        CellValue::I64(n) => Value::Int(*n),
        CellValue::Str(s) => Value::Text(s.clone()),
        CellValue::Null => Value::Null,
    }
}
