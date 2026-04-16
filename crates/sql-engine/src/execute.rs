pub mod aggregate;
pub mod bind;
pub mod filter_batch;
pub mod filter_row;
pub mod join;
pub mod materialize;
pub mod pipeline;
pub mod project;
pub mod rowset;
pub mod scan;
pub mod sort;

use std::collections::HashMap;
use std::time::Duration;

use fnv::FnvHashSet;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(all(target_arch = "wasm32", feature = "wasm-timing"))]
use web_time::Instant;

#[cfg(all(target_arch = "wasm32", not(feature = "wasm-timing")))]
struct Instant;

#[cfg(all(target_arch = "wasm32", not(feature = "wasm-timing")))]
impl Instant {
    fn now() -> Self { Instant }
    fn elapsed(&self) -> Duration { Duration::ZERO }
}

use crate::storage::{CellValue, Table};
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

pub use materialize::execute_plan;
pub use pipeline::execute;
pub use bind::{resolve_params, resolve_filter, resolve_value};
pub use rowset::{RowSet, NULL_ROW};

pub type Column = Vec<CellValue>;
pub type Columns = Vec<Column>; // columns[col_idx][row_idx]

// ── Execution context with span-based tracing ─────────────────────────────

/// How a table scan was performed.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum ScanMethod {
    Full,
    Index { columns: Vec<usize>, prefix_len: usize, is_hash: bool },
}

/// Describes one operation in the execution tree.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum SpanOperation {
    Execute,
    Materialize { step: usize },
    Scan { table: String, method: ScanMethod, rows: usize },
    Filter { rows_in: usize, rows_out: usize },
    Join { rows_out: usize },
    Aggregate { groups: usize },
    Sort { rows: usize },
    Project { columns: usize, rows: usize },
}

/// A completed span: operation + duration + children.
#[derive(Debug, Clone)]
pub struct Span {
    pub operation: SpanOperation,
    pub duration: Duration,
    pub children: Vec<Span>,
}

#[cfg(feature = "serde")]
impl serde::Serialize for Span {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = s.serialize_struct("Span", 3)?;
        state.serialize_field("operation", &self.operation)?;
        state.serialize_field("duration_us", &(self.duration.as_micros() as u64))?;
        state.serialize_field("children", &self.children)?;
        state.end()
    }
}

/// In-flight span on the context stack (children accumulate here).
struct OpenSpan {
    start: Instant,
    children: Vec<Span>,
}

/// Threaded through all execution functions as first parameter.
/// Builds a tree of [`Span`]s with timing information.
pub struct ExecutionContext<'execution> {
    pub db: &'execution HashMap<String, Table>,
    stack: Vec<OpenSpan>,
    pub spans: Vec<Span>,
    pub params: Params,
    /// Which reactive condition indices were triggered (for SELECT reactive() output).
    pub triggered_conditions: Option<FnvHashSet<usize>>,
}

impl<'execution> ExecutionContext<'execution> {
    pub fn new(db: &'execution HashMap<String, Table>) -> Self {
        Self { db, stack: Vec::new(), spans: Vec::new(), params: HashMap::new(), triggered_conditions: None }
    }

    pub fn with_params(db: &'execution HashMap<String, Table>, params: Params) -> Self {
        Self { db, stack: Vec::new(), spans: Vec::new(), params, triggered_conditions: None }
    }

    fn close_span(&mut self, op: SpanOperation) {
        let open = self.stack.pop().expect("span stack underflow");
        let span = Span {
            operation: op,
            duration: open.start.elapsed(),
            children: open.children,
        };
        match self.stack.last_mut() {
            Some(parent) => parent.children.push(span),
            None => self.spans.push(span),
        }
    }

    /// Wrap work in a span. Use when the operation is fully known upfront.
    pub fn span<T>(&mut self, op: SpanOperation, f: impl FnOnce(&mut Self) -> T) -> T {
        self.stack.push(OpenSpan { start: Instant::now(), children: Vec::new() });
        let result = f(self);
        self.close_span(op);
        result
    }

    /// Wrap work in a span. The closure returns `(SpanOperation, T)` —
    /// use when operation details (e.g. row counts) are only known after the work.
    pub fn span_with<T>(&mut self, f: impl FnOnce(&mut Self) -> (SpanOperation, T)) -> T {
        self.stack.push(OpenSpan { start: Instant::now(), children: Vec::new() });
        let (op, result) = f(self);
        self.close_span(op);
        result
    }

    /// Pretty-print the span tree (without timing — for deterministic snapshot tests).
    pub fn pretty_print(&self) -> String {
        let mut out = String::new();
        for span in &self.spans {
            span.pretty_print_to(&mut out, 0);
        }
        out
    }
}

impl Span {
    fn pretty_print_to(&self, out: &mut String, depth: usize) {
        use std::fmt::Write;
        for _ in 0..depth { out.push_str("  "); }
        match &self.operation {
            SpanOperation::Execute => writeln!(out, "Execute").unwrap(),
            SpanOperation::Materialize { step } => writeln!(out, "Materialize step={step}").unwrap(),
            SpanOperation::Scan { table, method, rows } => {
                let m = match method {
                    ScanMethod::Full => "Full".to_string(),
                    ScanMethod::Index { columns, prefix_len, is_hash } => {
                        let kind = if *is_hash { "Hash" } else { "BTree" };
                        format!("{kind}({columns:?} prefix={prefix_len})")
                    }
                };
                writeln!(out, "Scan table={table} method={m} rows={rows}").unwrap();
            }
            SpanOperation::Filter { rows_in, rows_out } =>
                writeln!(out, "Filter rows_in={rows_in} rows_out={rows_out}").unwrap(),
            SpanOperation::Join { rows_out } =>
                writeln!(out, "Join rows_out={rows_out}").unwrap(),
            SpanOperation::Aggregate { groups } =>
                writeln!(out, "Aggregate groups={groups}").unwrap(),
            SpanOperation::Sort { rows } =>
                writeln!(out, "Sort rows={rows}").unwrap(),
            SpanOperation::Project { columns, rows } =>
                writeln!(out, "Project columns={columns} rows={rows}").unwrap(),
        }
        for child in &self.children {
            child.pretty_print_to(out, depth + 1);
        }
    }
}

// ── Error ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ExecuteError {
    TableNotFound(String),
    MaterializeError(String),
    BindError(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::TableNotFound(t) => write!(f, "table not found: {t}"),
            ExecuteError::MaterializeError(msg) => write!(f, "subquery materialization error: {msg}"),
            ExecuteError::BindError(msg) => write!(f, "bind error: {msg}"),
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

fn cell_to_value(cell: &CellValue) -> Value {
    match cell {
        CellValue::I64(n) => Value::Int(*n),
        CellValue::Str(s) => Value::Text(s.clone()),
        CellValue::Null => Value::Null,
    }
}
