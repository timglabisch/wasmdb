//! Execution context with span-based tracing.
//!
//! `ExecutionContext` is threaded through every execution function as the
//! first parameter. It owns the DB reference, the span stack for timing, and
//! — after Phase 0 — the `RequirementsResult` that Phase 3's scan_requirement
//! consults for PK tuples.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

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

use sql_parser::ast::Value;

use crate::storage::Table;

use super::{Params, RequirementsResult};

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
    pub triggered_conditions: Option<HashSet<usize>>,
    /// Values for internal placeholders that the planner injected from
    /// auto-platzhalterisierten Caller-Args. Populated from
    /// `ExecutionPlan.bound_values` in `execute_plan`.
    pub bound_values: HashMap<String, Value>,
    /// Resolved requirements produced by Phase 0. Phase 3's
    /// `scan_requirement` reads PK tuples from here. Empty when the plan
    /// contains no `PlanSource::Requirement` entries.
    pub requirements: RequirementsResult,
}

impl<'execution> ExecutionContext<'execution> {
    pub fn new(db: &'execution HashMap<String, Table>) -> Self {
        Self {
            db, stack: Vec::new(), spans: Vec::new(),
            params: HashMap::new(), triggered_conditions: None,
            bound_values: HashMap::new(), requirements: RequirementsResult::default(),
        }
    }

    pub fn with_params(db: &'execution HashMap<String, Table>, params: Params) -> Self {
        Self {
            db, stack: Vec::new(), spans: Vec::new(),
            params, triggered_conditions: None,
            bound_values: HashMap::new(), requirements: RequirementsResult::default(),
        }
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
