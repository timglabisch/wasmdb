//! Reactive execution — the hot path.
//!
//! When a mutation (INSERT/UPDATE/DELETE) happens, the execute module determines
//! which subscriptions are affected. The pipeline has two phases:
//!
//! 1. **Candidates** (`candidates::collect`): O(1) reverse-index lookup to narrow
//!    down which subscriptions *might* be affected.
//! 2. **Verify** (`verify::check`): evaluate the full verify_filter predicate on
//!    each candidate to confirm it is actually affected.

pub mod candidates;
pub mod verify;

use fnv::{FnvHashMap, FnvHashSet};

use crate::reactive::identity::SubscriptionId;
use crate::reactive::registry::SubscriptionRegistry;
use crate::storage::{CellValue, ZSet};

// ── Reactive tracing ─────────────────────────────────────────────────────

/// Describes one operation in the reactive execution tree.
#[derive(Debug, Clone)]
pub enum ReactiveSpanOperation {
    OnZSet { entries: usize },
    CheckMutation { table: String, row: Vec<CellValue>, weight: i32 },
    /// Individual hash-index lookup: composite key values → which subs matched.
    HashLookup { key_values: Vec<CellValue>, hit_subs: Vec<SubscriptionId> },
    /// Table-level scan: subs watching the entire table.
    ScanLookup { hit_subs: Vec<SubscriptionId> },
    Verify { candidates: usize, triggered: usize },
    /// Per-condition evaluation result — child of Verify.
    ConditionEval { sub_id: SubscriptionId, idx: usize, filter: String, matched: bool },
}

/// A completed reactive span: operation + children.
#[derive(Debug, Clone)]
pub struct ReactiveSpan {
    pub operation: ReactiveSpanOperation,
    pub children: Vec<ReactiveSpan>,
}

impl ReactiveSpan {
    fn pretty_print_to(&self, out: &mut String, depth: usize) {
        use std::fmt::Write;
        let indent = |out: &mut String| { for _ in 0..depth { out.push_str("  "); } };
        match &self.operation {
            ReactiveSpanOperation::OnZSet { entries } => {
                indent(out);
                writeln!(out, "OnZSet {entries} mutations").unwrap();
            }
            ReactiveSpanOperation::CheckMutation { table, row, weight } => {
                indent(out);
                let op = if *weight >= 0 { "INSERT" } else { "DELETE" };
                let row_str = format_row(row);
                writeln!(out, "{op} {table} {row_str}").unwrap();
            }
            ReactiveSpanOperation::HashLookup { key_values, hit_subs } => {
                indent(out);
                let key_str = format_row(key_values);
                if hit_subs.is_empty() {
                    writeln!(out, "Hash {key_str} --> miss").unwrap();
                } else {
                    let subs: Vec<String> = hit_subs.iter().map(|s| format!("Sub({})", s.0)).collect();
                    writeln!(out, "Hash {key_str} --> {}", subs.join(", ")).unwrap();
                }
            }
            ReactiveSpanOperation::ScanLookup { hit_subs } => {
                indent(out);
                if hit_subs.is_empty() {
                    writeln!(out, "Scan --> miss").unwrap();
                } else {
                    let subs: Vec<String> = hit_subs.iter().map(|s| format!("Sub({})", s.0)).collect();
                    writeln!(out, "Scan --> {}", subs.join(", ")).unwrap();
                }
            }
            ReactiveSpanOperation::Verify { candidates, triggered } => {
                // Skip verify header when no candidates
                if *candidates == 0 { return; }
                indent(out);
                writeln!(out, "Verify {triggered}/{candidates} triggered").unwrap();
            }
            ReactiveSpanOperation::ConditionEval { sub_id, idx, filter, matched } => {
                indent(out);
                writeln!(out, "Sub({}) Condition[{idx}] ({filter}) --> {matched}", sub_id.0).unwrap();
            }
        }
        for child in &self.children {
            child.pretty_print_to(out, depth + 1);
        }
    }
}

/// Per-condition evaluation counter.
#[derive(Debug, Clone, Default)]
pub struct ConditionStats {
    pub evaluated: usize,
    pub matched: usize,
}

/// In-flight span on the context stack (children accumulate here).
struct OpenReactiveSpan {
    children: Vec<ReactiveSpan>,
}

/// Threaded through all reactive execution functions.
/// Builds a tree of [`ReactiveSpan`]s — same pattern as `ExecutionContext`.
///
/// Also tracks per-condition counters:
/// - `run_stats`: reset per `on_zset_ctx` call (current ZSet)
/// - `total_stats`: accumulated across all calls on this context
pub struct ReactiveContext {
    stack: Vec<OpenReactiveSpan>,
    pub spans: Vec<ReactiveSpan>,
    run_stats: FnvHashMap<usize, ConditionStats>,
    pub total_stats: FnvHashMap<usize, ConditionStats>,
}

impl ReactiveContext {
    pub fn new() -> Self {
        Self {
            stack: Vec::new(),
            spans: Vec::new(),
            run_stats: FnvHashMap::default(),
            total_stats: FnvHashMap::default(),
        }
    }

    fn close_span(&mut self, op: ReactiveSpanOperation) {
        let open = self.stack.pop().expect("reactive span stack underflow");
        let span = ReactiveSpan {
            operation: op,
            children: open.children,
        };
        match self.stack.last_mut() {
            Some(parent) => parent.children.push(span),
            None => self.spans.push(span),
        }
    }

    /// Wrap work in a span. Use when the operation is fully known upfront.
    pub fn span<T>(&mut self, op: ReactiveSpanOperation, f: impl FnOnce(&mut Self) -> T) -> T {
        self.stack.push(OpenReactiveSpan { children: Vec::new() });
        let result = f(self);
        self.close_span(op);
        result
    }

    /// Wrap work in a span. The closure returns `(ReactiveSpanOperation, T)` —
    /// use when operation details are only known after the work.
    pub fn span_with<T>(&mut self, f: impl FnOnce(&mut Self) -> (ReactiveSpanOperation, T)) -> T {
        self.stack.push(OpenReactiveSpan { children: Vec::new() });
        let (op, result) = f(self);
        self.close_span(op);
        result
    }

    /// Record a condition evaluation (called by verify).
    pub(crate) fn record_condition(&mut self, idx: usize, matched: bool) {
        let run = self.run_stats.entry(idx).or_default();
        run.evaluated += 1;
        if matched { run.matched += 1; }

        let total = self.total_stats.entry(idx).or_default();
        total.evaluated += 1;
        if matched { total.matched += 1; }
    }

    /// Pretty-print the span tree (deterministic, no timing).
    /// Appends per-condition stats after each OnZSet span.
    pub fn pretty_print(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        for span in &self.spans {
            span.pretty_print_to(&mut out, 0);
            if matches!(span.operation, ReactiveSpanOperation::OnZSet { .. }) {
                // Per-condition stats (sorted by index)
                let mut indices: Vec<usize> = self.run_stats.keys().copied().collect();
                indices.extend(self.total_stats.keys());
                indices.sort_unstable();
                indices.dedup();
                for idx in &indices {
                    let run = self.run_stats.get(idx);
                    let total = self.total_stats.get(idx);
                    let (r_eval, r_match) = run.map_or((0, 0), |s| (s.evaluated, s.matched));
                    let (t_eval, t_match) = total.map_or((0, 0), |s| (s.evaluated, s.matched));
                    writeln!(out, "  Condition[{idx}]: run={r_eval}/{r_match} total={t_eval}/{t_match}").unwrap();
                }
            }
        }
        out
    }
}

fn format_row(row: &[CellValue]) -> String {
    let vals: Vec<String> = row.iter().map(|v| match v {
        CellValue::I64(n) => n.to_string(),
        CellValue::Str(s) => format!("'{s}'"),
        CellValue::Null => "NULL".to_string(),
    }).collect();
    format!("[{}]", vals.join(", "))
}

// ── Execution ────────────────────────────────────────────────────────────

/// Process a ZSet against the registry — the primary integration point.
///
/// Iterates all entries in the ZSet and determines which subscriptions are
/// affected. Returns a map of SubscriptionId → set of triggered condition indices.
pub fn on_zset(
    registry: &SubscriptionRegistry,
    zset: &ZSet,
) -> FnvHashMap<SubscriptionId, FnvHashSet<usize>> {
    let mut ctx = ReactiveContext::new();
    on_zset_ctx(&mut ctx, registry, zset)
}

/// Like `on_zset`, but uses the provided `ReactiveContext` for tracing.
pub fn on_zset_ctx(
    ctx: &mut ReactiveContext,
    registry: &SubscriptionRegistry,
    zset: &ZSet,
) -> FnvHashMap<SubscriptionId, FnvHashSet<usize>> {
    ctx.run_stats.clear();
    ctx.span_with(|ctx| {
        let mut affected: FnvHashMap<SubscriptionId, FnvHashSet<usize>> = FnvHashMap::default();
        for entry in &zset.entries {
            let mutations = check_mutation_ctx(ctx, registry, &entry.table, &entry.row, entry.weight);
            for (sub_id, indices) in mutations {
                affected.entry(sub_id).or_default().extend(indices);
            }
        }
        (ReactiveSpanOperation::OnZSet { entries: zset.entries.len() }, affected)
    })
}

fn check_mutation_ctx(
    ctx: &mut ReactiveContext,
    registry: &SubscriptionRegistry,
    table: &str,
    row: &[CellValue],
    weight: i32,
) -> FnvHashMap<SubscriptionId, FnvHashSet<usize>> {
    ctx.span(ReactiveSpanOperation::CheckMutation { table: table.to_string(), row: row.to_vec(), weight }, |ctx| {
        let candidate_set = candidates::collect(ctx, registry, table, row);
        verify::check(ctx, registry, candidate_set, table, row)
    })
}

/// Check which subscriptions are affected by an INSERT.
pub fn on_insert(registry: &SubscriptionRegistry, table: &str, new_row: &[CellValue]) -> Vec<SubscriptionId> {
    let mut ctx = ReactiveContext::new();
    check_mutation_ctx(&mut ctx, registry, table, new_row, 1).into_keys().collect()
}

/// Check which subscriptions are affected by a DELETE.
pub fn on_delete(registry: &SubscriptionRegistry, table: &str, old_row: &[CellValue]) -> Vec<SubscriptionId> {
    let mut ctx = ReactiveContext::new();
    check_mutation_ctx(&mut ctx, registry, table, old_row, -1).into_keys().collect()
}
