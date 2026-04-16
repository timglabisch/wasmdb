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

use std::collections::{HashMap, HashSet};

use crate::reactive::registry::{SubId, SubscriptionRegistry};
use crate::storage::{CellValue, ZSet};

// ── Reactive tracing ─────────────────────────────────────────────────────

/// Describes one operation in the reactive execution tree.
#[derive(Debug, Clone)]
pub enum ReactiveSpanOperation {
    OnZSet { entries: usize },
    CheckMutation { table: String, weight: i32 },
    /// How candidates were found:
    /// - `by_value`: O(1) reverse-index lookup per column value
    /// - `by_table`: O(1) lookup, but subscription watches ALL mutations on the table
    Candidates { by_value: usize, by_table: usize, total: usize },
    Verify { candidates: usize, triggered: usize },
    /// Per-condition evaluation result — child of Verify.
    /// `cost`: `"O(1)"` = equality key extracted → found via reverse-index,
    ///         `"O(s)"` = no key → registered as table-level (s = subscriptions on table).
    ConditionEval { idx: usize, cost: &'static str, filter: String, matched: bool },
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
        for _ in 0..depth { out.push_str("  "); }
        match &self.operation {
            ReactiveSpanOperation::OnZSet { entries } =>
                writeln!(out, "OnZSet entries={entries}").unwrap(),
            ReactiveSpanOperation::CheckMutation { table, weight } =>
                writeln!(out, "CheckMutation table={table} weight={weight}").unwrap(),
            ReactiveSpanOperation::Candidates { by_value, by_table, total } =>
                writeln!(out, "Candidates by_value={by_value} by_table={by_table} total={total}").unwrap(),
            ReactiveSpanOperation::Verify { candidates, triggered } =>
                writeln!(out, "Verify candidates={candidates} triggered={triggered}").unwrap(),
            ReactiveSpanOperation::ConditionEval { idx, cost, filter, matched } =>
                writeln!(out, "Condition[{idx}] {cost} filter=({filter}) matched={matched}").unwrap(),
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
    run_stats: HashMap<usize, ConditionStats>,
    pub total_stats: HashMap<usize, ConditionStats>,
}

impl ReactiveContext {
    pub fn new() -> Self {
        Self {
            stack: Vec::new(),
            spans: Vec::new(),
            run_stats: HashMap::new(),
            total_stats: HashMap::new(),
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

// ── Execution ────────────────────────────────────────────────────────────

/// Process a ZSet against the registry — the primary integration point.
///
/// Iterates all entries in the ZSet and determines which subscriptions are
/// affected. Returns a map of SubId → set of triggered condition indices.
pub fn on_zset(
    registry: &SubscriptionRegistry,
    zset: &ZSet,
) -> HashMap<SubId, HashSet<usize>> {
    let mut ctx = ReactiveContext::new();
    on_zset_ctx(&mut ctx, registry, zset)
}

/// Like `on_zset`, but uses the provided `ReactiveContext` for tracing.
pub fn on_zset_ctx(
    ctx: &mut ReactiveContext,
    registry: &SubscriptionRegistry,
    zset: &ZSet,
) -> HashMap<SubId, HashSet<usize>> {
    ctx.run_stats.clear();
    ctx.span_with(|ctx| {
        let mut affected: HashMap<SubId, HashSet<usize>> = HashMap::new();
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
) -> HashMap<SubId, HashSet<usize>> {
    ctx.span(ReactiveSpanOperation::CheckMutation { table: table.to_string(), weight }, |ctx| {
        let candidate_set = candidates::collect(ctx, registry, table, row);
        verify::check(ctx, registry, candidate_set, table, row)
    })
}

/// Check which subscriptions are affected by an INSERT.
pub fn on_insert(registry: &SubscriptionRegistry, table: &str, new_row: &[CellValue]) -> Vec<SubId> {
    let mut ctx = ReactiveContext::new();
    check_mutation_ctx(&mut ctx, registry, table, new_row, 1).into_keys().collect()
}

/// Check which subscriptions are affected by a DELETE.
pub fn on_delete(registry: &SubscriptionRegistry, table: &str, old_row: &[CellValue]) -> Vec<SubId> {
    let mut ctx = ReactiveContext::new();
    check_mutation_ctx(&mut ctx, registry, table, old_row, -1).into_keys().collect()
}
