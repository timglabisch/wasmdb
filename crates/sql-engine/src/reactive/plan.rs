//! Reactive planning: type definitions, condition extraction, and optimization.

pub mod extract;
pub mod optimize;

use sql_parser::ast::Value;

use crate::planner::plan::{self, PlanFilterPredicate, PlanSourceEntry};

// ── Logical types (Phase 1: extraction output) ──────────────────────────

/// One logical reactive condition extracted from the AST.
/// Contains the raw filter predicate with no optimization applied.
#[derive(Debug, Clone)]
pub struct ReactiveCondition {
    pub table: String,
    pub kind: ReactiveConditionKind,
    pub source_idx: usize,
}

/// Whether a reactive condition is table-level or has a filter predicate.
#[derive(Debug, Clone)]
pub enum ReactiveConditionKind {
    /// Table-level: any change to the table triggers invalidation.
    /// Produced by `reactive(column_ref)`.
    TableLevel,
    /// Condition-level: the full predicate from the REACTIVE() expression.
    /// No optimization has been applied yet.
    Condition {
        filter: PlanFilterPredicate,
    },
}

// ── Optimized types (Phase 2: optimizer output) ─────────────────────────

/// A single equality predicate used for O(1) reverse-index lookup.
///
/// At plan time `value` may be a `Value::Placeholder`; after parameter binding
/// it becomes a concrete value (Int, Text, ...) that serves as hash key
/// in the `SubscriptionRegistry`'s reverse index.
#[derive(Debug, Clone)]
pub struct ReactiveLookupKey {
    pub col: usize,
    pub value: Value,
}

/// The lookup strategy chosen by the reactive optimizer.
#[derive(Debug, Clone)]
pub enum ReactiveLookupStrategy {
    /// No equality predicate could be extracted; every mutation on the table
    /// is checked against the verify filter.
    TableScan,
    /// One or more equality predicates extracted for O(1) reverse-index lookup.
    /// After candidate retrieval, the verify filter is always evaluated.
    IndexLookup {
        lookup_keys: Vec<ReactiveLookupKey>,
    },
}

/// A reactive condition after optimization: lookup strategy + verify filter.
///
/// The `verify_filter` is ALWAYS the full original predicate. Lookup keys are
/// an optimization for fast candidate retrieval, but correctness relies on
/// the verify filter being evaluated on every candidate.
#[derive(Debug, Clone)]
pub struct OptimizedReactiveCondition {
    pub table: String,
    pub source_idx: usize,
    pub strategy: ReactiveLookupStrategy,
    /// The full original predicate — always evaluated after candidate lookup.
    /// For TableLevel conditions this is `PlanFilterPredicate::None`.
    /// For Condition-level this is the complete predicate tree, never stripped.
    pub verify_filter: PlanFilterPredicate,
}

// ── Reactive plan (top-level) ─────────────────────────────────────────

/// The complete reactive plan — analogous to `ExecutionPlan` for SQL.
///
/// Bundles optimized conditions with the source schemas needed for
/// pretty-printing and inspection. Produced by `plan_reactive()`.
pub struct ReactivePlan {
    pub conditions: Vec<OptimizedReactiveCondition>,
    pub sources: Vec<PlanSourceEntry>,
}

impl ReactivePlan {
    pub fn pretty_print(&self) -> String {
        let mut out = String::new();
        if self.conditions.is_empty() {
            out.push_str("ReactivePlan (no conditions)\n");
            return out;
        }
        for (i, cond) in self.conditions.iter().enumerate() {
            let strategy = match &cond.strategy {
                ReactiveLookupStrategy::TableScan => "TableScan".to_string(),
                ReactiveLookupStrategy::IndexLookup { lookup_keys } => {
                    let keys: Vec<String> = lookup_keys.iter().map(|k| {
                        let col_name = plan::col_name(
                            &crate::planner::plan::ColumnRef { source: cond.source_idx, col: k.col },
                            &self.sources,
                        );
                        format!("{col_name} = {}", plan::val(&k.value))
                    }).collect();
                    format!("IndexLookup [{}]", keys.join(", "))
                }
            };
            out.push_str(&format!("Reactive[{i}] table={} strategy={strategy}\n", cond.table));

            if !matches!(cond.verify_filter, PlanFilterPredicate::None) {
                out.push_str("  verify: ");
                cond.verify_filter.pretty_print_to(&mut out, &self.sources);
                out.push('\n');
            }
        }
        out
    }
}
