//! Reactive planning: type definitions, condition extraction, and optimization.
//!
//! Pipeline:
//!   `plan_reactive(ast, schemas)`
//!     ├─ `plan_select_ctx`            — reuse the SQL planner for source resolution
//!     ├─ `extract::extract_reactive_conditions` — AST REACTIVE() exprs → logical conditions
//!     └─ `optimize::optimize`         — logical → optimized (lookup keys + verify filter)

pub mod extract;
pub mod optimize;

use std::collections::HashMap;

use sql_parser::ast::{self, Value};

use crate::planner::PlanError;
use crate::planner::requirement::RequirementRegistry;
use crate::planner::shared::plan::{self, PlanFilterPredicate, PlanSourceEntry};
use crate::schema::TableSchema;

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
#[derive(Debug, Clone, PartialEq)]
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
    /// O(1) reverse-index lookup via one or more composite key sets.
    ///
    /// Each inner `Vec<ReactiveLookupKey>` is one composite key registered in the
    /// reverse index. Multiple sets arise from IN-list expansion:
    ///
    /// - `REACTIVE(id = 1)` → 1 key set: `[[(id, 1)]]`
    /// - `REACTIVE(id IN (1, 2))` → 2 key sets: `[[(id, 1)], [(id, 2)]]`
    /// - `REACTIVE(status = 'a' AND id IN (1, 2))` → 2 key sets (Cartesian product):
    ///   `[[(status, 'a'), (id, 1)], [(status, 'a'), (id, 2)]]`
    IndexLookup {
        lookup_key_sets: Vec<Vec<ReactiveLookupKey>>,
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
#[derive(Debug)]
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
                ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
                    let sets: Vec<String> = lookup_key_sets.iter().map(|keys| {
                        let parts: Vec<String> = keys.iter().map(|k| {
                            let col_name = plan::col_name(
                                &crate::planner::shared::plan::ColumnRef { source: cond.source_idx, col: k.col },
                                &self.sources,
                            );
                            format!("{col_name} = {}", plan::val(&k.value))
                        }).collect();
                        format!("[{}]", parts.join(", "))
                    }).collect();
                    if sets.len() == 1 {
                        format!("IndexLookup {}", sets[0])
                    } else {
                        format!("IndexLookup {} sets: {}", sets.len(), sets.join(", "))
                    }
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

// ── Entry point ──────────────────────────────────────────────────────────

/// Extract and optimize reactive conditions from an AstSelect.
///
/// Pipeline: `plan_select_ctx()` → `extract_reactive_conditions` → `optimize`.
/// Returns a `ReactivePlan` that bundles the optimized conditions with the
/// source schemas (for pretty-printing and inspection).
pub fn plan_reactive(
    ast: &ast::AstSelect,
    table_schemas: &HashMap<String, TableSchema>,
    requirements: &RequirementRegistry,
) -> Result<ReactivePlan, PlanError> {
    let mut ctx = crate::planner::make_plan_context(table_schemas, requirements);
    let main = crate::planner::plan_select_ctx(ast, &mut ctx)?;
    let logical = extract::extract_reactive_conditions(ast, &main)?;
    let conditions = optimize::optimize(logical);
    let sources = main.sources;
    Ok(ReactivePlan { conditions, sources })
}
