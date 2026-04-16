//! Reactive planning: type definitions, condition extraction, and optimization.

pub mod extract;
pub mod optimize;

use sql_parser::ast::Value;

use crate::planner::plan::PlanFilterPredicate;

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
