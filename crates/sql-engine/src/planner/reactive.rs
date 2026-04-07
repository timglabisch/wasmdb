use sql_parser::ast::Value;
use super::plan::PlanFilterPredicate;

/// An equality predicate that becomes a reverse-index key.
#[derive(Debug, Clone)]
pub struct InvalidationKey {
    pub table: String,
    pub col: usize,
    pub value: Value,
}

/// One INVALIDATE_ON condition, decomposed into index keys + verify filter.
#[derive(Debug, Clone)]
pub struct InvalidationCondition {
    pub table: String,
    pub index_keys: Vec<InvalidationKey>,
    pub verify_filter: PlanFilterPredicate,
    pub source_idx: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationStrategy {
    ReExecute,
    Invalidate,
}

#[derive(Debug, Clone)]
pub struct ReactiveMetadata {
    pub conditions: Vec<InvalidationCondition>,
    pub strategy: InvalidationStrategy,
}
