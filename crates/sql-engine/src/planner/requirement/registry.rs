//! Caller metadata consumed by the SQL and Reactive planners.
//!
//! Parallel to `table_schemas`: an immutable lookup map passed as a plan
//! input. The planner uses it to resolve `schema.function(args)` FROM
//! sources into `PlanSource::Requirement`. Empty in contexts where no
//! callers are registered — matches the existing conventions for
//! schema-less tests and prepared statement planning.

use std::collections::HashMap;

use crate::planner::shared::plan::CallerId;
use crate::schema::DataType;

/// Caller registry — maps caller id (`"{schema}::{function}"`) to its
/// metadata. Kept as a plain `HashMap` alias on purpose: the SQL and
/// Reactive planners expect this shape identically to `table_schemas`.
pub type RequirementRegistry = HashMap<CallerId, RequirementMeta>;

/// Static metadata describing a single caller. Row shape and parameter
/// shape are all the planner needs to type-check a call site; the
/// executor will later pair this with an IO implementation.
#[derive(Debug, Clone)]
pub struct RequirementMeta {
    /// Base table into which this caller's rows are merged (PK-deduped
    /// across all callers with the same `row_table`).
    pub row_table: String,
    /// Positional parameter shape expected at the call site.
    pub params: Vec<RequirementParamDef>,
}

/// One positional parameter slot on a caller.
#[derive(Debug, Clone)]
pub struct RequirementParamDef {
    /// Name shown in error messages; not required to match anything.
    pub name: String,
    pub data_type: DataType,
}
