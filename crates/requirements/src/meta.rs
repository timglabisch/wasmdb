//! Static metadata describing a single requirement (formerly "caller").
//!
//! This is what the planner used to consult to type-check `schema.fn(args)`
//! call sources. Today we keep `row_table` and `params` — further policy
//! fields (e.g. `stale_time_ms`, `retry`, slot-lifecycle hints) are
//! deliberately omitted per the design's Cut 1 scope and re-added when a
//! concrete frontend pain motivates them.

use sql_engine::schema::DataType;

/// Static metadata describing a single requirement. Row shape and
/// parameter shape are all the planner-side type-check needs; the
/// runtime pairs this with an IO implementation (`RequirementFn`).
#[derive(Debug, Clone)]
pub struct RequirementMeta {
    /// Base table into which this requirement's rows are merged
    /// (PK-deduped across all requirements with the same `row_table`).
    pub row_table: String,
    /// Positional parameter shape expected at the call site.
    pub params: Vec<RequirementParamDef>,
}

/// One positional parameter slot on a requirement.
#[derive(Debug, Clone)]
pub struct RequirementParamDef {
    /// Name shown in error messages; not required to match anything.
    pub name: String,
    pub data_type: DataType,
}
