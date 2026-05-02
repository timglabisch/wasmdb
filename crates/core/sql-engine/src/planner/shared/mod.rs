//! Shared planner primitives used by both the SQL and reactive planners.
//!
//! - `plan`: core plan types (`PlanFilterPredicate`, `PlanSelect`, `ColumnRef`, …)
//! - `translate`: AST → raw plan translation
//! - `optimize`: predicate-level passes that are domain-agnostic (e.g. `or_to_in`)

pub mod plan;
pub mod translate;
pub mod optimize;
