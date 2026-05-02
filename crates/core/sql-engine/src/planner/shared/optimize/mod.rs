//! Shared optimizer passes — work on `PlanFilterPredicate` independent of
//! whether the predicate came from a SQL `WHERE` clause or a `REACTIVE(...)`
//! expression. Both the SQL and reactive optimizers compose these passes
//! with their domain-specific ones.

pub(crate) mod or_to_in;
