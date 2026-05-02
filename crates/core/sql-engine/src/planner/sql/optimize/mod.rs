//! Optimization passes on a built PlanSelect.
//!
//! Applied after AST translation, before execution.
//! Each pass is in its own submodule. New passes are added here.

pub(crate) mod physical;
mod pushdown;

use std::collections::HashMap;

use crate::schema::TableSchema;

use crate::planner::shared::plan::*;
use crate::planner::shared::optimize::or_to_in;

/// Run all optimization passes on a plan (order matters).
pub fn run(plan: &mut PlanSelect, table_schemas: &HashMap<String, TableSchema>) {
    or_to_in::rewrite(plan);
    pushdown::rewrite(plan);
    physical::rewrite(plan, table_schemas);
}
