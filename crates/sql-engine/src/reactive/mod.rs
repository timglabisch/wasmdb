//! Reactive subscription system: planning, optimization, registry, and parameter binding.

pub mod plan;
pub mod extract;
pub mod optimize;
pub mod registry;
pub mod bind;

use std::collections::HashMap;

use sql_parser::ast;
use crate::planner::PlanError;
use crate::schema::TableSchema;

/// Entry-point: extract and optimize reactive conditions from an AstSelect.
///
/// Pipeline: plan_select_ctx() → extract conditions → optimize (extract lookup keys).
pub fn plan_reactive(
    ast: &ast::AstSelect,
    table_schemas: &HashMap<String, TableSchema>,
) -> Result<Vec<plan::OptimizedReactiveCondition>, PlanError> {
    let mut ctx = crate::planner::make_plan_context(table_schemas);
    let main = crate::planner::plan_select_ctx(ast, &mut ctx)?;
    let logical = extract::extract_reactive_conditions(ast, &main)?;
    Ok(optimize::optimize(logical))
}
