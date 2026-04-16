//! Reactive subscription system: planning, optimization, registry, and parameter binding.

pub mod plan;
pub mod execute;
pub mod registry;

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
    let logical = plan::extract::extract_reactive_conditions(ast, &main)?;
    Ok(plan::optimize::optimize(logical))
}
