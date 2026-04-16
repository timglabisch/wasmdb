//! SQL-specific planning: execution plans, subquery materialization, optimization.

pub mod plan;
pub(crate) mod optimize;
pub(crate) mod materialize;

use std::collections::HashMap;

use sql_parser::ast;
use crate::schema::TableSchema;
use crate::planner::PlanError;
use crate::planner::plan::PlanSelect;

/// Translate an AstSelect into an ExecutionPlan with materialization steps for subqueries.
pub fn plan(
    ast: &ast::AstSelect,
    table_schemas: &HashMap<String, TableSchema>,
) -> Result<plan::ExecutionPlan, PlanError> {
    let mut ctx = crate::planner::make_plan_context(table_schemas);
    let main = crate::planner::plan_select_ctx(ast, &mut ctx)?;
    Ok(plan::ExecutionPlan {
        materializations: ctx.materializations,
        main,
    })
}

/// Translate an AstSelect into a PlanSelect (convenience wrapper).
/// Errors if the AST contains subqueries — use `plan()` instead.
pub fn plan_select(
    select: &ast::AstSelect,
    table_schemas: &HashMap<String, TableSchema>,
) -> Result<PlanSelect, PlanError> {
    let ep = plan(select, table_schemas)?;
    if !ep.materializations.is_empty() {
        return Err(PlanError::UnsupportedExpr(
            "unexpected subqueries; use plan() instead".into(),
        ));
    }
    Ok(ep.main)
}
