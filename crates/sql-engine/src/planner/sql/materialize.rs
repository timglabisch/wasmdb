//! Subquery materialization planning.
//!
//! When the planner encounters a subquery (IN subquery or scalar comparison),
//! it recursively plans the subquery and registers a materialization step
//! in the [`PlanContext`]. The main query gets a placeholder predicate
//! (`InMaterialized` / `CompareMaterialized`) that is resolved before execution.

use sql_parser::ast;

use crate::planner::plan::*;
use crate::planner::PlanContext;
use crate::planner::PlanError;
use super::plan::MaterializeKind;

/// Plan an IN subquery: `col IN (SELECT ...)`.
/// Recursively plans the subquery, registers it as a List materialization,
/// and returns an `InMaterialized` placeholder predicate.
pub fn plan_in_subquery(
    col: ColumnRef,
    subquery: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanFilterPredicate, PlanError> {
    let subquery_plan = crate::planner::plan_select_ctx(subquery, ctx)?;
    let mat_id = ctx.add_materialization(subquery_plan, MaterializeKind::List);
    Ok(PlanFilterPredicate::InMaterialized { col, mat_id })
}

/// Plan a scalar subquery comparison: `col op (SELECT ...)`.
/// Recursively plans the subquery, registers it as a Scalar materialization,
/// and returns a `CompareMaterialized` placeholder predicate.
pub fn plan_scalar_subquery(
    col: ColumnRef,
    op: ast::Operator,
    subquery: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanFilterPredicate, PlanError> {
    let subquery_plan = crate::planner::plan_select_ctx(subquery, ctx)?;
    let mat_id = ctx.add_materialization(subquery_plan, MaterializeKind::Scalar);
    Ok(PlanFilterPredicate::CompareMaterialized { col, op, mat_id })
}
