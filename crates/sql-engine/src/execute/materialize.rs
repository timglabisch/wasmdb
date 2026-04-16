//! Subquery materialization: resolves InMaterialized/CompareMaterialized
//! placeholders by executing subquery plans bottom-up before the main query.

use std::collections::HashMap;

use crate::planner::sql::optimize;
use crate::planner::plan::*;
use crate::planner::sql::plan::{ExecutionPlan, MaterializeStep, MaterializeKind};
use sql_parser::ast::{Operator, Value};
use crate::schema::TableSchema;

use super::bind;
use super::pipeline::execute;
use super::{cell_to_value, Columns, ExecuteError, ExecutionContext, SpanOperation};

/// Execute an ExecutionPlan: resolve params, run materializations, then run main query.
pub fn execute_plan(
    ctx: &mut ExecutionContext,
    plan: &ExecutionPlan,
) -> Result<Columns, ExecuteError> {
    let plan = &bind::resolve_plan_params(plan, &ctx.params)?;
    let main = resolve_materializations(ctx, plan)?;
    execute(ctx, &main)
}

/// Execute subquery materializations bottom-up, then resolve placeholders in the main query.
fn resolve_materializations(
    ctx: &mut ExecutionContext,
    plan: &ExecutionPlan,
) -> Result<PlanSelect, ExecuteError> {
    if plan.materializations.is_empty() {
        return Ok(plan.main.clone());
    }

    let table_schemas: HashMap<String, TableSchema> = ctx.db.iter()
        .map(|(name, table)| (name.clone(), table.schema.clone()))
        .collect();

    let materialized = execute_materialization_steps(ctx, &plan.materializations, &table_schemas)?;
    Ok(resolve_materialized(&plan.main, &materialized, &table_schemas))
}

/// Execute each materialization step bottom-up, collecting result values.
fn execute_materialization_steps(
    ctx: &mut ExecutionContext,
    steps: &[MaterializeStep],
    table_schemas: &HashMap<String, TableSchema>,
) -> Result<Vec<Vec<Value>>, ExecuteError> {
    let mut materialized: Vec<Vec<Value>> = Vec::new();

    for (i, step) in steps.iter().enumerate() {
        let values = ctx.span(SpanOperation::Materialize { step: i }, |ctx| {
            let resolved = resolve_materialized(&step.plan, &materialized, table_schemas);
            let result = execute(ctx, &resolved)?;

            if result.len() != 1 {
                return Err(ExecuteError::MaterializeError(
                    format!("subquery must return 1 column, got {}", result.len()),
                ));
            }
            if matches!(step.kind, MaterializeKind::Scalar) && result[0].len() != 1 {
                return Err(ExecuteError::MaterializeError(
                    format!("scalar subquery must return 1 row, got {}", result[0].len()),
                ));
            }

            Ok(result[0].iter().map(cell_to_value).collect::<Vec<_>>())
        })?;
        materialized.push(values);
    }

    Ok(materialized)
}

fn resolve_materialized(
    plan: &PlanSelect,
    materialized: &[Vec<Value>],
    table_schemas: &HashMap<String, TableSchema>,
) -> PlanSelect {
    let mut resolved = plan.clone();
    resolved.filter = resolve_materialized_filter(&resolved.filter, materialized);
    for source in &mut resolved.sources {
        source.pre_filter = resolve_materialized_filter(&source.pre_filter, materialized);
        if let Some(ref mut join) = source.join {
            join.on = resolve_materialized_filter(&join.on, materialized);
        }
    }
    // Re-run all optimization passes on resolved predicates.
    optimize::run(&mut resolved, table_schemas);
    resolved
}

fn resolve_materialized_filter(
    pred: &PlanFilterPredicate,
    materialized: &[Vec<Value>],
) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::InMaterialized { col, mat_id } => {
            PlanFilterPredicate::In { col: *col, values: materialized[*mat_id].clone() }
        }
        PlanFilterPredicate::CompareMaterialized { col, op, mat_id } => {
            let value = materialized[*mat_id][0].clone();
            match op {
                Operator::Eq => PlanFilterPredicate::Equals { col: *col, value },
                Operator::Neq => PlanFilterPredicate::NotEquals { col: *col, value },
                Operator::Gt => PlanFilterPredicate::GreaterThan { col: *col, value },
                Operator::Gte => PlanFilterPredicate::GreaterThanOrEqual { col: *col, value },
                Operator::Lt => PlanFilterPredicate::LessThan { col: *col, value },
                Operator::Lte => PlanFilterPredicate::LessThanOrEqual { col: *col, value },
                _ => unreachable!("And/Or not valid for CompareMaterialized"),
            }
        }
        PlanFilterPredicate::And(l, r) => PlanFilterPredicate::And(
            Box::new(resolve_materialized_filter(l, materialized)),
            Box::new(resolve_materialized_filter(r, materialized)),
        ),
        PlanFilterPredicate::Or(l, r) => PlanFilterPredicate::Or(
            Box::new(resolve_materialized_filter(l, materialized)),
            Box::new(resolve_materialized_filter(r, materialized)),
        ),
        other => other.clone(),
    }
}
