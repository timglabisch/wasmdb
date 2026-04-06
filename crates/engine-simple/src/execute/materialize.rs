//! Subquery materialization: resolves InMaterialized/CompareMaterialized
//! placeholders by executing subquery plans bottom-up before the main query.

use std::collections::HashMap;

use crate::planner::optimize::access_path;
use crate::planner::plan::*;
use crate::storage::Table;
use query_engine::ast::{Operator, Value};
use schema_engine::schema::TableSchema;

use super::pipeline::execute;
use super::{cell_to_value, Columns, ExecuteError, ExecutionContext, SpanOperation};

/// Execute an ExecutionPlan: run materializations first, resolve placeholders, then run main query.
pub fn execute_plan(
    ctx: &mut ExecutionContext,
    plan: &ExecutionPlan,
    db: &HashMap<String, Table>,
) -> Result<Columns, ExecuteError> {
    if plan.materializations.is_empty() {
        return execute(ctx, &plan.main, db);
    }

    // Extract table schemas from db for re-running access_path after materialization.
    let table_schemas: HashMap<String, TableSchema> = db.iter()
        .map(|(name, table)| (name.clone(), table.schema.clone()))
        .collect();

    let mut materialized: Vec<Vec<Value>> = Vec::new();

    for (i, step) in plan.materializations.iter().enumerate() {
        let values = ctx.span(SpanOperation::Materialize { step: i }, |ctx| {
            let resolved = resolve_materialized(&step.plan, &materialized, &table_schemas);
            let result = execute(ctx, &resolved, db)?;

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

    let resolved = resolve_materialized(&plan.main, &materialized, &table_schemas);
    execute(ctx, &resolved, db)
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
    // Re-run access_path to pick indexes for resolved predicates.
    access_path::rewrite(&mut resolved, table_schemas);
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
