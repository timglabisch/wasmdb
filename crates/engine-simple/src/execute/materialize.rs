//! Subquery materialization: resolves InMaterialized/CompareMaterialized
//! placeholders by executing subquery plans bottom-up before the main query.

use std::collections::HashMap;

use crate::planner::plan::*;
use crate::storage::Table;
use query_engine::ast::{Operator, Value};

use super::pipeline::execute;
use super::{cell_to_value, Columns, ExecuteError};

/// Execute an ExecutionPlan: run materializations first, resolve placeholders, then run main query.
pub fn execute_plan(
    plan: &ExecutionPlan,
    db: &HashMap<String, Table>,
) -> Result<Columns, ExecuteError> {
    if plan.materializations.is_empty() {
        return execute(&plan.main, db);
    }

    let mut materialized: Vec<Vec<Value>> = Vec::new();

    for step in &plan.materializations {
        let resolved = resolve_materialized(&step.plan, &materialized);
        let result = execute(&resolved, db)?;

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

        let values = result[0].iter().map(cell_to_value).collect();
        materialized.push(values);
    }

    let resolved = resolve_materialized(&plan.main, &materialized);
    execute(&resolved, db)
}

fn resolve_materialized(plan: &PlanSelect, materialized: &[Vec<Value>]) -> PlanSelect {
    let mut resolved = plan.clone();
    resolved.filter = resolve_materialized_filter(&resolved.filter, materialized);
    for source in &mut resolved.sources {
        source.pre_filter = resolve_materialized_filter(&source.pre_filter, materialized);
        if let Some(ref mut join) = source.join {
            join.on = resolve_materialized_filter(&join.on, materialized);
        }
    }
    resolved
}

fn resolve_materialized_filter(
    pred: &PlanFilterPredicate,
    materialized: &[Vec<Value>],
) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::InMaterialized { col, mat_id } => {
            PlanFilterPredicate::In {
                col: *col,
                values: materialized[*mat_id].clone(),
            }
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
