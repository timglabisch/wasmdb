use crate::planner::plan::{ColumnRef, PlanResultColumn};
use crate::storage::CellValue;

use super::{Columns, ExecutionContext, SpanOperation};

pub fn project_rowset(
    ctx: &mut ExecutionContext,
    rs: &super::RowSet,
    result_columns: &[PlanResultColumn],
) -> Columns {
    ctx.span_with(|_ctx| {
        let mut result: Columns = Vec::with_capacity(result_columns.len());
        for rc in result_columns {
            match rc {
                PlanResultColumn::Column { col, .. } => {
                    result.push((0..rs.num_rows).map(|row| rs.get(row, *col)).collect());
                }
                PlanResultColumn::Aggregate { .. } => {
                    unreachable!("aggregate result column without aggregates in plan");
                }
                PlanResultColumn::InvalidateOn { .. } => {
                    result.push(vec![CellValue::I64(0); rs.num_rows]);
                }
            }
        }
        let rows = result.first().map_or(0, |c| c.len());
        (SpanOperation::Project { columns: result.len(), rows }, result)
    })
}

/// Project materialized columns after aggregation.
/// Layout of `cols`: `[group_by_0, group_by_1, ..., agg_0, agg_1, ...]`.
pub fn project(
    ctx: &mut ExecutionContext,
    cols: &Columns,
    result_columns: &[PlanResultColumn],
    group_by: &[ColumnRef],
) -> Columns {
    ctx.span_with(|_ctx| {
        let mut result: Columns = Vec::with_capacity(result_columns.len());
        let mut agg_counter = 0;
        for rc in result_columns {
            match rc {
                PlanResultColumn::Column { col, .. } => {
                    let pos = group_by.iter().position(|&gb| gb == *col)
                        .expect("column in aggregate query must be in group_by");
                    result.push(cols[pos].clone());
                }
                PlanResultColumn::Aggregate { .. } => {
                    let pos = group_by.len() + agg_counter;
                    result.push(cols[pos].clone());
                    agg_counter += 1;
                }
                PlanResultColumn::InvalidateOn { .. } => {
                    let num_rows = cols.first().map_or(0, |c| c.len());
                    result.push(vec![CellValue::I64(0); num_rows]);
                }
            }
        }
        let rows = result.first().map_or(0, |c| c.len());
        (SpanOperation::Project { columns: result.len(), rows }, result)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::storage::CellValue;
    use sql_parser::ast::AggFunc;

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    #[test]
    fn test_project_group_by_only() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        // After aggregate_rowset with group_by=[col 1], no aggregates:
        // cols layout: [group_by_0_values]
        let cols: Columns = vec![
            vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())],
        ];
        let result = project(&mut ctx, &cols, &[
            PlanResultColumn::Column { col: c(0, 1), alias: None },
        ], &[c(0, 1)]);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())]);
    }

    #[test]
    fn test_project_after_aggregate() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        // After aggregate_rowset with group_by=[col 1], aggregates=[MIN(col 2)]:
        // cols layout: [group_by_0_values, agg_0_values]
        let cols: Columns = vec![
            vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())],
            vec![CellValue::I64(25), CellValue::I64(30)],
        ];
        let result = project(&mut ctx, &cols, &[
            PlanResultColumn::Column { col: c(0, 1), alias: None },
            PlanResultColumn::Aggregate { func: AggFunc::Min, col: c(0, 2), alias: Some("min_age".into()) },
        ], &[c(0, 1)]);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())]);
        assert_eq!(result[1], vec![CellValue::I64(25), CellValue::I64(30)]);
    }
}
