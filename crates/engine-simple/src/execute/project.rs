use crate::planner::plan::{ColumnRef, PlanResultColumn};

use super::Columns;

/// Project result columns directly from a RowSet — only result columns are materialized.
pub fn project_rowset(
    rs: &super::RowSet,
    result_columns: &[PlanResultColumn],
) -> Columns {
    let mut result: Columns = Vec::with_capacity(result_columns.len());
    for rc in result_columns {
        match rc {
            PlanResultColumn::Column { col, .. } => {
                result.push(
                    (0..rs.num_rows).map(|row| rs.get(row, *col)).collect(),
                );
            }
            PlanResultColumn::Aggregate { .. } => {
                unreachable!("aggregate result column without aggregates in plan");
            }
        }
    }
    result
}

pub fn project(
    cols: &Columns,
    result_columns: &[PlanResultColumn],
    group_by: &[ColumnRef],
    has_aggregates: bool,
) -> Columns {
    let mut result: Columns = Vec::with_capacity(result_columns.len());
    let mut agg_counter = 0;

    for rc in result_columns {
        match rc {
            PlanResultColumn::Column { col, .. } => {
                if has_aggregates {
                    let pos = group_by
                        .iter()
                        .position(|&gb| gb == *col)
                        .expect("column in aggregate query must be in group_by");
                    result.push(cols[pos].clone());
                } else {
                    result.push(cols[col.col].clone());
                }
            }
            PlanResultColumn::Aggregate { .. } => {
                let pos = group_by.len() + agg_counter;
                result.push(cols[pos].clone());
                agg_counter += 1;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CellValue;
    use query_engine::ast::AggFunc;

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    #[test]
    fn test_project_simple() {
        let cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(2)],
            vec![CellValue::Str("a".into()), CellValue::Str("b".into())],
            vec![CellValue::I64(10), CellValue::I64(20)],
        ];

        let result = project(
            &cols,
            &[
                PlanResultColumn::Column { col: c(0, 2), alias: None },
                PlanResultColumn::Column { col: c(0, 0), alias: None },
            ],
            &[],
            false,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::I64(10), CellValue::I64(20)]);
        assert_eq!(result[1], vec![CellValue::I64(1), CellValue::I64(2)]);
    }

    #[test]
    fn test_project_after_aggregate() {
        let cols: Columns = vec![
            vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())],
            vec![CellValue::I64(25), CellValue::I64(30)],
        ];

        let result = project(
            &cols,
            &[
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Aggregate {
                    func: AggFunc::Min,
                    col: c(0, 2),
                    alias: Some("min_age".into()),
                },
            ],
            &[c(0, 1)],
            true,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())]);
        assert_eq!(result[1], vec![CellValue::I64(25), CellValue::I64(30)]);
    }
}
