use crate::planner::plan::PlanOrderSpec;
use crate::storage::CellValue;
use query_engine::ast::OrderDirection;

use super::Columns;

pub fn sort_columns(cols: &mut Columns, order_by: &[PlanOrderSpec]) {
    if cols.is_empty() || cols[0].is_empty() {
        return;
    }
    let num_rows = cols[0].len();
    let mut row_order: Vec<usize> = (0..num_rows).collect();

    row_order.sort_by(|&a, &b| {
        for spec in order_by {
            let col_idx = spec.col.col;
            let cmp = cols[col_idx][a].cmp(&cols[col_idx][b]);
            let cmp = match spec.direction {
                OrderDirection::Asc => cmp,
                OrderDirection::Desc => cmp.reverse(),
            };
            if cmp != core::cmp::Ordering::Equal {
                return cmp;
            }
        }
        core::cmp::Ordering::Equal
    });

    for col in cols.iter_mut() {
        let sorted: Vec<CellValue> = row_order.iter().map(|&i| col[i].clone()).collect();
        *col = sorted;
    }
}

pub fn sort_rowset_columns(cols: &mut Columns, order_by: &[PlanOrderSpec], col_mapping: &[(usize, usize)]) {
    if cols.is_empty() || cols[0].is_empty() || order_by.is_empty() {
        return;
    }
    let num_rows = cols[0].len();
    let mut row_order: Vec<usize> = (0..num_rows).collect();

    row_order.sort_by(|&a, &b| {
        for spec in order_by {
            let result_col = col_mapping
                .iter()
                .position(|&(src, col)| src == spec.col.source && col == spec.col.col);
            if let Some(ci) = result_col {
                let cmp = cols[ci][a].cmp(&cols[ci][b]);
                let cmp = match spec.direction {
                    OrderDirection::Asc => cmp,
                    OrderDirection::Desc => cmp.reverse(),
                };
                if cmp != core::cmp::Ordering::Equal {
                    return cmp;
                }
            }
        }
        core::cmp::Ordering::Equal
    });

    for col in cols.iter_mut() {
        let sorted: Vec<CellValue> = row_order.iter().map(|&i| col[i].clone()).collect();
        *col = sorted;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::ColumnRef;
    use crate::storage::CellValue;

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    #[test]
    fn test_sort_asc() {
        let mut cols: Columns = vec![
            vec![CellValue::I64(3), CellValue::I64(1), CellValue::I64(2)],
            vec![
                CellValue::Str("c".into()),
                CellValue::Str("a".into()),
                CellValue::Str("b".into()),
            ],
        ];

        sort_columns(
            &mut cols,
            &[PlanOrderSpec {
                col: c(0, 0),
                direction: OrderDirection::Asc,
            }],
        );

        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(2), CellValue::I64(3)]);
        assert_eq!(
            cols[1],
            vec![
                CellValue::Str("a".into()),
                CellValue::Str("b".into()),
                CellValue::Str("c".into()),
            ]
        );
    }

    #[test]
    fn test_sort_desc() {
        let mut cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(3), CellValue::I64(2)],
        ];

        sort_columns(
            &mut cols,
            &[PlanOrderSpec {
                col: c(0, 0),
                direction: OrderDirection::Desc,
            }],
        );

        assert_eq!(cols[0], vec![CellValue::I64(3), CellValue::I64(2), CellValue::I64(1)]);
    }

    #[test]
    fn test_sort_multi_key() {
        let mut cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(1), CellValue::I64(2)],
            vec![CellValue::I64(20), CellValue::I64(10), CellValue::I64(5)],
        ];

        sort_columns(
            &mut cols,
            &[
                PlanOrderSpec {
                    col: c(0, 0),
                    direction: OrderDirection::Asc,
                },
                PlanOrderSpec {
                    col: c(0, 1),
                    direction: OrderDirection::Asc,
                },
            ],
        );

        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(1), CellValue::I64(2)]);
        assert_eq!(cols[1], vec![CellValue::I64(10), CellValue::I64(20), CellValue::I64(5)]);
    }

    #[test]
    fn test_sort_nulls_last_asc() {
        let mut cols: Columns = vec![
            vec![CellValue::I64(2), CellValue::Null, CellValue::I64(1)],
        ];

        sort_columns(
            &mut cols,
            &[PlanOrderSpec {
                col: c(0, 0),
                direction: OrderDirection::Asc,
            }],
        );

        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(2), CellValue::Null]);
    }

    #[test]
    fn test_sort_empty() {
        let mut cols: Columns = vec![vec![]];
        sort_columns(
            &mut cols,
            &[PlanOrderSpec {
                col: c(0, 0),
                direction: OrderDirection::Asc,
            }],
        );
        assert_eq!(cols[0].len(), 0);
    }
}
