use crate::planner::plan::{PlanOrderSpec, PlanResultColumn};
use crate::storage::CellValue;
use query_engine::ast::OrderDirection;

use super::Columns;

pub fn sort_materialized(
    cols: &mut Columns,
    order_by: &[PlanOrderSpec],
    result_columns: &[PlanResultColumn],
) {
    if cols.is_empty() || cols[0].is_empty() {
        return;
    }
    let num_rows = cols[0].len();
    let mut row_order: Vec<usize> = (0..num_rows).collect();

    // Map each order_by spec to a result column index.
    let order_indices: Vec<(usize, OrderDirection)> = order_by
        .iter()
        .filter_map(|spec| {
            let pos = result_columns.iter().position(|rc| match rc {
                PlanResultColumn::Column { col, .. } => *col == spec.col,
                PlanResultColumn::Aggregate { col, .. } => *col == spec.col,
            });
            pos.map(|p| (p, spec.direction))
        })
        .collect();

    row_order.sort_by(|&a, &b| {
        for &(col_idx, dir) in &order_indices {
            let cmp = cols[col_idx][a].cmp(&cols[col_idx][b]);
            let cmp = match dir {
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
