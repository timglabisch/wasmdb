use crate::planner::plan::{ColumnRef, PlanFilterPredicate, PlanOrderSpec};
use crate::storage::{CellValue, Table};
use sql_parser::ast::OrderDirection;

use super::{ExecutionContext, SpanOperation};

/// Sentinel row ID for null-fill in left joins (no match on right side).
pub const NULL_ROW: usize = usize::MAX;

/// Virtual row set backed by references to underlying Tables.
/// No data is copied — cell access goes through row_id indirection.
pub struct RowSet<'a> {
    pub tables: Vec<&'a Table>,
    /// `row_ids[source][output_row]` = physical row in that table.
    /// [`NULL_ROW`] means null fill (left join, no match).
    pub row_ids: Vec<Vec<usize>>,
    pub num_rows: usize,
}

impl<'a> RowSet<'a> {
    pub fn from_scan(table: &'a Table, row_ids: Vec<usize>) -> Self {
        let num_rows = row_ids.len();
        RowSet { tables: vec![table], row_ids: vec![row_ids], num_rows }
    }

    pub fn get(&self, row: usize, col: ColumnRef) -> CellValue {
        let row_id = self.row_ids[col.source][row];
        if row_id == NULL_ROW { CellValue::Null } else { self.tables[col.source].get(row_id, col.col) }
    }

    pub fn sort(&mut self, ctx: &mut ExecutionContext, order_by: &[PlanOrderSpec]) {
        if order_by.is_empty() || self.num_rows <= 1 { return; }
        let n = self.num_rows;
        ctx.span(SpanOperation::Sort { rows: n }, |_ctx| {
            let mut row_order: Vec<usize> = (0..self.num_rows).collect();
            row_order.sort_by(|&a, &b| {
                for spec in order_by {
                    let va = self.get(a, spec.col);
                    let vb = self.get(b, spec.col);
                    let cmp = va.cmp(&vb);
                    let cmp = match spec.direction {
                        OrderDirection::Asc => cmp,
                        OrderDirection::Desc => cmp.reverse(),
                    };
                    if cmp != core::cmp::Ordering::Equal { return cmp; }
                }
                core::cmp::Ordering::Equal
            });
            for ids in &mut self.row_ids {
                let sorted: Vec<usize> = row_order.iter().map(|&i| ids[i]).collect();
                *ids = sorted;
            }
        });
    }

    pub fn filter(&self, ctx: &mut ExecutionContext, pred: &PlanFilterPredicate) -> RowSet<'a> {
        let rows_in = self.num_rows;
        ctx.span_with(|ctx| {
            let mut new_row_ids: Vec<Vec<usize>> =
                (0..self.tables.len()).map(|_| Vec::new()).collect();
            let mut count = 0;
            for row in 0..self.num_rows {
                if super::filter_row::filter_rowset_row(ctx, pred, self, row) {
                    for (ti, ids) in self.row_ids.iter().enumerate() {
                        new_row_ids[ti].push(ids[row]);
                    }
                    count += 1;
                }
            }
            let rs = RowSet { tables: self.tables.clone(), row_ids: new_row_ids, num_rows: count };
            (SpanOperation::Filter { rows_in, rows_out: count }, rs)
        })
    }
}
