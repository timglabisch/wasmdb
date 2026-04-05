//! Row-wise predicate evaluation (general path).
//!
//! Evaluates a predicate one row at a time via a cell-accessor closure.
//! Works for any predicate — including cross-table (joins, post-filter).
//!
//! This is the flexible path. For the optimized single-table batch path
//! that operates directly on typed column arrays, see [`super::filter_batch`].

use crate::planner::plan::{ColumnRef, PlanFilterPredicate};
use crate::storage::{CellValue, Table};

use super::value_to_cell;

#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// SQL comparison semantics: any comparison involving NULL returns false.
pub fn cmp_cell(left: &CellValue, right: &CellValue, op: CmpOp) -> bool {
    if matches!(left, CellValue::Null) || matches!(right, CellValue::Null) {
        return false;
    }
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Neq => left != right,
        CmpOp::Lt => left < right,
        CmpOp::Lte => left <= right,
        CmpOp::Gt => left > right,
        CmpOp::Gte => left >= right,
    }
}

/// Evaluate a predicate for a single row.
///
/// `get` resolves a [`ColumnRef`] to the cell value for the current row.
/// Returns `true` if the row matches.
pub fn filter_row<F: Fn(ColumnRef) -> CellValue>(pred: &PlanFilterPredicate, get: &F) -> bool {
    match pred {
        PlanFilterPredicate::None => true,
        PlanFilterPredicate::Equals { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Eq),
        PlanFilterPredicate::NotEquals { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Neq),
        PlanFilterPredicate::GreaterThan { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Gt),
        PlanFilterPredicate::GreaterThanOrEqual { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Gte),
        PlanFilterPredicate::LessThan { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Lt),
        PlanFilterPredicate::LessThanOrEqual { col, value } => cmp_cell(&get(*col), &value_to_cell(value), CmpOp::Lte),
        PlanFilterPredicate::ColumnEquals { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Eq),
        PlanFilterPredicate::ColumnNotEquals { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Neq),
        PlanFilterPredicate::ColumnGreaterThan { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Gt),
        PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Gte),
        PlanFilterPredicate::ColumnLessThan { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Lt),
        PlanFilterPredicate::ColumnLessThanOrEqual { left, right } => cmp_cell(&get(*left), &get(*right), CmpOp::Lte),
        PlanFilterPredicate::IsNull { col } => matches!(get(*col), CellValue::Null),
        PlanFilterPredicate::IsNotNull { col } => !matches!(get(*col), CellValue::Null),
        PlanFilterPredicate::In { col, values } => {
            let cell = get(*col);
            if matches!(cell, CellValue::Null) { return false; }
            values.iter().any(|v| cmp_cell(&cell, &value_to_cell(v), CmpOp::Eq))
        }
        PlanFilterPredicate::InMaterialized { .. }
        | PlanFilterPredicate::CompareMaterialized { .. } => {
            unreachable!("must be resolved before execution")
        }
        PlanFilterPredicate::And(a, b) => filter_row(a, get) && filter_row(b, get),
        PlanFilterPredicate::Or(a, b) => filter_row(a, get) || filter_row(b, get),
    }
}

/// Evaluate predicate on a single RowSet row.
pub fn filter_rowset_row(pred: &PlanFilterPredicate, rs: &super::RowSet, row: usize) -> bool {
    filter_row(pred, &|col| rs.get(row, col))
}

/// Evaluate join predicate: left columns from RowSet, right columns from Table.
pub fn filter_join_row(
    pred: &PlanFilterPredicate,
    left: &super::RowSet,
    right_table: &Table,
    right_source: usize,
    l: usize,
    r: usize,
) -> bool {
    filter_row(pred, &|col| {
        if col.source < right_source {
            left.get(l, col)
        } else {
            right_table.get(r, col.col)
        }
    })
}
