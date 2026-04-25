//! Row-wise predicate evaluation (general path).
//!
//! Evaluates a predicate one row at a time via a cell-accessor closure.
//! Works for any predicate — including cross-table (joins, post-filter).
//!
//! This is the flexible path. For the optimized single-table batch path
//! that operates directly on typed column arrays, see [`super::filter_batch`].

use crate::planner::shared::plan::{ColumnRef, PlanFilterPredicate};
use crate::storage::CellValue;

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

/// Pure predicate evaluation — no ExecutionContext needed.
///
/// `get` resolves a [`ColumnRef`] to the cell value for the current row.
/// Returns `true` if the row matches.
pub fn eval_predicate<F: Fn(ColumnRef) -> CellValue>(pred: &PlanFilterPredicate, get: &F) -> bool {
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
        PlanFilterPredicate::And(a, b) => eval_predicate(a, get) && eval_predicate(b, get),
        PlanFilterPredicate::Or(a, b) => eval_predicate(a, get) || eval_predicate(b, get),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;

    fn uuid_n(n: u8) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[15] = n;
        b
    }

    #[test]
    fn cmp_cell_uuid_eq() {
        let a = CellValue::Uuid(uuid_n(1));
        assert!(cmp_cell(&a, &CellValue::Uuid(uuid_n(1)), CmpOp::Eq));
        assert!(!cmp_cell(&a, &CellValue::Uuid(uuid_n(2)), CmpOp::Eq));
    }

    #[test]
    fn cmp_cell_uuid_lt_lex_byte() {
        let a = CellValue::Uuid(uuid_n(1));
        let b = CellValue::Uuid(uuid_n(2));
        assert!(cmp_cell(&a, &b, CmpOp::Lt));
        assert!(!cmp_cell(&b, &a, CmpOp::Lt));
    }

    #[test]
    fn cmp_cell_uuid_against_null_is_false() {
        let a = CellValue::Uuid(uuid_n(1));
        assert!(!cmp_cell(&a, &CellValue::Null, CmpOp::Eq));
        assert!(!cmp_cell(&CellValue::Null, &a, CmpOp::Eq));
        assert!(!cmp_cell(&a, &CellValue::Null, CmpOp::Neq));
    }

    #[test]
    fn eval_predicate_uuid_in_list() {
        let row = vec![CellValue::Uuid(uuid_n(2))];
        let get = |cr: ColumnRef| row[cr.col].clone();
        let pred = PlanFilterPredicate::In {
            col: ColumnRef { source: 0, col: 0 },
            values: vec![Value::Uuid(uuid_n(1)), Value::Uuid(uuid_n(2))],
        };
        assert!(eval_predicate(&pred, &get));
    }

    #[test]
    fn eval_predicate_uuid_in_list_skips_null() {
        let row = vec![CellValue::Null];
        let get = |cr: ColumnRef| row[cr.col].clone();
        let pred = PlanFilterPredicate::In {
            col: ColumnRef { source: 0, col: 0 },
            values: vec![Value::Uuid(uuid_n(1))],
        };
        assert!(!eval_predicate(&pred, &get));
    }
}
