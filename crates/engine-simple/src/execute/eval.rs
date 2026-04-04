use crate::planner::plan::PlanFilterPredicate;
use crate::storage::CellValue;
use query_engine::ast::Value;

use super::{num_rows, value_to_cell, Columns};

#[derive(Debug, Clone, Copy)]
enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Evaluate a predicate against columnar data, producing a bitmask.
/// mask[row] == true means the row passes the filter.
pub fn eval_predicate(cols: &Columns, pred: &PlanFilterPredicate) -> Vec<bool> {
    let n = num_rows(cols);
    match pred {
        PlanFilterPredicate::None => vec![true; n],

        PlanFilterPredicate::Equals { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Eq)
        }
        PlanFilterPredicate::NotEquals { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Neq)
        }
        PlanFilterPredicate::GreaterThan { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Gt)
        }
        PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Gte)
        }
        PlanFilterPredicate::LessThan { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Lt)
        }
        PlanFilterPredicate::LessThanOrEqual { column_idx, value } => {
            eval_column_value_cmp(cols, *column_idx, value, CmpOp::Lte)
        }

        PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Eq)
        }
        PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Neq)
        }
        PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Gt)
        }
        PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Gte)
        }
        PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Lt)
        }
        PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => {
            eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Lte)
        }

        PlanFilterPredicate::IsNull { column_idx } => eval_is_null(cols, *column_idx, true),
        PlanFilterPredicate::IsNotNull { column_idx } => eval_is_null(cols, *column_idx, false),

        PlanFilterPredicate::And(l, r) => {
            let lm = eval_predicate(cols, l);
            let rm = eval_predicate(cols, r);
            lm.iter().zip(rm.iter()).map(|(a, b)| *a && *b).collect()
        }
        PlanFilterPredicate::Or(l, r) => {
            let lm = eval_predicate(cols, l);
            let rm = eval_predicate(cols, r);
            lm.iter().zip(rm.iter()).map(|(a, b)| *a || *b).collect()
        }
    }
}

/// SIMD-ready: inner loop over a single homogeneous column.
fn eval_column_value_cmp(
    cols: &Columns,
    col_idx: usize,
    value: &Value,
    op: CmpOp,
) -> Vec<bool> {
    let col = &cols[col_idx];
    let cell_val = value_to_cell(value);
    col.iter().map(|cell| cmp_cell(cell, &cell_val, op)).collect()
}

fn eval_column_column_cmp(
    cols: &Columns,
    left_idx: usize,
    right_idx: usize,
    op: CmpOp,
) -> Vec<bool> {
    let left = &cols[left_idx];
    let right = &cols[right_idx];
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| cmp_cell(l, r, op))
        .collect()
}

fn eval_is_null(cols: &Columns, col_idx: usize, want_null: bool) -> Vec<bool> {
    cols[col_idx]
        .iter()
        .map(|v| matches!(v, CellValue::Null) == want_null)
        .collect()
}

/// SQL comparison semantics: any comparison involving NULL returns false.
fn cmp_cell(left: &CellValue, right: &CellValue, op: CmpOp) -> bool {
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

/// Evaluate a join condition for a single (left_row, right_row) pair
/// without materializing temporary columns.
pub fn eval_join_condition(
    left: &Columns,
    right: &Columns,
    l: usize,
    r: usize,
    on: &PlanFilterPredicate,
) -> bool {
    match on {
        PlanFilterPredicate::None => true,
        PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
            get_combined(left, right, l, r, *left_idx)
                == get_combined(left, right, l, r, *right_idx)
        }
        PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
            get_combined(left, right, l, r, *left_idx)
                != get_combined(left, right, l, r, *right_idx)
        }
        PlanFilterPredicate::Equals { column_idx, value } => {
            let cell = get_combined(left, right, l, r, *column_idx);
            cell == value_to_cell(value)
        }
        PlanFilterPredicate::And(a, b) => {
            eval_join_condition(left, right, l, r, a)
                && eval_join_condition(left, right, l, r, b)
        }
        PlanFilterPredicate::Or(a, b) => {
            eval_join_condition(left, right, l, r, a)
                || eval_join_condition(left, right, l, r, b)
        }
        // General fallback: build single-row Columns
        other => {
            let combined = build_single_row(left, right, l, r);
            let mask = eval_predicate(&combined, other);
            mask[0]
        }
    }
}

fn get_combined(
    left: &Columns,
    right: &Columns,
    l: usize,
    r: usize,
    global_idx: usize,
) -> CellValue {
    if global_idx < left.len() {
        left[global_idx][l].clone()
    } else {
        right[global_idx - left.len()][r].clone()
    }
}

fn build_single_row(left: &Columns, right: &Columns, l: usize, r: usize) -> Columns {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    for col in left {
        combined.push(vec![col[l].clone()]);
    }
    for col in right {
        combined.push(vec![col[r].clone()]);
    }
    combined
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cols(data: Vec<Vec<CellValue>>) -> Columns {
        data
    }

    #[test]
    fn test_eval_equals_i64() {
        let cols = make_cols(vec![vec![
            CellValue::I64(1),
            CellValue::I64(2),
            CellValue::I64(3),
            CellValue::I64(2),
            CellValue::I64(5),
        ]]);
        let pred = PlanFilterPredicate::Equals {
            column_idx: 0,
            value: Value::Int(2),
        };
        assert_eq!(
            eval_predicate(&cols, &pred),
            vec![false, true, false, true, false]
        );
    }

    #[test]
    fn test_eval_greater_than() {
        let cols = make_cols(vec![vec![
            CellValue::I64(1),
            CellValue::I64(5),
            CellValue::I64(3),
        ]]);
        let pred = PlanFilterPredicate::GreaterThan {
            column_idx: 0,
            value: Value::Int(2),
        };
        assert_eq!(eval_predicate(&cols, &pred), vec![false, true, true]);
    }

    #[test]
    fn test_eval_is_null() {
        let cols = make_cols(vec![vec![
            CellValue::I64(1),
            CellValue::Null,
            CellValue::I64(3),
            CellValue::Null,
        ]]);
        let pred = PlanFilterPredicate::IsNull { column_idx: 0 };
        assert_eq!(
            eval_predicate(&cols, &pred),
            vec![false, true, false, true]
        );

        let pred = PlanFilterPredicate::IsNotNull { column_idx: 0 };
        assert_eq!(
            eval_predicate(&cols, &pred),
            vec![true, false, true, false]
        );
    }

    #[test]
    fn test_eval_and() {
        let cols = make_cols(vec![
            vec![CellValue::I64(1), CellValue::I64(5), CellValue::I64(3)],
            vec![
                CellValue::Str("a".into()),
                CellValue::Str("b".into()),
                CellValue::Str("a".into()),
            ],
        ]);
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan {
                column_idx: 0,
                value: Value::Int(2),
            }),
            Box::new(PlanFilterPredicate::Equals {
                column_idx: 1,
                value: Value::Text("a".into()),
            }),
        );
        // row0: 1>2=false AND "a"="a"=true → false
        // row1: 5>2=true AND "b"="a"=false → false
        // row2: 3>2=true AND "a"="a"=true → true
        assert_eq!(eval_predicate(&cols, &pred), vec![false, false, true]);
    }

    #[test]
    fn test_eval_or() {
        let cols = make_cols(vec![vec![
            CellValue::I64(1),
            CellValue::I64(5),
            CellValue::I64(3),
        ]]);
        let pred = PlanFilterPredicate::Or(
            Box::new(PlanFilterPredicate::Equals {
                column_idx: 0,
                value: Value::Int(1),
            }),
            Box::new(PlanFilterPredicate::Equals {
                column_idx: 0,
                value: Value::Int(5),
            }),
        );
        assert_eq!(eval_predicate(&cols, &pred), vec![true, true, false]);
    }

    #[test]
    fn test_eval_column_equals() {
        let cols = make_cols(vec![
            vec![CellValue::I64(1), CellValue::I64(2), CellValue::I64(3)],
            vec![CellValue::I64(1), CellValue::I64(99), CellValue::I64(3)],
        ]);
        let pred = PlanFilterPredicate::ColumnEquals {
            left_idx: 0,
            right_idx: 1,
        };
        assert_eq!(eval_predicate(&cols, &pred), vec![true, false, true]);
    }

    #[test]
    fn test_null_comparison_returns_false() {
        let cols = make_cols(vec![vec![
            CellValue::I64(10),
            CellValue::Null,
            CellValue::I64(30),
        ]]);
        // NULL > 18 should be false (SQL semantics)
        let pred = PlanFilterPredicate::GreaterThan {
            column_idx: 0,
            value: Value::Int(18),
        };
        assert_eq!(eval_predicate(&cols, &pred), vec![false, false, true]);

        // NULL = NULL should be false
        let pred = PlanFilterPredicate::Equals {
            column_idx: 0,
            value: Value::Null,
        };
        assert_eq!(eval_predicate(&cols, &pred), vec![false, false, false]);
    }

    #[test]
    fn test_eval_none_accepts_all() {
        let cols = make_cols(vec![vec![
            CellValue::I64(1),
            CellValue::I64(2),
        ]]);
        assert_eq!(
            eval_predicate(&cols, &PlanFilterPredicate::None),
            vec![true, true]
        );
    }

    #[test]
    fn test_join_condition_column_equals() {
        let left = make_cols(vec![
            vec![CellValue::I64(1), CellValue::I64(2)],
        ]);
        let right = make_cols(vec![
            vec![CellValue::I64(2), CellValue::I64(3)],
        ]);
        let on = PlanFilterPredicate::ColumnEquals {
            left_idx: 0,
            right_idx: 1,
        };
        // left row 0 (val=1) vs right row 0 (val=2): 1 != 2 → false
        assert!(!eval_join_condition(&left, &right, 0, 0, &on));
        // left row 1 (val=2) vs right row 0 (val=2): 2 == 2 → true
        assert!(eval_join_condition(&left, &right, 1, 0, &on));
    }
}
