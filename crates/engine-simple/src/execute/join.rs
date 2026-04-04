use crate::planner::plan::PlanFilterPredicate;
use crate::storage::CellValue;
use query_engine::ast::JoinType;

use super::eval::eval_predicate;
use super::{num_rows, value_to_cell, Columns};

pub fn nested_loop_join(
    left: &Columns,
    right: &Columns,
    on: &PlanFilterPredicate,
    join_type: JoinType,
) -> Columns {
    let left_rows = num_rows(left);
    let right_rows = num_rows(right);
    let total_cols = left.len() + right.len();
    let mut result: Columns = (0..total_cols).map(|_| Vec::new()).collect();

    for l in 0..left_rows {
        let mut matched = false;

        for r in 0..right_rows {
            if eval_join_condition(left, right, l, r, on) {
                matched = true;
                for (ci, col) in left.iter().enumerate() {
                    result[ci].push(col[l].clone());
                }
                for (ci, col) in right.iter().enumerate() {
                    result[left.len() + ci].push(col[r].clone());
                }
            }
        }

        if !matched && join_type == JoinType::Left {
            for (ci, col) in left.iter().enumerate() {
                result[ci].push(col[l].clone());
            }
            for ci in 0..right.len() {
                result[left.len() + ci].push(CellValue::Null);
            }
        }
    }

    result
}

/// Evaluate a join condition for a single (left_row, right_row) pair
/// without materializing temporary columns.
fn eval_join_condition(
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
    use crate::execute::scan::{materialize, scan_indices};
    use crate::storage::CellValue;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    fn make_users_table() -> crate::storage::Table {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = crate::storage::Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
        t
    }

    fn make_orders_table() -> crate::storage::Table {
        let schema = TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = crate::storage::Table::new(schema);
        t.insert(&[CellValue::I64(10), CellValue::I64(1), CellValue::I64(100)]).unwrap();
        t.insert(&[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]).unwrap();
        t.insert(&[CellValue::I64(12), CellValue::I64(2), CellValue::I64(50)]).unwrap();
        t
    }

    #[test]
    fn test_inner_join() {
        let ut = make_users_table();
        let left = materialize(&ut, &scan_indices(&ut));
        let ot = make_orders_table();
        let right = materialize(&ot, &scan_indices(&ot));

        let result = nested_loop_join(
            &left,
            &right,
            &PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 },
            JoinType::Inner,
        );

        assert_eq!(result[0].len(), 3);
        assert_eq!(result.len(), 6);
        assert_eq!(result[1], vec![
            CellValue::Str("Alice".into()),
            CellValue::Str("Alice".into()),
            CellValue::Str("Bob".into()),
        ]);
    }

    #[test]
    fn test_left_join_with_nulls() {
        let ut = make_users_table();
        let left = materialize(&ut, &scan_indices(&ut));
        let ot = make_orders_table();
        let right = materialize(&ot, &scan_indices(&ot));

        let result = nested_loop_join(
            &left,
            &right,
            &PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 },
            JoinType::Left,
        );

        assert_eq!(result[0].len(), 4);
        assert_eq!(result[1][3], CellValue::Str("Carol".into()));
        assert_eq!(result[3], vec![
            CellValue::I64(10),
            CellValue::I64(11),
            CellValue::I64(12),
            CellValue::Null,
        ]);
    }

    #[test]
    fn test_join_condition_column_equals() {
        let left: Columns = vec![vec![CellValue::I64(1), CellValue::I64(2)]];
        let right: Columns = vec![vec![CellValue::I64(2), CellValue::I64(3)]];
        let on = PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 1 };
        assert!(!eval_join_condition(&left, &right, 0, 0, &on));
        assert!(eval_join_condition(&left, &right, 1, 0, &on));
    }
}
