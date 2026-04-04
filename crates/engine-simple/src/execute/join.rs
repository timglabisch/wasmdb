use crate::planner::plan::PlanFilterPredicate;
use crate::storage::Table;
use query_engine::ast::JoinType;

use super::eval;
use super::RowSet;

/// Join a RowSet with a new table, producing an extended RowSet.
/// No data is copied — only row_id pairs are recorded.
pub fn nested_loop_join<'a>(
    left: &RowSet<'a>,
    right_table: &'a Table,
    right_row_ids: &[usize],
    on: &PlanFilterPredicate,
    join_type: JoinType,
) -> RowSet<'a> {
    let right_col_offset = left.num_cols;
    let right_num_cols = right_table.columns.len();
    let num_existing = left.tables.len();

    let mut new_row_ids: Vec<Vec<usize>> =
        (0..num_existing + 1).map(|_| Vec::new()).collect();

    for l in 0..left.num_rows {
        let mut matched = false;
        for &r in right_row_ids {
            if eval::eval_join_row(on, left, right_table, right_col_offset, l, r) {
                matched = true;
                for ti in 0..num_existing {
                    new_row_ids[ti].push(left.row_ids[ti][l]);
                }
                new_row_ids[num_existing].push(r);
            }
        }
        if !matched && join_type == JoinType::Left {
            for ti in 0..num_existing {
                new_row_ids[ti].push(left.row_ids[ti][l]);
            }
            new_row_ids[num_existing].push(super::NULL_ROW);
        }
    }

    let num_rows = new_row_ids.first().map_or(0, |v| v.len());
    let mut tables = left.tables.clone();
    tables.push(right_table);
    let mut col_offsets = left.col_offsets.clone();
    col_offsets.push(right_col_offset);

    RowSet {
        tables,
        col_offsets,
        num_cols: left.num_cols + right_num_cols,
        row_ids: new_row_ids,
        num_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::scan::scan_row_ids;
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
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&ut));
        let right_ids = scan_row_ids(&ot);

        let result = nested_loop_join(
            &left,
            &ot,
            &right_ids,
            &PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 },
            JoinType::Inner,
        );

        assert_eq!(result.num_rows, 3);
        assert_eq!(result.num_cols, 6);
        assert_eq!(result.get(0, 1), CellValue::Str("Alice".into()));
        assert_eq!(result.get(1, 1), CellValue::Str("Alice".into()));
        assert_eq!(result.get(2, 1), CellValue::Str("Bob".into()));
    }

    #[test]
    fn test_left_join_with_nulls() {
        let ut = make_users_table();
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&ut));
        let right_ids = scan_row_ids(&ot);

        let result = nested_loop_join(
            &left,
            &ot,
            &right_ids,
            &PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 },
            JoinType::Left,
        );

        assert_eq!(result.num_rows, 4);
        assert_eq!(result.get(3, 1), CellValue::Str("Carol".into()));
        // Right side column 3 (orders.id): matched rows have values, unmatched is Null
        assert_eq!(result.get(0, 3), CellValue::I64(10));
        assert_eq!(result.get(1, 3), CellValue::I64(11));
        assert_eq!(result.get(2, 3), CellValue::I64(12));
        assert_eq!(result.get(3, 3), CellValue::Null);
    }

    #[test]
    fn test_join_condition_column_equals() {
        let ut = make_users_table();
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&ut));
        let right_ids = scan_row_ids(&ot);

        let on = PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 };
        let result = nested_loop_join(&left, &ot, &right_ids, &on, JoinType::Inner);

        // users.id=1 matches orders.user_id=1 twice, users.id=2 matches once
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.get(0, 0), CellValue::I64(1)); // users.id
        assert_eq!(result.get(0, 4), CellValue::I64(1)); // orders.user_id
        assert_eq!(result.get(2, 0), CellValue::I64(2)); // users.id
        assert_eq!(result.get(2, 4), CellValue::I64(2)); // orders.user_id
    }
}
