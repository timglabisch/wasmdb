use crate::planner::plan::{ColumnRef, PlanFilterPredicate};
use crate::storage::Table;
use sql_parser::ast::JoinType;

use super::filter_row::eval_predicate;
use super::{ExecutionContext, RowSet, SpanOperation};

pub fn nested_loop_join<'a>(
    ctx: &mut ExecutionContext,
    left: &RowSet<'a>,
    right_table: &'a Table,
    right_row_ids: &[usize],
    right_source: usize,
    on: &PlanFilterPredicate,
    join_type: JoinType,
) -> RowSet<'a> {
    ctx.span_with(|_ctx| {
        let num_existing = left.tables.len();
        let mut new_row_ids: Vec<Vec<usize>> = (0..num_existing + 1).map(|_| Vec::new()).collect();

        for l in 0..left.num_rows {
            let mut matched = false;
            for &r in right_row_ids {
                if eval_predicate(on, &|col| {
                    if col.source < right_source {
                        left.get(l, col)
                    } else {
                        right_table.get(r, col.col)
                    }
                }) {
                    matched = true;
                    for ti in 0..num_existing { new_row_ids[ti].push(left.row_ids[ti][l]); }
                    new_row_ids[num_existing].push(r);
                }
            }
            if !matched && join_type == JoinType::Left {
                for ti in 0..num_existing { new_row_ids[ti].push(left.row_ids[ti][l]); }
                new_row_ids[num_existing].push(super::NULL_ROW);
            }
        }

        let num_rows = new_row_ids.first().map_or(0, |v| v.len());
        let mut tables = left.tables.clone();
        tables.push(right_table);
        let rs = RowSet { tables, row_ids: new_row_ids, num_rows };
        (SpanOperation::Join { rows_out: num_rows }, rs)
    })
}

pub fn index_nested_loop_join<'a>(
    ctx: &mut ExecutionContext,
    left: &RowSet<'a>,
    right_table: &'a Table,
    join_type: JoinType,
    left_col: ColumnRef,
    index_columns: &[usize],
    right_pre_filter: &PlanFilterPredicate,
) -> RowSet<'a> {
    ctx.span_with(|_ctx| {
        let idx = right_table.indexes().iter()
            .find(|idx| idx.columns() == index_columns)
            .expect("planned index must exist at runtime");

        let num_existing = left.tables.len();
        let mut new_row_ids: Vec<Vec<usize>> = (0..num_existing + 1).map(|_| Vec::new()).collect();

        for l in 0..left.num_rows {
            let lookup_value = left.get(l, left_col);
            let key = vec![lookup_value];
            let right_rows = idx.lookup_eq(&key)
                .map(|s| s.to_vec())
                .unwrap_or_default();

            let mut matched = false;
            for r in right_rows {
                if right_table.is_deleted(r) { continue; }
                // Apply right pre_filter if any
                if !matches!(right_pre_filter, PlanFilterPredicate::None) {
                    if !eval_predicate(
                        right_pre_filter,
                        &|col_ref| right_table.get(r, col_ref.col),
                    ) {
                        continue;
                    }
                }
                matched = true;
                for ti in 0..num_existing { new_row_ids[ti].push(left.row_ids[ti][l]); }
                new_row_ids[num_existing].push(r);
            }
            if !matched && join_type == JoinType::Left {
                for ti in 0..num_existing { new_row_ids[ti].push(left.row_ids[ti][l]); }
                new_row_ids[num_existing].push(super::NULL_ROW);
            }
        }

        let num_rows = new_row_ids.first().map_or(0, |v| v.len());
        let mut tables = left.tables.clone();
        tables.push(right_table);
        let rs = RowSet { tables, row_ids: new_row_ids, num_rows };
        (SpanOperation::Join { rows_out: num_rows }, rs)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::execute::scan::scan_row_ids;
    use crate::planner::plan::ColumnRef;
    use crate::storage::CellValue;
    use crate::schema::{ColumnSchema, DataType, TableSchema};

    fn make_users_table() -> crate::storage::Table {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0], indexes: vec![],
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
            primary_key: vec![0], indexes: vec![],
        };
        let mut t = crate::storage::Table::new(schema);
        t.insert(&[CellValue::I64(10), CellValue::I64(1), CellValue::I64(100)]).unwrap();
        t.insert(&[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]).unwrap();
        t.insert(&[CellValue::I64(12), CellValue::I64(2), CellValue::I64(50)]).unwrap();
        t
    }

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    #[test]
    fn test_inner_join() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let ut = make_users_table();
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&mut ctx, &ut));
        let right_ids = scan_row_ids(&mut ctx, &ot);
        let result = nested_loop_join(&mut ctx, &left, &ot, &right_ids, 1,
            &PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) }, JoinType::Inner);
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.get(0, c(0, 1)), CellValue::Str("Alice".into()));
        assert_eq!(result.get(1, c(0, 1)), CellValue::Str("Alice".into()));
        assert_eq!(result.get(2, c(0, 1)), CellValue::Str("Bob".into()));
    }

    #[test]
    fn test_left_join_with_nulls() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let ut = make_users_table();
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&mut ctx, &ut));
        let right_ids = scan_row_ids(&mut ctx, &ot);
        let result = nested_loop_join(&mut ctx, &left, &ot, &right_ids, 1,
            &PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) }, JoinType::Left);
        assert_eq!(result.num_rows, 4);
        assert_eq!(result.get(3, c(0, 1)), CellValue::Str("Carol".into()));
        assert_eq!(result.get(3, c(1, 0)), CellValue::Null);
    }

    #[test]
    fn test_join_condition_column_equals() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let ut = make_users_table();
        let ot = make_orders_table();
        let left = RowSet::from_scan(&ut, scan_row_ids(&mut ctx, &ut));
        let right_ids = scan_row_ids(&mut ctx, &ot);
        let result = nested_loop_join(&mut ctx, &left, &ot, &right_ids, 1,
            &PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) }, JoinType::Inner);
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.get(0, c(0, 0)), CellValue::I64(1));
        assert_eq!(result.get(0, c(1, 1)), CellValue::I64(1));
    }
}
