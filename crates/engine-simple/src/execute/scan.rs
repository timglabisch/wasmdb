use crate::planner::plan::PlanFilterPredicate;
use crate::storage::{RangeOp, Table};
use super::value_to_cell;

use super::RowSet;

/// Scan + pre-filter → RowSet (no materialization).
/// Tries index lookup first; falls back to full scan + filter.
pub fn scan<'a>(table: &'a Table, pre_filter: &PlanFilterPredicate) -> RowSet<'a> {
    let row_ids = if matches!(pre_filter, PlanFilterPredicate::None) {
        scan_row_ids(table)
    } else {
        match try_index_scan(table, pre_filter) {
            Some(ids) => ids,
            None => scan_filtered(table, pre_filter),
        }
    };
    RowSet::from_scan(table, row_ids)
}

pub fn scan_row_ids(table: &Table) -> Vec<usize> {
    table.row_ids().collect()
}

pub fn scan_filtered(table: &Table, pred: &PlanFilterPredicate) -> Vec<usize> {
    let row_ids = scan_row_ids(table);
    pred.eval_table(table, &row_ids)
}

/// Try to satisfy the predicate via an index lookup.
/// Returns `Some(row_ids)` if an index was used, `None` otherwise.
fn try_index_scan(table: &Table, pred: &PlanFilterPredicate) -> Option<Vec<usize>> {
    match pred {
        PlanFilterPredicate::Equals { col, value } => {
            let idx = table.index_for_column(col.col)?;
            let cell = value_to_cell(value);
            let ids = idx.lookup_eq(&cell)?;
            // Filter out deleted rows.
            Some(ids.iter().copied().filter(|&r| !table.is_deleted(r)).collect())
        }
        PlanFilterPredicate::GreaterThan { col, value } => {
            let idx = table.index_for_column(col.col)?;
            let cell = value_to_cell(value);
            let ids = idx.lookup_range(RangeOp::Gt, &cell)?;
            Some(ids.into_iter().filter(|&r| !table.is_deleted(r)).collect())
        }
        PlanFilterPredicate::GreaterThanOrEqual { col, value } => {
            let idx = table.index_for_column(col.col)?;
            let cell = value_to_cell(value);
            let ids = idx.lookup_range(RangeOp::Gte, &cell)?;
            Some(ids.into_iter().filter(|&r| !table.is_deleted(r)).collect())
        }
        PlanFilterPredicate::LessThan { col, value } => {
            let idx = table.index_for_column(col.col)?;
            let cell = value_to_cell(value);
            let ids = idx.lookup_range(RangeOp::Lt, &cell)?;
            Some(ids.into_iter().filter(|&r| !table.is_deleted(r)).collect())
        }
        PlanFilterPredicate::LessThanOrEqual { col, value } => {
            let idx = table.index_for_column(col.col)?;
            let cell = value_to_cell(value);
            let ids = idx.lookup_range(RangeOp::Lte, &cell)?;
            Some(ids.into_iter().filter(|&r| !table.is_deleted(r)).collect())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::ColumnRef;
    use crate::storage::CellValue;
    use query_engine::ast::Value;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    fn make_users_table() -> Table {
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
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
        t
    }

    #[test]
    fn test_scan_row_ids_skips_deleted() {
        let mut table = make_users_table();
        table.delete(1).unwrap();
        let row_ids = scan_row_ids(&table);
        assert_eq!(row_ids, vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_equals() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) },
        );
        assert_eq!(row_ids, vec![1]);
    }

    #[test]
    fn test_scan_filtered_greater_than() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) },
        );
        assert_eq!(row_ids, vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_skips_deleted() {
        let mut table = make_users_table();
        table.delete(0).unwrap();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) },
        );
        assert_eq!(row_ids, vec![2]);
    }

    #[test]
    fn test_scan_filtered_and() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::And(
                Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(24) }),
                Box::new(PlanFilterPredicate::LessThan { col: c(0, 2), value: Value::Int(32) }),
            ),
        );
        assert_eq!(row_ids, vec![0, 1]);
    }

    #[test]
    fn test_scan_returns_rowset() {
        let table = make_users_table();
        let rs = scan(&table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) });
        assert_eq!(rs.num_rows, 2);
        assert_eq!(rs.get(0, c(0, 1)), CellValue::Str("Alice".into()));
        assert_eq!(rs.get(1, c(0, 1)), CellValue::Str("Carol".into()));
    }
}
