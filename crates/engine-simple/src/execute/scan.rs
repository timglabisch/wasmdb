use crate::planner::plan::PlanFilterPredicate;
use crate::storage::Table;

use super::RowSet;

/// Scan + pre-filter → RowSet (no materialization).
pub fn scan<'a>(table: &'a Table, pre_filter: &PlanFilterPredicate) -> RowSet<'a> {
    let row_ids = if matches!(pre_filter, PlanFilterPredicate::None) {
        scan_row_ids(table)
    } else {
        scan_filtered(table, pre_filter)
    };
    RowSet::from_scan(table, row_ids)
}

/// Return live (non-deleted) row IDs without materializing any data.
pub fn scan_row_ids(table: &Table) -> Vec<usize> {
    table.row_ids().collect()
}

/// Scan + filter: collect live row IDs, then evaluate predicate.
pub fn scan_filtered(table: &Table, pred: &PlanFilterPredicate) -> Vec<usize> {
    let row_ids = scan_row_ids(table);
    pred.eval_table(table, &row_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CellValue;
    use query_engine::ast::Value;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

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
            &PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(2) },
        );
        assert_eq!(row_ids, vec![1]);
    }

    #[test]
    fn test_scan_filtered_greater_than() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::GreaterThan { column_idx: 2, value: Value::Int(28) },
        );
        assert_eq!(row_ids, vec![0, 2]); // Alice(30), Carol(35)
    }

    #[test]
    fn test_scan_filtered_skips_deleted() {
        let mut table = make_users_table();
        table.delete(0).unwrap();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::GreaterThan { column_idx: 2, value: Value::Int(28) },
        );
        assert_eq!(row_ids, vec![2]); // only Carol
    }

    #[test]
    fn test_scan_filtered_and() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::And(
                Box::new(PlanFilterPredicate::GreaterThan { column_idx: 2, value: Value::Int(24) }),
                Box::new(PlanFilterPredicate::LessThan { column_idx: 2, value: Value::Int(32) }),
            ),
        );
        assert_eq!(row_ids, vec![0, 1]); // Alice(30), Bob(25)
    }

    #[test]
    fn test_scan_returns_rowset() {
        let table = make_users_table();
        let rs = scan(&table, &PlanFilterPredicate::GreaterThan { column_idx: 2, value: Value::Int(28) });
        assert_eq!(rs.num_rows, 2);
        assert_eq!(rs.get(0, 1), CellValue::Str("Alice".into()));
        assert_eq!(rs.get(1, 1), CellValue::Str("Carol".into()));
    }
}
