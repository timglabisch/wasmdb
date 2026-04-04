use crate::planner::plan::PlanFilterPredicate;
use crate::storage::Table;

use super::Columns;

/// Return live (non-deleted) row IDs without materializing any data.
pub fn scan_row_ids(table: &Table) -> Vec<usize> {
    table.row_ids().collect()
}

/// Scan + filter fused: evaluate predicate on the full contiguous table
/// storage (SIMD-friendly path), returning qualifying row IDs.
pub fn scan_filtered(table: &Table, pred: &PlanFilterPredicate) -> Vec<usize> {
    pred.eval_full_scan(table)
}

/// Materialize all columns for the given row IDs (column-at-a-time batch conversion).
pub fn materialize(table: &Table, row_ids: &[usize]) -> Columns {
    table.columns.iter().map(|col| col.to_cells(row_ids)).collect()
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
    fn test_materialize_all() {
        let mut table = make_users_table();
        table.delete(1).unwrap();
        let row_ids = scan_row_ids(&table);
        let cols = materialize(&table, &row_ids);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(cols[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    #[test]
    fn test_scan_filtered_then_materialize() {
        let table = make_users_table();
        let row_ids = scan_filtered(
            &table,
            &PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(2) },
        );
        let cols = materialize(&table, &row_ids);
        assert_eq!(cols[0], vec![CellValue::I64(2)]);
        assert_eq!(cols[1], vec![CellValue::Str("Bob".into())]);
        assert_eq!(cols[2], vec![CellValue::I64(25)]);
    }
}
