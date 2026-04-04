use crate::storage::Table;

use super::Columns;

/// Return live (non-deleted) row indices without materializing any data.
pub fn scan_indices(table: &Table) -> Vec<usize> {
    table.row_indices().collect()
}

/// Materialize all columns for the given row indices (column-at-a-time batch conversion).
pub fn materialize(table: &Table, indices: &[usize]) -> Columns {
    table.columns.iter().map(|col| col.to_cells(indices)).collect()
}

/// Materialize only specific columns for the given row indices.
/// Returns a sparse Columns where only `col_indices` positions are populated;
/// other positions are empty Vecs.
pub fn materialize_sparse(
    table: &Table,
    row_indices: &[usize],
    col_indices: &[usize],
) -> Columns {
    let num_cols = table.columns.len();
    let mut cols: Columns = (0..num_cols).map(|_| Vec::new()).collect();
    for &ci in col_indices {
        cols[ci] = table.columns[ci].to_cells(row_indices);
    }
    cols
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CellValue;
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
    fn test_scan_indices_skips_deleted() {
        let mut table = make_users_table();
        table.delete(1).unwrap();
        let indices = scan_indices(&table);
        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn test_materialize_all() {
        let mut table = make_users_table();
        table.delete(1).unwrap();
        let indices = scan_indices(&table);
        let cols = materialize(&table, &indices);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(cols[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    #[test]
    fn test_materialize_sparse() {
        let table = make_users_table();
        let indices = scan_indices(&table);
        // Only materialize column 2 (age)
        let cols = materialize_sparse(&table, &indices, &[2]);
        assert_eq!(cols.len(), 3);
        assert!(cols[0].is_empty()); // not materialized
        assert!(cols[1].is_empty()); // not materialized
        assert_eq!(cols[2], vec![CellValue::I64(30), CellValue::I64(25), CellValue::I64(35)]);
    }
}
