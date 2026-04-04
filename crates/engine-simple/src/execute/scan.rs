use crate::storage::Table;

use super::Columns;

pub fn scan(table: &Table) -> Columns {
    let num_cols = table.columns.len();
    let mut columns: Columns = (0..num_cols).map(|_| Vec::new()).collect();

    for row_idx in table.row_indices() {
        for col_idx in 0..num_cols {
            columns[col_idx].push(table.get(row_idx, col_idx));
        }
    }
    columns
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
    fn test_scan_reads_live_rows() {
        let mut table = make_users_table();
        table.delete(1).unwrap();
        let cols = scan(&table);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].len(), 2);
        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(cols[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }
}
