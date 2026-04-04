use schema_engine::schema::{DataType, TableSchema};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CellValue {
    I64(i64),
    Str(String),
    Null,
}

#[derive(Debug)]
pub enum TypedColumn {
    I64(Vec<i64>),
    Str(Vec<String>),
    NullableI64(Vec<Option<i64>>),
    NullableStr(Vec<Option<String>>),
}

impl TypedColumn {
    fn new(data_type: DataType, nullable: bool) -> Self {
        match (data_type, nullable) {
            (DataType::I64, false) => TypedColumn::I64(Vec::new()),
            (DataType::String, false) => TypedColumn::Str(Vec::new()),
            (DataType::I64, true) => TypedColumn::NullableI64(Vec::new()),
            (DataType::String, true) => TypedColumn::NullableStr(Vec::new()),
        }
    }

    fn push(&mut self, value: &CellValue) -> Result<(), StorageError> {
        match (self, value) {
            (TypedColumn::I64(v), CellValue::I64(val)) => v.push(*val),
            (TypedColumn::Str(v), CellValue::Str(val)) => v.push(val.clone()),

            (TypedColumn::NullableI64(v), CellValue::I64(val)) => v.push(Some(*val)),
            (TypedColumn::NullableI64(v), CellValue::Null) => v.push(None),
            (TypedColumn::NullableStr(v), CellValue::Str(val)) => v.push(Some(val.clone())),
            (TypedColumn::NullableStr(v), CellValue::Null) => v.push(None),

            (TypedColumn::I64(_), CellValue::Null)
            | (TypedColumn::Str(_), CellValue::Null) => {
                return Err(StorageError::NullInNonNullable);
            }

            _ => return Err(StorageError::TypeMismatch),
        }
        Ok(())
    }

    fn get(&self, row_idx: usize) -> CellValue {
        match self {
            TypedColumn::I64(v) => CellValue::I64(v[row_idx]),
            TypedColumn::Str(v) => CellValue::Str(v[row_idx].clone()),
            TypedColumn::NullableI64(v) => match v[row_idx] {
                Some(val) => CellValue::I64(val),
                None => CellValue::Null,
            },
            TypedColumn::NullableStr(v) => match &v[row_idx] {
                Some(val) => CellValue::Str(val.clone()),
                None => CellValue::Null,
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            TypedColumn::I64(v) => v.len(),
            TypedColumn::Str(v) => v.len(),
            TypedColumn::NullableI64(v) => v.len(),
            TypedColumn::NullableStr(v) => v.len(),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    TypeMismatch,
    NullInNonNullable,
    ColumnCountMismatch { expected: usize, got: usize },
    RowNotFound { row_idx: usize },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::TypeMismatch => write!(f, "type mismatch"),
            StorageError::NullInNonNullable => write!(f, "null value in non-nullable column"),
            StorageError::ColumnCountMismatch { expected, got } => {
                write!(f, "expected {expected} columns, got {got}")
            }
            StorageError::RowNotFound { row_idx } => {
                write!(f, "row {row_idx} not found or deleted")
            }
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug)]
pub struct Table {
    pub schema: TableSchema,
    pub columns: Vec<TypedColumn>,
    deleted: Vec<bool>,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|col| TypedColumn::new(col.data_type, col.nullable))
            .collect();
        Table {
            schema,
            columns,
            deleted: Vec::new(),
        }
    }

    /// Number of physical rows (including deleted).
    pub fn physical_len(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    /// Number of live (non-deleted) rows.
    pub fn len(&self) -> usize {
        self.physical_len() - self.deleted.iter().filter(|&&d| d).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_deleted(&self, row_idx: usize) -> bool {
        self.deleted[row_idx]
    }

    pub fn insert(&mut self, row: &[CellValue]) -> Result<usize, StorageError> {
        if row.len() != self.columns.len() {
            return Err(StorageError::ColumnCountMismatch {
                expected: self.columns.len(),
                got: row.len(),
            });
        }
        let row_idx = self.physical_len();
        for (col, val) in self.columns.iter_mut().zip(row.iter()) {
            col.push(val)?;
        }
        self.deleted.push(false);
        Ok(row_idx)
    }

    pub fn delete(&mut self, row_idx: usize) -> Result<(), StorageError> {
        if row_idx >= self.physical_len() || self.deleted[row_idx] {
            return Err(StorageError::RowNotFound { row_idx });
        }
        self.deleted[row_idx] = true;
        Ok(())
    }

    pub fn get(&self, row_idx: usize, col_idx: usize) -> CellValue {
        self.columns[col_idx].get(row_idx)
    }

    /// Iterator over live (non-deleted) row indices.
    pub fn row_indices(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.physical_len()).filter(|&i| !self.deleted[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema_engine::schema::ColumnSchema;

    fn users_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    #[test]
    fn test_create_table_from_schema() {
        let table = Table::new(users_schema());
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = Table::new(users_schema());
        table.insert(&[
            CellValue::I64(1),
            CellValue::Str("Alice".into()),
            CellValue::I64(30),
        ]).unwrap();
        table.insert(&[
            CellValue::I64(2),
            CellValue::Str("Bob".into()),
            CellValue::Null,
        ]).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(0, 0), CellValue::I64(1));
        assert_eq!(table.get(0, 1), CellValue::Str("Alice".into()));
        assert_eq!(table.get(0, 2), CellValue::I64(30));
        assert_eq!(table.get(1, 0), CellValue::I64(2));
        assert_eq!(table.get(1, 1), CellValue::Str("Bob".into()));
        assert_eq!(table.get(1, 2), CellValue::Null);
    }

    #[test]
    fn test_null_in_non_nullable_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[
            CellValue::Null,
            CellValue::Str("Alice".into()),
            CellValue::I64(30),
        ]).unwrap_err();
        assert!(matches!(err, StorageError::NullInNonNullable));
    }

    #[test]
    fn test_type_mismatch_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[
            CellValue::Str("not_an_i64".into()),
            CellValue::Str("Alice".into()),
            CellValue::I64(30),
        ]).unwrap_err();
        assert!(matches!(err, StorageError::TypeMismatch));
    }

    #[test]
    fn test_wrong_column_count_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[CellValue::I64(1)]).unwrap_err();
        assert!(matches!(err, StorageError::ColumnCountMismatch { expected: 3, got: 1 }));
    }

    #[test]
    fn test_both_types() {
        let schema = TableSchema {
            name: "both_types".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[
            CellValue::I64(-100),
            CellValue::Str("hello".into()),
        ]).unwrap();

        assert_eq!(table.get(0, 0), CellValue::I64(-100));
        assert_eq!(table.get(0, 1), CellValue::Str("hello".into()));
    }

    #[test]
    fn test_delete_tombstone() {
        let mut table = Table::new(users_schema());
        let r0 = table.insert(&[
            CellValue::I64(1),
            CellValue::Str("Alice".into()),
            CellValue::I64(30),
        ]).unwrap();
        let r1 = table.insert(&[
            CellValue::I64(2),
            CellValue::Str("Bob".into()),
            CellValue::Null,
        ]).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.physical_len(), 2);

        table.delete(r0).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table.physical_len(), 2);
        assert!(table.is_deleted(r0));
        assert!(!table.is_deleted(r1));

        assert_eq!(table.get(r0, 1), CellValue::Str("Alice".into()));
    }

    #[test]
    fn test_row_indices_skips_deleted() {
        let mut table = Table::new(users_schema());
        table.insert(&[CellValue::I64(1), CellValue::Str("A".into()), CellValue::I64(1)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::Str("B".into()), CellValue::I64(2)]).unwrap();
        table.insert(&[CellValue::I64(3), CellValue::Str("C".into()), CellValue::I64(3)]).unwrap();

        table.delete(1).unwrap();

        let live: Vec<usize> = table.row_indices().collect();
        assert_eq!(live, vec![0, 2]);
    }

    #[test]
    fn test_delete_already_deleted_fails() {
        let mut table = Table::new(users_schema());
        table.insert(&[CellValue::I64(1), CellValue::Str("A".into()), CellValue::I64(1)]).unwrap();
        table.delete(0).unwrap();
        let err = table.delete(0).unwrap_err();
        assert!(matches!(err, StorageError::RowNotFound { row_idx: 0 }));
    }

    #[test]
    fn test_delete_out_of_bounds_fails() {
        let table = Table::new(users_schema());
        let mut table = table;
        let err = table.delete(0).unwrap_err();
        assert!(matches!(err, StorageError::RowNotFound { row_idx: 0 }));
    }

    #[test]
    fn test_cell_value_ordering() {
        assert!(CellValue::I64(1) < CellValue::I64(2));
        assert!(CellValue::Str("a".into()) < CellValue::Str("b".into()));
        assert!(CellValue::I64(-1) < CellValue::I64(1));
    }

    #[test]
    fn test_from_parsed_schema() {
        let ast = schema_engine::parser::parse(
            "CREATE TABLE orders (
                id I64 NOT NULL PRIMARY KEY,
                user_id I64 NOT NULL,
                amount I64,
                INDEX idx_user (user_id)
            );"
        ).unwrap();
        let schema = schema_engine::schema::resolve(&ast).unwrap();

        let mut table = Table::new(schema);
        table.insert(&[
            CellValue::I64(1),
            CellValue::I64(42),
            CellValue::I64(9999),
        ]).unwrap();
        table.insert(&[
            CellValue::I64(2),
            CellValue::I64(42),
            CellValue::Null,
        ]).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(0, 2), CellValue::I64(9999));
        assert_eq!(table.get(1, 2), CellValue::Null);
    }
}
