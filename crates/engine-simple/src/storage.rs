use schema_engine::schema::{DataType, TableSchema};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CellValue {
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Str(String),
    Null,
}

#[derive(Debug)]
pub enum TypedColumn {
    U32(Vec<u32>),
    I32(Vec<i32>),
    U64(Vec<u64>),
    I64(Vec<i64>),
    Str(Vec<String>),
    NullableU32(Vec<Option<u32>>),
    NullableI32(Vec<Option<i32>>),
    NullableU64(Vec<Option<u64>>),
    NullableI64(Vec<Option<i64>>),
    NullableStr(Vec<Option<String>>),
}

impl TypedColumn {
    fn new(data_type: DataType, nullable: bool) -> Self {
        match (data_type, nullable) {
            (DataType::U32, false) => TypedColumn::U32(Vec::new()),
            (DataType::I32, false) => TypedColumn::I32(Vec::new()),
            (DataType::U64, false) => TypedColumn::U64(Vec::new()),
            (DataType::I64, false) => TypedColumn::I64(Vec::new()),
            (DataType::String, false) => TypedColumn::Str(Vec::new()),
            (DataType::U32, true) => TypedColumn::NullableU32(Vec::new()),
            (DataType::I32, true) => TypedColumn::NullableI32(Vec::new()),
            (DataType::U64, true) => TypedColumn::NullableU64(Vec::new()),
            (DataType::I64, true) => TypedColumn::NullableI64(Vec::new()),
            (DataType::String, true) => TypedColumn::NullableStr(Vec::new()),
        }
    }

    fn push(&mut self, value: &CellValue) -> Result<(), StorageError> {
        match (self, value) {
            (TypedColumn::U32(v), CellValue::U32(val)) => v.push(*val),
            (TypedColumn::I32(v), CellValue::I32(val)) => v.push(*val),
            (TypedColumn::U64(v), CellValue::U64(val)) => v.push(*val),
            (TypedColumn::I64(v), CellValue::I64(val)) => v.push(*val),
            (TypedColumn::Str(v), CellValue::Str(val)) => v.push(val.clone()),

            (TypedColumn::NullableU32(v), CellValue::U32(val)) => v.push(Some(*val)),
            (TypedColumn::NullableU32(v), CellValue::Null) => v.push(None),
            (TypedColumn::NullableI32(v), CellValue::I32(val)) => v.push(Some(*val)),
            (TypedColumn::NullableI32(v), CellValue::Null) => v.push(None),
            (TypedColumn::NullableU64(v), CellValue::U64(val)) => v.push(Some(*val)),
            (TypedColumn::NullableU64(v), CellValue::Null) => v.push(None),
            (TypedColumn::NullableI64(v), CellValue::I64(val)) => v.push(Some(*val)),
            (TypedColumn::NullableI64(v), CellValue::Null) => v.push(None),
            (TypedColumn::NullableStr(v), CellValue::Str(val)) => v.push(Some(val.clone())),
            (TypedColumn::NullableStr(v), CellValue::Null) => v.push(None),

            // Non-nullable column got Null.
            (TypedColumn::U32(_), CellValue::Null)
            | (TypedColumn::I32(_), CellValue::Null)
            | (TypedColumn::U64(_), CellValue::Null)
            | (TypedColumn::I64(_), CellValue::Null)
            | (TypedColumn::Str(_), CellValue::Null) => {
                return Err(StorageError::NullInNonNullable);
            }

            _ => return Err(StorageError::TypeMismatch),
        }
        Ok(())
    }

    fn get(&self, row_idx: usize) -> CellValue {
        match self {
            TypedColumn::U32(v) => CellValue::U32(v[row_idx]),
            TypedColumn::I32(v) => CellValue::I32(v[row_idx]),
            TypedColumn::U64(v) => CellValue::U64(v[row_idx]),
            TypedColumn::I64(v) => CellValue::I64(v[row_idx]),
            TypedColumn::Str(v) => CellValue::Str(v[row_idx].clone()),
            TypedColumn::NullableU32(v) => match v[row_idx] {
                Some(val) => CellValue::U32(val),
                None => CellValue::Null,
            },
            TypedColumn::NullableI32(v) => match v[row_idx] {
                Some(val) => CellValue::I32(val),
                None => CellValue::Null,
            },
            TypedColumn::NullableU64(v) => match v[row_idx] {
                Some(val) => CellValue::U64(val),
                None => CellValue::Null,
            },
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
            TypedColumn::U32(v) => v.len(),
            TypedColumn::I32(v) => v.len(),
            TypedColumn::U64(v) => v.len(),
            TypedColumn::I64(v) => v.len(),
            TypedColumn::Str(v) => v.len(),
            TypedColumn::NullableU32(v) => v.len(),
            TypedColumn::NullableI32(v) => v.len(),
            TypedColumn::NullableU64(v) => v.len(),
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
                ColumnSchema { name: "id".into(), data_type: DataType::U64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I32, nullable: true },
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
            CellValue::U64(1),
            CellValue::Str("Alice".into()),
            CellValue::I32(30),
        ]).unwrap();
        table.insert(&[
            CellValue::U64(2),
            CellValue::Str("Bob".into()),
            CellValue::Null,
        ]).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(0, 0), CellValue::U64(1));
        assert_eq!(table.get(0, 1), CellValue::Str("Alice".into()));
        assert_eq!(table.get(0, 2), CellValue::I32(30));
        assert_eq!(table.get(1, 0), CellValue::U64(2));
        assert_eq!(table.get(1, 1), CellValue::Str("Bob".into()));
        assert_eq!(table.get(1, 2), CellValue::Null);
    }

    #[test]
    fn test_null_in_non_nullable_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[
            CellValue::Null,
            CellValue::Str("Alice".into()),
            CellValue::I32(30),
        ]).unwrap_err();
        assert!(matches!(err, StorageError::NullInNonNullable));
    }

    #[test]
    fn test_type_mismatch_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[
            CellValue::Str("not_a_u64".into()),
            CellValue::Str("Alice".into()),
            CellValue::I32(30),
        ]).unwrap_err();
        assert!(matches!(err, StorageError::TypeMismatch));
    }

    #[test]
    fn test_wrong_column_count_fails() {
        let mut table = Table::new(users_schema());
        let err = table.insert(&[CellValue::U64(1)]).unwrap_err();
        assert!(matches!(err, StorageError::ColumnCountMismatch { expected: 3, got: 1 }));
    }

    #[test]
    fn test_all_types() {
        let schema = TableSchema {
            name: "all_types".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::U32, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::I32, nullable: false },
                ColumnSchema { name: "c".into(), data_type: DataType::U64, nullable: false },
                ColumnSchema { name: "d".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "e".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[
            CellValue::U32(1),
            CellValue::I32(-1),
            CellValue::U64(100),
            CellValue::I64(-100),
            CellValue::Str("hello".into()),
        ]).unwrap();

        assert_eq!(table.get(0, 0), CellValue::U32(1));
        assert_eq!(table.get(0, 1), CellValue::I32(-1));
        assert_eq!(table.get(0, 2), CellValue::U64(100));
        assert_eq!(table.get(0, 3), CellValue::I64(-100));
        assert_eq!(table.get(0, 4), CellValue::Str("hello".into()));
    }

    #[test]
    fn test_delete_tombstone() {
        let mut table = Table::new(users_schema());
        let r0 = table.insert(&[
            CellValue::U64(1),
            CellValue::Str("Alice".into()),
            CellValue::I32(30),
        ]).unwrap();
        let r1 = table.insert(&[
            CellValue::U64(2),
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

        // Data is still physically there.
        assert_eq!(table.get(r0, 1), CellValue::Str("Alice".into()));
    }

    #[test]
    fn test_row_indices_skips_deleted() {
        let mut table = Table::new(users_schema());
        table.insert(&[CellValue::U64(1), CellValue::Str("A".into()), CellValue::I32(1)]).unwrap();
        table.insert(&[CellValue::U64(2), CellValue::Str("B".into()), CellValue::I32(2)]).unwrap();
        table.insert(&[CellValue::U64(3), CellValue::Str("C".into()), CellValue::I32(3)]).unwrap();

        table.delete(1).unwrap();

        let live: Vec<usize> = table.row_indices().collect();
        assert_eq!(live, vec![0, 2]);
    }

    #[test]
    fn test_delete_already_deleted_fails() {
        let mut table = Table::new(users_schema());
        table.insert(&[CellValue::U64(1), CellValue::Str("A".into()), CellValue::I32(1)]).unwrap();
        table.delete(0).unwrap();
        let err = table.delete(0).unwrap_err();
        assert!(matches!(err, StorageError::RowNotFound { row_idx: 0 }));
    }

    #[test]
    fn test_delete_out_of_bounds_fails() {
        let table = Table::new(users_schema());
        // Can't delete from empty table — need mutable reference though.
        let mut table = table;
        let err = table.delete(0).unwrap_err();
        assert!(matches!(err, StorageError::RowNotFound { row_idx: 0 }));
    }

    #[test]
    fn test_cell_value_ordering() {
        assert!(CellValue::U64(1) < CellValue::U64(2));
        assert!(CellValue::Str("a".into()) < CellValue::Str("b".into()));
        assert!(CellValue::I32(-1) < CellValue::I32(1));
    }

    #[test]
    fn test_from_parsed_schema() {
        let ast = schema_engine::parser::parse(
            "CREATE TABLE orders (
                id U64 NOT NULL PRIMARY KEY,
                user_id U64 NOT NULL,
                amount I64,
                INDEX idx_user (user_id)
            );"
        ).unwrap();
        let schema = schema_engine::schema::resolve(&ast).unwrap();

        let mut table = Table::new(schema);
        table.insert(&[
            CellValue::U64(1),
            CellValue::U64(42),
            CellValue::I64(9999),
        ]).unwrap();
        table.insert(&[
            CellValue::U64(2),
            CellValue::U64(42),
            CellValue::Null,
        ]).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(0, 2), CellValue::I64(9999));
        assert_eq!(table.get(1, 2), CellValue::Null);
    }
}
