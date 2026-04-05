use std::collections::{BTreeMap, HashMap};

use crate::bitmap::Bitmap;
use schema_engine::schema::{DataType, IndexType, TableSchema};

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
    NullableI64 { values: Vec<i64>, nulls: Bitmap },
    NullableStr { values: Vec<String>, nulls: Bitmap },
}

impl TypedColumn {
    fn new(data_type: DataType, nullable: bool) -> Self {
        match (data_type, nullable) {
            (DataType::I64, false) => TypedColumn::I64(Vec::new()),
            (DataType::String, false) => TypedColumn::Str(Vec::new()),
            (DataType::I64, true) => TypedColumn::NullableI64 { values: Vec::new(), nulls: Bitmap::with_capacity(0) },
            (DataType::String, true) => TypedColumn::NullableStr { values: Vec::new(), nulls: Bitmap::with_capacity(0) },
        }
    }

    fn push(&mut self, value: &CellValue) -> Result<(), StorageError> {
        match (self, value) {
            (TypedColumn::I64(v), CellValue::I64(val)) => v.push(*val),
            (TypedColumn::Str(v), CellValue::Str(val)) => v.push(val.clone()),

            (TypedColumn::NullableI64 { values, nulls }, CellValue::I64(val)) => {
                values.push(*val);
                nulls.push(false);
            }
            (TypedColumn::NullableI64 { values, nulls }, CellValue::Null) => {
                values.push(0);
                nulls.push(true);
            }
            (TypedColumn::NullableStr { values, nulls }, CellValue::Str(val)) => {
                values.push(val.clone());
                nulls.push(false);
            }
            (TypedColumn::NullableStr { values, nulls }, CellValue::Null) => {
                values.push(String::new());
                nulls.push(true);
            }

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
            TypedColumn::NullableI64 { values, nulls } => {
                if nulls.get(row_idx) { CellValue::Null } else { CellValue::I64(values[row_idx]) }
            }
            TypedColumn::NullableStr { values, nulls } => {
                if nulls.get(row_idx) { CellValue::Null } else { CellValue::Str(values[row_idx].clone()) }
            }
        }
    }

    /// Batch-convert specific rows to CellValues (column-at-a-time, no per-cell dispatch).
    pub fn to_cells(&self, row_ids: &[usize]) -> Vec<CellValue> {
        match self {
            TypedColumn::I64(v) => row_ids.iter().map(|&i| CellValue::I64(v[i])).collect(),
            TypedColumn::Str(v) => row_ids.iter().map(|&i| CellValue::Str(v[i].clone())).collect(),
            TypedColumn::NullableI64 { values, nulls } => row_ids.iter().map(|&i| {
                if nulls.get(i) { CellValue::Null } else { CellValue::I64(values[i]) }
            }).collect(),
            TypedColumn::NullableStr { values, nulls } => row_ids.iter().map(|&i| {
                if nulls.get(i) { CellValue::Null } else { CellValue::Str(values[i].clone()) }
            }).collect(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TypedColumn::I64(v) => v.len(),
            TypedColumn::Str(v) => v.len(),
            TypedColumn::NullableI64 { values, .. } => values.len(),
            TypedColumn::NullableStr { values, .. } => values.len(),
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

/// A single-column index backed by either a BTreeMap or HashMap.
#[derive(Debug)]
pub enum TableIndex {
    BTree {
        column: usize,
        map: BTreeMap<CellValue, Vec<usize>>,
    },
    Hash {
        column: usize,
        map: HashMap<CellValue, Vec<usize>>,
    },
}

impl TableIndex {
    fn new(column: usize, index_type: IndexType) -> Self {
        match index_type {
            IndexType::BTree => TableIndex::BTree { column, map: BTreeMap::new() },
            IndexType::Hash => TableIndex::Hash { column, map: HashMap::new() },
        }
    }

    pub fn column(&self) -> usize {
        match self {
            TableIndex::BTree { column, .. } => *column,
            TableIndex::Hash { column, .. } => *column,
        }
    }

    fn insert(&mut self, value: CellValue, row_id: usize) {
        match self {
            TableIndex::BTree { map, .. } => map.entry(value).or_default().push(row_id),
            TableIndex::Hash { map, .. } => map.entry(value).or_default().push(row_id),
        }
    }

    fn remove(&mut self, value: &CellValue, row_id: usize) {
        match self {
            TableIndex::BTree { map, .. } => {
                if let Some(ids) = map.get_mut(value) {
                    ids.retain(|&id| id != row_id);
                    if ids.is_empty() { map.remove(value); }
                }
            }
            TableIndex::Hash { map, .. } => {
                if let Some(ids) = map.get_mut(value) {
                    ids.retain(|&id| id != row_id);
                    if ids.is_empty() { map.remove(value); }
                }
            }
        }
    }

    /// Equality lookup — works for both BTree and Hash.
    pub fn lookup_eq(&self, value: &CellValue) -> Option<&[usize]> {
        match self {
            TableIndex::BTree { map, .. } => map.get(value).map(|v| v.as_slice()),
            TableIndex::Hash { map, .. } => map.get(value).map(|v| v.as_slice()),
        }
    }

    /// Range lookup — only BTree supports this. Returns row_ids matching the range.
    /// NULL keys are excluded (SQL semantics: NULL comparisons are always false).
    pub fn lookup_range(&self, op: RangeOp, value: &CellValue) -> Option<Vec<usize>> {
        match self {
            TableIndex::BTree { map, .. } => {
                use std::ops::Bound::*;
                // Cap upper bound at Excluded(Null) so NULL entries never appear.
                let null_upper = Excluded(CellValue::Null);
                let iter: Box<dyn Iterator<Item = (&CellValue, &Vec<usize>)>> = match op {
                    RangeOp::Gt => Box::new(map.range((Excluded(value.clone()), null_upper))),
                    RangeOp::Gte => Box::new(map.range((Included(value.clone()), null_upper))),
                    RangeOp::Lt => Box::new(map.range((Unbounded, Excluded(value.clone())))),
                    RangeOp::Lte => Box::new(map.range((Unbounded, Included(value.clone())))),
                };
                let mut result = Vec::new();
                for (k, ids) in iter {
                    if *k != CellValue::Null {
                        result.extend_from_slice(ids);
                    }
                }
                Some(result)
            }
            TableIndex::Hash { .. } => None, // Hash does not support range queries
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RangeOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug)]
pub struct Table {
    pub schema: TableSchema,
    pub columns: Vec<TypedColumn>,
    deleted: Bitmap,
    indexes: Vec<TableIndex>,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|col| TypedColumn::new(col.data_type, col.nullable))
            .collect();
        // Build single-column indexes (composite indexes are not yet supported).
        let indexes = schema
            .indexes
            .iter()
            .filter(|idx| idx.columns.len() == 1)
            .map(|idx| TableIndex::new(idx.columns[0], idx.index_type))
            .collect();
        Table {
            schema,
            columns,
            deleted: Bitmap::with_capacity(0),
            indexes,
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
        self.physical_len() - self.deleted.count_ones()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_deleted(&self, row_idx: usize) -> bool {
        self.deleted.get(row_idx)
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
        for idx in &mut self.indexes {
            idx.insert(row[idx.column()].clone(), row_idx);
        }
        Ok(row_idx)
    }

    pub fn delete(&mut self, row_idx: usize) -> Result<(), StorageError> {
        if row_idx >= self.physical_len() || self.deleted.get(row_idx) {
            return Err(StorageError::RowNotFound { row_idx });
        }
        for idx in &mut self.indexes {
            let value = self.columns[idx.column()].get(row_idx);
            idx.remove(&value, row_idx);
        }
        self.deleted.set(row_idx, true);
        Ok(())
    }

    pub fn get(&self, row_idx: usize, col_idx: usize) -> CellValue {
        self.columns[col_idx].get(row_idx)
    }

    /// Find an index for the given column, if one exists.
    pub fn index_for_column(&self, col: usize) -> Option<&TableIndex> {
        self.indexes.iter().find(|idx| idx.column() == col)
    }

    /// Iterator over live (non-deleted) row IDs.
    pub fn row_ids(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.physical_len()).filter(move |&i| !self.deleted.get(i))
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
    fn test_row_ids_skips_deleted() {
        let mut table = Table::new(users_schema());
        table.insert(&[CellValue::I64(1), CellValue::Str("A".into()), CellValue::I64(1)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::Str("B".into()), CellValue::I64(2)]).unwrap();
        table.insert(&[CellValue::I64(3), CellValue::Str("C".into()), CellValue::I64(3)]).unwrap();

        table.delete(1).unwrap();

        let live: Vec<usize> = table.row_ids().collect();
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

    fn indexed_schema() -> TableSchema {
        use schema_engine::schema::IndexSchema;
        TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![
                IndexSchema { name: Some("idx_user".into()), columns: vec![1], index_type: IndexType::BTree },
                IndexSchema { name: Some("idx_id_hash".into()), columns: vec![0], index_type: IndexType::Hash },
            ],
        }
    }

    #[test]
    fn test_index_btree_eq_lookup() {
        let mut table = Table::new(indexed_schema());
        table.insert(&[CellValue::I64(1), CellValue::I64(42), CellValue::I64(100)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::I64(42), CellValue::I64(200)]).unwrap();
        table.insert(&[CellValue::I64(3), CellValue::I64(99), CellValue::I64(300)]).unwrap();

        let idx = table.index_for_column(1).unwrap();
        assert_eq!(idx.lookup_eq(&CellValue::I64(42)), Some([0, 1].as_slice()));
        assert_eq!(idx.lookup_eq(&CellValue::I64(99)), Some([2].as_slice()));
        assert_eq!(idx.lookup_eq(&CellValue::I64(0)), None);
    }

    #[test]
    fn test_index_hash_eq_lookup() {
        let mut table = Table::new(indexed_schema());
        table.insert(&[CellValue::I64(10), CellValue::I64(1), CellValue::I64(100)]).unwrap();
        table.insert(&[CellValue::I64(20), CellValue::I64(2), CellValue::I64(200)]).unwrap();

        let idx = table.index_for_column(0).unwrap();
        assert_eq!(idx.lookup_eq(&CellValue::I64(10)), Some([0].as_slice()));
        assert_eq!(idx.lookup_eq(&CellValue::I64(20)), Some([1].as_slice()));
        assert_eq!(idx.lookup_eq(&CellValue::I64(99)), None);
    }

    #[test]
    fn test_index_btree_range_lookup() {
        let mut table = Table::new(indexed_schema());
        table.insert(&[CellValue::I64(1), CellValue::I64(10), CellValue::I64(0)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::I64(20), CellValue::I64(0)]).unwrap();
        table.insert(&[CellValue::I64(3), CellValue::I64(30), CellValue::I64(0)]).unwrap();
        table.insert(&[CellValue::I64(4), CellValue::I64(40), CellValue::I64(0)]).unwrap();

        let idx = table.index_for_column(1).unwrap();
        assert_eq!(idx.lookup_range(RangeOp::Gt, &CellValue::I64(20)).unwrap(), vec![2, 3]);
        assert_eq!(idx.lookup_range(RangeOp::Gte, &CellValue::I64(20)).unwrap(), vec![1, 2, 3]);
        assert_eq!(idx.lookup_range(RangeOp::Lt, &CellValue::I64(30)).unwrap(), vec![0, 1]);
        assert_eq!(idx.lookup_range(RangeOp::Lte, &CellValue::I64(30)).unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn test_index_hash_range_unsupported() {
        let mut table = Table::new(indexed_schema());
        table.insert(&[CellValue::I64(1), CellValue::I64(10), CellValue::I64(0)]).unwrap();

        let idx = table.index_for_column(0).unwrap();
        assert!(idx.lookup_range(RangeOp::Gt, &CellValue::I64(0)).is_none());
    }

    #[test]
    fn test_index_maintained_after_delete() {
        let mut table = Table::new(indexed_schema());
        table.insert(&[CellValue::I64(1), CellValue::I64(42), CellValue::I64(100)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::I64(42), CellValue::I64(200)]).unwrap();

        table.delete(0).unwrap();

        let idx = table.index_for_column(1).unwrap();
        assert_eq!(idx.lookup_eq(&CellValue::I64(42)), Some([1].as_slice()));
    }

    #[test]
    fn test_index_for_column_none() {
        let table = Table::new(users_schema()); // no indexes
        assert!(table.index_for_column(0).is_none());
    }
}
