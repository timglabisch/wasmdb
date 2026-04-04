use crate::ast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    I64,
    String,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: Option<String>,
    pub columns: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<usize>,
    pub indexes: Vec<IndexSchema>,
}

#[derive(Debug)]
pub enum SchemaError {
    UnknownColumn { column: String },
    PrimaryKeyNullable { column: String },
    DuplicateColumn { column: String },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::UnknownColumn { column } => write!(f, "unknown column: {column}"),
            SchemaError::PrimaryKeyNullable { column } => {
                write!(f, "primary key column '{column}' must be NOT NULL")
            }
            SchemaError::DuplicateColumn { column } => {
                write!(f, "duplicate column: {column}")
            }
        }
    }
}

impl std::error::Error for SchemaError {}

pub fn resolve(create: &ast::AstCreateTable) -> Result<TableSchema, SchemaError> {
    // Check for duplicate column names.
    let mut seen = std::collections::HashSet::new();
    for col in &create.columns {
        if !seen.insert(&col.name) {
            return Err(SchemaError::DuplicateColumn {
                column: col.name.clone(),
            });
        }
    }

    let columns: Vec<ColumnSchema> = create
        .columns
        .iter()
        .map(|c| ColumnSchema {
            name: c.name.clone(),
            data_type: convert_data_type(c.data_type),
            nullable: !c.not_null,
        })
        .collect();

    // Collect primary key indices.
    let mut pk_indices = Vec::new();

    // From inline PRIMARY KEY on columns.
    for (i, col) in create.columns.iter().enumerate() {
        if col.primary_key {
            pk_indices.push(i);
        }
    }

    // From table-level PRIMARY KEY constraint.
    for constraint in &create.constraints {
        if let ast::AstTableConstraint::PrimaryKey {
            columns: pk_cols,
        } = constraint
        {
            for col_name in pk_cols {
                let idx = resolve_column_idx(col_name, &columns)?;
                if !pk_indices.contains(&idx) {
                    pk_indices.push(idx);
                }
            }
        }
    }

    // Validate PK columns are NOT NULL.
    for &idx in &pk_indices {
        if columns[idx].nullable {
            return Err(SchemaError::PrimaryKeyNullable {
                column: columns[idx].name.clone(),
            });
        }
    }

    // Resolve indexes.
    let mut indexes = Vec::new();
    for constraint in &create.constraints {
        if let ast::AstTableConstraint::Index {
            name,
            columns: idx_cols,
        } = constraint
        {
            let col_indices = idx_cols
                .iter()
                .map(|c| resolve_column_idx(c, &columns))
                .collect::<Result<Vec<_>, _>>()?;
            indexes.push(IndexSchema {
                name: name.clone(),
                columns: col_indices,
            });
        }
    }

    Ok(TableSchema {
        name: create.name.clone(),
        columns,
        primary_key: pk_indices,
        indexes,
    })
}

fn convert_data_type(dt: ast::AstDataType) -> DataType {
    match dt {
        ast::AstDataType::I64 => DataType::I64,
        ast::AstDataType::String => DataType::String,
    }
}

fn resolve_column_idx(name: &str, columns: &[ColumnSchema]) -> Result<usize, SchemaError> {
    columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| SchemaError::UnknownColumn {
            column: name.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;

    fn resolve_sql(sql: &str) -> Result<TableSchema, SchemaError> {
        let ast = parser::parse(sql).expect("parse failed");
        resolve(&ast)
    }

    #[test]
    fn test_simple_resolve() {
        let schema = resolve_sql("CREATE TABLE users (id I64 NOT NULL, name STRING)").unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].data_type, DataType::I64);
        assert!(!schema.columns[0].nullable);
        assert_eq!(schema.columns[1].data_type, DataType::String);
        assert!(schema.columns[1].nullable);
    }

    #[test]
    fn test_primary_key_resolve() {
        let schema = resolve_sql(
            "CREATE TABLE t (id I64 NOT NULL PRIMARY KEY, name STRING)"
        ).unwrap();
        assert_eq!(schema.primary_key, vec![0]);
    }

    #[test]
    fn test_constraint_primary_key_resolve() {
        let schema = resolve_sql(
            "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL, PRIMARY KEY (a, b))"
        ).unwrap();
        assert_eq!(schema.primary_key, vec![0, 1]);
    }

    #[test]
    fn test_pk_nullable_error() {
        let err = resolve_sql(
            "CREATE TABLE t (a I64, b I64, PRIMARY KEY (a))"
        ).unwrap_err();
        assert!(matches!(err, SchemaError::PrimaryKeyNullable { .. }));
    }

    #[test]
    fn test_duplicate_column_error() {
        let err = resolve_sql(
            "CREATE TABLE t (id I64, id STRING)"
        ).unwrap_err();
        assert!(matches!(err, SchemaError::DuplicateColumn { .. }));
    }

    #[test]
    fn test_index_resolve() {
        let schema = resolve_sql(
            "CREATE TABLE t (id I64, name STRING, age I64, INDEX idx_name (name), INDEX (name, age))"
        ).unwrap();
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.indexes[0].name.as_deref(), Some("idx_name"));
        assert_eq!(schema.indexes[0].columns, vec![1]);
        assert!(schema.indexes[1].name.is_none());
        assert_eq!(schema.indexes[1].columns, vec![1, 2]);
    }

    #[test]
    fn test_unknown_column_in_index() {
        let err = resolve_sql(
            "CREATE TABLE t (id I64, INDEX (nonexistent))"
        ).unwrap_err();
        assert!(matches!(err, SchemaError::UnknownColumn { .. }));
    }

    #[test]
    fn test_full_example() {
        let schema = resolve_sql("
            CREATE TABLE users (
                id I64 NOT NULL PRIMARY KEY,
                name STRING NOT NULL,
                age I64,
                email STRING,
                INDEX idx_email (email),
                INDEX idx_name_age (name, age)
            );
        ").unwrap();

        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 4);
        assert_eq!(schema.primary_key, vec![0]);
        assert_eq!(schema.indexes.len(), 2);

        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
        assert!(schema.columns[2].nullable);
        assert!(schema.columns[3].nullable);

        assert_eq!(schema.indexes[0].columns, vec![3]); // email
        assert_eq!(schema.indexes[1].columns, vec![1, 2]); // name, age
    }
}
