use std::collections::HashMap;

use sql_engine::execute::ExecuteError;
use sql_engine::schema::TableSchema;
use sql_engine::storage::{CellValue, Table};
use sql_parser::ast::Statement;

use crate::error::DbError;

#[derive(Clone)]
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), DbError> {
        if self.tables.contains_key(&schema.name) {
            return Err(DbError::TableAlreadyExists(schema.name.clone()));
        }
        let name = schema.name.clone();
        self.tables.insert(name, Table::new(schema));
        Ok(())
    }

    pub fn execute_ddl(&mut self, ddl: &str) -> Result<(), DbError> {
        let stmts = sql_parser::parser::parse_statements(ddl)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        for stmt in stmts {
            match stmt {
                Statement::CreateTable(ct) => {
                    let schema = sql_engine::schema::resolve(&ct)
                        .map_err(|e| DbError::Parse(format!("{e:?}")))?;
                    self.create_table(schema)?;
                }
                _ => return Err(DbError::Parse("expected CREATE TABLE statement".into())),
            }
        }
        Ok(())
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_schemas(&self) -> HashMap<String, TableSchema> {
        self.tables.iter()
            .map(|(name, table)| (name.clone(), table.schema.clone()))
            .collect()
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn insert(&mut self, table: &str, row: &[CellValue]) -> Result<usize, DbError> {
        let t = self.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.into()))?;
        t.insert(row).map_err(|e| DbError::Execute(ExecuteError::TableNotFound(format!("{e}"))))
    }

    pub(crate) fn tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    pub(crate) fn tables_mut(&mut self) -> &mut HashMap<String, Table> {
        &mut self.tables
    }
}
