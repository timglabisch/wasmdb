use std::collections::HashMap;

use sql_engine::execute::{Columns, ExecuteError, Params, Span};
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

    /// Execute multiple statements (CREATE, INSERT, SELECT), discarding results.
    pub fn execute_all(&mut self, sql: &str) -> Result<(), DbError> {
        let stmts = sql_parser::parser::parse_statements(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        for stmt in stmts {
            self.execute_statement(stmt, HashMap::new())?;
        }
        Ok(())
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
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

    pub fn execute(&mut self, sql: &str) -> Result<Columns, DbError> {
        self.execute_with_params(sql, HashMap::new())
    }

    pub fn execute_with_params(&mut self, sql: &str, params: Params) -> Result<Columns, DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        self.execute_statement(stmt, params)
    }

    pub fn execute_traced(&mut self, sql: &str) -> Result<(Columns, Vec<Span>), DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        match stmt {
            Statement::Select(select) => {
                crate::select::execute_select_traced(&self.tables, &select, HashMap::new())
            }
            _ => {
                let result = self.execute_statement(stmt, HashMap::new())?;
                Ok((result, vec![]))
            }
        }
    }

    fn execute_statement(&mut self, stmt: Statement, params: Params) -> Result<Columns, DbError> {
        match stmt {
            Statement::Select(select) => {
                crate::select::execute_select(&self.tables, &select, params)
            }
            Statement::Insert(insert) => {
                crate::insert::execute_insert(&mut self.tables, &insert)?;
                Ok(vec![])
            }
            Statement::CreateTable(ct) => {
                let schema = sql_engine::schema::resolve(&ct)
                    .map_err(|e| DbError::Parse(format!("{e:?}")))?;
                self.create_table(schema)?;
                Ok(vec![])
            }
        }
    }
}
