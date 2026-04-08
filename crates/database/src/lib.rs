use std::collections::HashMap;

use ddl_parser::schema::TableSchema;
use sql_engine::execute::{self, Columns, ExecuteError, ExecutionContext, Params};
use sql_engine::planner::{self, PlanError};
use sql_engine::storage::{CellValue, Table};

#[derive(Debug)]
pub enum DbError {
    Parse(String),
    Plan(PlanError),
    Execute(ExecuteError),
    TableAlreadyExists(String),
    TableNotFound(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Parse(msg) => write!(f, "parse error: {msg}"),
            DbError::Plan(e) => write!(f, "plan error: {e}"),
            DbError::Execute(e) => write!(f, "execute error: {e}"),
            DbError::TableAlreadyExists(t) => write!(f, "table already exists: {t}"),
            DbError::TableNotFound(t) => write!(f, "table not found: {t}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<PlanError> for DbError {
    fn from(e: PlanError) -> Self { DbError::Plan(e) }
}

impl From<ExecuteError> for DbError {
    fn from(e: ExecuteError) -> Self { DbError::Execute(e) }
}

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
        for stmt in ddl.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() { continue; }
            let ast = ddl_parser::parser::parse(&format!("{stmt};"))
                .map_err(|e| DbError::Parse(format!("{e:?}")))?;
            let schema = ddl_parser::schema::resolve(&ast)
                .map_err(|e| DbError::Parse(format!("{e:?}")))?;
            self.create_table(schema)?;
        }
        Ok(())
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

    pub fn execute(&self, sql: &str) -> Result<Columns, DbError> {
        self.execute_with_params(sql, HashMap::new())
    }

    pub fn execute_with_params(&self, sql: &str, params: Params) -> Result<Columns, DbError> {
        let ast = sql_parser::parser::parse(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        let table_schemas = self.table_schemas();
        let plan = planner::plan(&ast, &table_schemas)?;
        let mut ctx = ExecutionContext::with_params(&self.tables, params);
        let result = execute::execute_plan(&mut ctx, &plan)?;
        Ok(result)
    }

    fn table_schemas(&self) -> HashMap<String, TableSchema> {
        self.tables.iter()
            .map(|(name, table)| (name.clone(), table.schema.clone()))
            .collect()
    }
}
