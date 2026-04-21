mod apply;
mod delete;
mod filter;
mod insert;
mod select;
mod update;

use std::collections::HashMap;

use sql_engine::execute::{Columns, Params, Span};
use sql_engine::storage::{CellValue, ZSet};
use sql_parser::ast::Statement;

use crate::Database;
use crate::error::DbError;

#[derive(Debug, Clone)]
pub enum MutResult {
    Mutation(ZSet),
    Rows(Columns),
    Ddl,
}

impl Database {
    /// Execute multiple statements (CREATE, INSERT, SELECT), discarding results.
    pub fn execute_all(&mut self, sql: &str) -> Result<(), DbError> {
        let stmts = sql_parser::parser::parse_statements(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        for stmt in stmts {
            self.execute_statement(stmt, HashMap::new())?;
        }
        Ok(())
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
        self.execute_traced_with_triggered(sql, None)
    }

    pub fn execute_traced_with_triggered(
        &mut self,
        sql: &str,
        triggered_conditions: Option<std::collections::HashSet<usize>>,
    ) -> Result<(Columns, Vec<Span>), DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        match stmt {
            Statement::Select(select) => {
                select::execute_select_traced(self, &select, HashMap::new(), triggered_conditions)
            }
            _ => {
                let result = self.execute_statement(stmt, HashMap::new())?;
                Ok((result, vec![]))
            }
        }
    }

    pub fn execute_mut(&mut self, sql: &str) -> Result<MutResult, DbError> {
        self.execute_mut_with_params(sql, HashMap::new())
    }

    pub fn execute_mut_with_params(&mut self, sql: &str, params: Params) -> Result<MutResult, DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        self.execute_statement_mut(stmt, params)
    }

    // ── Async API ────────────────────────────────────────────────────────
    //
    // Use these when the query may contain a `schema.fn(args)` FROM source.
    // Phase 0 awaits all registered fetchers, upserts rows into the target
    // `row_table`, then runs the usual sync query over the populated state.
    //
    // For queries *without* fetcher sources, the sync methods above are
    // cheaper (no async runtime needed). The sync path returns
    // [`DbError::RequiresAsync`] if a fetcher source sneaks in.

    pub async fn execute_async(&mut self, sql: &str) -> Result<Columns, DbError> {
        self.execute_with_params_async(sql, HashMap::new()).await
    }

    pub async fn execute_with_params_async(
        &mut self, sql: &str, params: Params,
    ) -> Result<Columns, DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        match stmt {
            Statement::Select(s) => select::execute_select_async(self, &s, params).await,
            other => {
                // Mutationen / DDL kennen keine Requirements → sync-Pfad reicht.
                match self.execute_statement_mut(other, params)? {
                    MutResult::Rows(cols) => Ok(cols),
                    _ => Ok(vec![]),
                }
            }
        }
    }

    pub async fn execute_mut_async(&mut self, sql: &str) -> Result<MutResult, DbError> {
        self.execute_mut_with_params_async(sql, HashMap::new()).await
    }

    pub async fn execute_mut_with_params_async(
        &mut self, sql: &str, params: Params,
    ) -> Result<MutResult, DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        match stmt {
            Statement::Select(s) => {
                let cols = select::execute_select_async(self, &s, params).await?;
                Ok(MutResult::Rows(cols))
            }
            other => self.execute_statement_mut(other, params),
        }
    }

    fn execute_statement(&mut self, stmt: Statement, params: Params) -> Result<Columns, DbError> {
        match self.execute_statement_mut(stmt, params)? {
            MutResult::Rows(cols) => Ok(cols),
            _ => Ok(vec![]),
        }
    }

    fn execute_statement_mut(&mut self, stmt: Statement, params: Params) -> Result<MutResult, DbError> {
        match stmt {
            Statement::Select(select) => {
                let cols = select::execute_select(self, &select, params)?;
                Ok(MutResult::Rows(cols))
            }
            Statement::Insert(ref insert) => {
                let col_count = {
                    let table = self.table(&insert.table)
                        .ok_or_else(|| DbError::TableNotFound(insert.table.clone()))?;
                    table.schema.columns.len()
                };
                let before_count = self.table(&insert.table).map(|t| t.physical_len()).unwrap_or(0);
                insert::execute_insert(self.tables_mut(), insert, &params)?;
                let table = self.table(&insert.table).unwrap();
                let after_count = table.physical_len();
                let mut zset = ZSet::new();
                for idx in before_count..after_count {
                    let row: Vec<CellValue> = (0..col_count).map(|c| table.get(idx, c)).collect();
                    zset.insert(insert.table.clone(), row);
                }
                Ok(MutResult::Mutation(zset))
            }
            Statement::Delete(ref delete) => {
                let deleted = delete::execute_delete(self.tables_mut(), delete, &params)?;
                let mut zset = ZSet::new();
                for row in deleted {
                    zset.delete(delete.table.clone(), row);
                }
                Ok(MutResult::Mutation(zset))
            }
            Statement::Update(ref update) => {
                let pairs = update::execute_update(self.tables_mut(), update, &params)?;
                let mut zset = ZSet::new();
                for (old, new) in pairs {
                    zset.delete(update.table.clone(), old);
                    zset.insert(update.table.clone(), new);
                }
                Ok(MutResult::Mutation(zset))
            }
            Statement::CreateTable(ct) => {
                let schema = sql_engine::schema::resolve(&ct)
                    .map_err(|e| DbError::Parse(format!("{e:?}")))?;
                self.create_table(schema)?;
                Ok(MutResult::Ddl)
            }
        }
    }
}
