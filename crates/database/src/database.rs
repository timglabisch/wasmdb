use std::collections::HashMap;
use std::sync::Arc;

use sql_engine::execute::{ExecuteError, FetcherRuntime};
use sql_engine::schema::TableSchema;
use sql_engine::storage::{CellValue, Table};
use sql_engine::planner::requirement::RequirementRegistry;
use sql_engine::{Caller, CallerRegistry, DbCaller, DbTable};
use sql_parser::ast::Statement;

use crate::error::DbError;

#[derive(Clone)]
pub struct Database {
    pub(crate) tables: HashMap<String, Table>,
    /// Registered callers (planner meta + async fetcher). Separate from
    /// `tables` because it's config, not data — `replace_tables` keeps
    /// `callers` intact while swapping the row map.
    pub(crate) callers: CallerRegistry,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            callers: CallerRegistry::new(),
        }
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

    /// Requirement registry — planner input for caller type-checking
    /// and `PlanSource::Requirement` translation. Exposed so tests (and
    /// external tools) can re-run the planners against the live registry
    /// without executing.
    pub fn requirements(&self) -> &RequirementRegistry {
        &self.callers.requirements
    }

    /// Clone of the fetcher runtime. `FetcherRuntime` is
    /// `HashMap<String, Arc<...>>`, so clone is cheap — every entry is an
    /// `Arc`-bump. Callers that need to invoke fetchers without holding
    /// `&Database` over an `.await` use this to own the closures outright
    /// (see the wasm `query_async` split in `invoice-demo/wasm/src/api.rs`).
    pub fn fetchers(&self) -> FetcherRuntime {
        self.callers.fetchers.clone()
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

    pub(crate) fn tables_mut(&mut self) -> &mut HashMap<String, Table> {
        &mut self.tables
    }

    /// Replace only the row data (tables) with a clone of `other`'s tables.
    /// The caller registry stays intact — it's config, not data. Used by
    /// sync-client to rebuild optimistic state without losing callers.
    pub fn replace_tables(&mut self, other: &Database) {
        self.tables = other.tables.clone();
    }

    /// Register a caller (planner meta + async fetcher). After this, queries
    /// of the form `SELECT … FROM {caller.id_parts}(…)` type-check at plan
    /// time and resolve at Phase 0 of async execution.
    pub fn register_caller(&mut self, caller: Caller) {
        self.callers.insert(caller);
    }

    /// Register a typed row: builds the table from `T::schema()`. Sugar
    /// over `create_table` for the trait-based codegen path.
    pub fn register_table<T: DbTable>(&mut self) -> Result<(), DbError> {
        self.create_table(T::schema())
    }

    /// Register a typed caller. Wraps `Q::call` into the erased
    /// `AsyncFetcherFn` expected by `Caller`, clones `ctx` into the
    /// closure so every invocation gets an `Arc`-bump.
    pub fn register_caller_of<Q: DbCaller>(&mut self, ctx: Arc<Q::Ctx>) {
        let fetcher = Arc::new(move |args| Q::call(args, ctx.clone()));
        self.register_caller(Caller::new(Q::ID, Q::meta(), fetcher));
    }
}
