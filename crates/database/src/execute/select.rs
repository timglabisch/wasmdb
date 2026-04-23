use std::collections::{HashMap, HashSet};

use sql_engine::execute::{
    self, apply_fetched, collect_fetch_plan, fetch_requirements, Columns, ExecutionContext,
    FetchedRequirements, FetcherRuntime, Params, Span,
};
use sql_engine::planner::requirement::plan_requirements;
use sql_engine::planner::sql::plan::ExecutionPlan;
use sql_engine::planner;
use sql_engine::schema::TableSchema;

use crate::Database;
use crate::error::DbError;

fn plan_select(
    db: &Database,
    select: &sql_parser::ast::AstSelect,
) -> Result<ExecutionPlan, DbError> {
    let table_schemas: HashMap<String, TableSchema> = db.tables.iter()
        .map(|(name, table)| (name.clone(), table.schema.clone()))
        .collect();
    Ok(planner::sql::plan(select, &table_schemas, &db.callers.requirements)?)
}

pub(crate) fn execute_select(
    db: &Database,
    select: &sql_parser::ast::AstSelect,
    params: Params,
) -> Result<Columns, DbError> {
    let (columns, _spans) = execute_select_traced(db, select, params, None)?;
    Ok(columns)
}

pub(crate) fn execute_select_traced(
    db: &Database,
    select: &sql_parser::ast::AstSelect,
    params: Params,
    triggered_conditions: Option<HashSet<usize>>,
) -> Result<(Columns, Vec<Span>), DbError> {
    if !plan_requirements(select)?.requirements.is_empty() {
        return Err(DbError::RequiresAsync);
    }
    let plan = plan_select(db, select)?;
    let mut ctx = ExecutionContext::with_params(&db.tables, params);
    ctx.triggered_conditions = triggered_conditions;
    let result = execute::execute_plan(&mut ctx, &plan)?;
    Ok((result, ctx.spans))
}

/// Async execute — runs Phase 0 (resolve fetchers) then Phase 1+ on the
/// populated tables. Use this whenever the SELECT may contain a
/// `schema.fn(...)` source.
pub(crate) async fn execute_select_async(
    db: &mut Database,
    select: &sql_parser::ast::AstSelect,
    params: Params,
) -> Result<Columns, DbError> {
    let plan = plan_select(db, select)?;
    // Disjoint field borrows: Phase 0 needs `&mut tables` (upsert) and
    // `&fetchers` (invoke closures) at the same time.
    let tables = &mut db.tables;
    let fetchers = &db.callers.fetchers;
    Ok(execute::execute_and_resolve_requirements(tables, &plan, params, fetchers).await?)
}

// ── Split async API: prepare → fetch → apply+execute ─────────────────────
//
// For callers that must drop every DB borrow across the fetcher `.await`
// (wasm client: parallel `query_async` with no re-entrance panic). The
// three phases match `sql_engine::execute::requirement`'s 0a/0b/0c:
//
//   1. `prepare_select`  — sync, `&Database`: parse + plan + `collect_fetch_plan`.
//   2. `fetch_for`       — async, no DB ref:     invoke fetchers in parallel.
//   3. `apply_and_execute_select` — sync, `&mut Database`: upsert + execute.

/// Handle returned by [`Database::prepare_select`]. Carries the already-
/// planned query plus the resolved caller invocations. Feed into
/// [`fetch_for`] (async, no DB access) and then into
/// [`Database::apply_and_execute_select`] (sync, `&mut`).
pub struct PreparedSelect {
    plan: ExecutionPlan,
    params: Params,
    invocations: Vec<sql_engine::execute::ResolvedInvocation>,
}

impl Database {
    /// Phase 0a — parse, plan, and pre-resolve caller invocations against
    /// `params + plan.bound_values`. Purely sync: holds only `&self`, so
    /// callers can drop the borrow before the fetcher `.await`.
    pub fn prepare_select(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<PreparedSelect, DbError> {
        let stmt = sql_parser::parser::parse_statement(sql)
            .map_err(|e| DbError::Parse(format!("{e:?}")))?;
        let select = match stmt {
            sql_parser::ast::Statement::Select(s) => s,
            _ => return Err(DbError::Parse(
                "prepare_select: expected SELECT statement".into(),
            )),
        };
        let plan = plan_select(self, &select)?;
        let invocations = collect_fetch_plan(&plan, &params)
            .map_err(DbError::Execute)?;
        Ok(PreparedSelect { plan, params, invocations })
    }

    /// Phase 0c + 1+ — upsert fetched rows, then run the plan. Consumes
    /// `prepared` and `fetched`; both came from the pair above.
    pub fn apply_and_execute_select(
        &mut self,
        prepared: PreparedSelect,
        fetched: FetchedRequirements,
    ) -> Result<Columns, DbError> {
        let requirements = apply_fetched(&mut self.tables, fetched)
            .map_err(DbError::Execute)?;
        let mut ctx = ExecutionContext::with_params(&self.tables, prepared.params);
        ctx.requirements = requirements;
        let cols = execute::execute_plan(&mut ctx, &prepared.plan)
            .map_err(DbError::Execute)?;
        Ok(cols)
    }
}

/// Phase 0b — await every fetcher in parallel. Takes the prepared handle and
/// a cloned [`FetcherRuntime`] (see [`Database::fetchers`]); no DB reference
/// is held, so the caller is free to release all locks while this runs.
pub async fn fetch_for(
    prepared: &PreparedSelect,
    fetchers: &FetcherRuntime,
) -> Result<FetchedRequirements, DbError> {
    fetch_requirements(&prepared.invocations, fetchers).await
        .map_err(DbError::Execute)
}
