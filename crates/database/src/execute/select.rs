use std::collections::{HashMap, HashSet};

use sql_engine::execute::{
    self, Columns, ExecutionContext, Params, Span,
};
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
    Ok(planner::sql::plan(select, &table_schemas)?)
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
    let plan = plan_select(db, select)?;
    let mut ctx = ExecutionContext::with_params(&db.tables, params);
    ctx.triggered_conditions = triggered_conditions;
    let result = execute::execute_plan(&mut ctx, &plan)?;
    Ok((result, ctx.spans))
}
