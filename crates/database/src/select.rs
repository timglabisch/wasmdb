use std::collections::HashMap;

use sql_engine::execute::{self, Columns, ExecutionContext, Params, Span};
use sql_engine::planner;
use sql_engine::storage::Table;

use crate::error::DbError;

pub fn execute_select(
    tables: &HashMap<String, Table>,
    select: &sql_parser::ast::AstSelect,
    params: Params,
) -> Result<Columns, DbError> {
    let table_schemas = tables.iter()
        .map(|(name, table)| (name.clone(), table.schema.clone()))
        .collect();
    let plan = planner::plan(select, &table_schemas)?;
    let mut ctx = ExecutionContext::with_params(tables, params);
    let result = execute::execute_plan(&mut ctx, &plan)?;
    Ok(result)
}

pub fn execute_select_traced(
    tables: &HashMap<String, Table>,
    select: &sql_parser::ast::AstSelect,
    params: Params,
) -> Result<(Columns, Vec<Span>), DbError> {
    let table_schemas = tables.iter()
        .map(|(name, table)| (name.clone(), table.schema.clone()))
        .collect();
    let plan = planner::plan(select, &table_schemas)?;
    let mut ctx = ExecutionContext::with_params(tables, params);
    let result = execute::execute_plan(&mut ctx, &plan)?;
    Ok((result, ctx.spans))
}
