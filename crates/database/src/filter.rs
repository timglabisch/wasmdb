use std::collections::HashMap;

use sql_engine::execute::filter_row::eval_predicate;
use sql_engine::execute::{Params, resolve_filter};
use sql_engine::planner::plan::{ColumnRef, PlanFilterPredicate, PlanSourceEntry, PlanScanMethod};
use sql_engine::planner::translate::plan_expr_to_predicate;
use sql_engine::planner::{PlanContext, PlanError};
use sql_engine::schema::TableSchema;
use sql_engine::storage::Table;
use sql_parser::ast::AstExpr;
use sql_parser::schema::{ColumnDef, Schema};

use crate::error::DbError;

pub fn build_predicate(
    table_name: &str,
    schema: &TableSchema,
    filter: &Option<AstExpr>,
    tables: &HashMap<String, Table>,
    params: &Params,
) -> Result<PlanFilterPredicate, DbError> {
    let filter_expr = match filter {
        Some(expr) => expr,
        None => return Ok(PlanFilterPredicate::None),
    };

    let query_schema = Schema::new(
        schema.columns.iter().map(|c| ColumnDef {
            table: Some(table_name.into()),
            name: c.name.clone(),
        }).collect()
    );

    let source = PlanSourceEntry {
        table: table_name.into(),
        schema: query_schema,
        join: None,
        pre_filter: PlanFilterPredicate::None,
        scan_method: PlanScanMethod::Full,
    };

    let table_schemas: HashMap<String, TableSchema> = tables.iter()
        .map(|(name, t)| (name.clone(), t.schema.clone()))
        .collect();

    let mut ctx = PlanContext {
        table_schemas: &table_schemas,
        query_schemas: HashMap::new(),
        materializations: Vec::new(),
    };

    let predicate = plan_expr_to_predicate(filter_expr, &[source], &mut ctx)
        .map_err(|e: PlanError| DbError::Parse(e.to_string()))?;

    if params.is_empty() {
        Ok(predicate)
    } else {
        resolve_filter(&predicate, params)
            .map_err(|e| DbError::Parse(e.to_string()))
    }
}

pub fn find_matching_rows(
    table: &Table,
    predicate: &PlanFilterPredicate,
) -> Vec<usize> {
    table.row_ids()
        .filter(|&row_idx| {
            eval_predicate(predicate, &|col: ColumnRef| table.get(row_idx, col.col))
        })
        .collect()
}
