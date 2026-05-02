use std::collections::HashMap;

use sql_engine::execute::Params;
use sql_engine::storage::{CellValue, Table};
use sql_parser::ast::AstDelete;

use crate::error::DbError;
use super::filter;

pub(crate) fn execute_delete(
    tables: &mut HashMap<String, Table>,
    delete: &AstDelete,
    params: &Params,
) -> Result<Vec<Vec<CellValue>>, DbError> {
    let table = tables.get(&delete.table)
        .ok_or_else(|| DbError::TableNotFound(delete.table.clone()))?;

    let col_count = table.schema.columns.len();
    let predicate = filter::build_predicate(&delete.table, &table.schema, &delete.filter, tables, params)?;
    let matching = filter::find_matching_rows(table, &predicate);

    let mut deleted_rows = Vec::with_capacity(matching.len());
    for &row_idx in &matching {
        let row: Vec<CellValue> = (0..col_count).map(|c| table.get(row_idx, c)).collect();
        deleted_rows.push(row);
    }

    let table = tables.get_mut(&delete.table).unwrap();
    for &row_idx in &matching {
        table.delete(row_idx)
            .map_err(|e| DbError::Parse(format!("delete failed: {e}")))?;
    }

    Ok(deleted_rows)
}
