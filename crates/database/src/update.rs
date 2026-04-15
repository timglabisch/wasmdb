use std::collections::HashMap;

use sql_engine::execute::value_to_cell;
use sql_engine::storage::{CellValue, Table};
use sql_parser::ast::{AstUpdate, AstExpr};

use crate::error::DbError;
use crate::filter;

/// Returns (old_row, new_row) pairs for each updated row.
pub fn execute_update(
    tables: &mut HashMap<String, Table>,
    update: &AstUpdate,
) -> Result<Vec<(Vec<CellValue>, Vec<CellValue>)>, DbError> {
    let table = tables.get(&update.table)
        .ok_or_else(|| DbError::TableNotFound(update.table.clone()))?;

    let col_count = table.schema.columns.len();

    // Resolve assignment column indices
    let assignment_cols: Vec<(usize, &AstExpr)> = update.assignments.iter()
        .map(|(col_name, expr)| {
            let col_idx = table.schema.columns.iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| DbError::Parse(format!("unknown column: {col_name}")))?;
            Ok((col_idx, expr))
        })
        .collect::<Result<Vec<_>, DbError>>()?;

    let predicate = filter::build_predicate(&update.table, &table.schema, &update.filter, tables)?;
    let matching = filter::find_matching_rows(table, &predicate);

    // Collect old rows and compute new rows
    let mut pairs = Vec::with_capacity(matching.len());
    for &row_idx in &matching {
        let old_row: Vec<CellValue> = (0..col_count).map(|c| table.get(row_idx, c)).collect();
        let mut new_row = old_row.clone();
        for &(col_idx, expr) in &assignment_cols {
            new_row[col_idx] = eval_set_expr(expr)?;
        }
        pairs.push((old_row, new_row));
    }

    // Apply: delete old, insert new
    let table = tables.get_mut(&update.table).unwrap();
    for &row_idx in &matching {
        table.delete(row_idx)
            .map_err(|e| DbError::Parse(format!("update delete failed: {e}")))?;
    }
    for (_, new_row) in &pairs {
        table.insert(new_row)
            .map_err(|e| DbError::Parse(format!("update insert failed: {e}")))?;
    }

    Ok(pairs)
}

fn eval_set_expr(expr: &AstExpr) -> Result<CellValue, DbError> {
    match expr {
        AstExpr::Literal(v) => Ok(value_to_cell(v)),
        _ => Err(DbError::Parse(format!("unsupported SET expression: {expr:?}"))),
    }
}
