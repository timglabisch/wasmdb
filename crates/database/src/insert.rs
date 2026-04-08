use std::collections::HashMap;

use sql_engine::execute::{value_to_cell, ExecuteError};
use sql_engine::storage::{CellValue, Table};
use sql_parser::ast::{AstExpr, AstInsert};

use crate::error::DbError;

pub fn execute_insert(
    tables: &mut HashMap<String, Table>,
    insert: &AstInsert,
) -> Result<(), DbError> {
    let table = tables.get_mut(&insert.table)
        .ok_or_else(|| DbError::TableNotFound(insert.table.clone()))?;
    let schema = table.schema.clone();

    for value_row in &insert.values {
        let cells = if insert.columns.is_empty() {
            value_row.iter().map(eval_literal).collect::<Result<Vec<_>, _>>()?
        } else {
            let mut cells = vec![CellValue::Null; schema.columns.len()];
            for (i, col_name) in insert.columns.iter().enumerate() {
                let col_idx = schema.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| DbError::Parse(format!("unknown column: {col_name}")))?;
                cells[col_idx] = eval_literal(&value_row[i])?;
            }
            cells
        };
        table.insert(&cells)
            .map_err(|e| DbError::Execute(ExecuteError::TableNotFound(format!("{e}"))))?;
    }
    Ok(())
}

fn eval_literal(expr: &AstExpr) -> Result<CellValue, DbError> {
    match expr {
        AstExpr::Literal(v) => Ok(value_to_cell(v)),
        other => Err(DbError::Parse(format!("expected literal value, got {other:?}"))),
    }
}
