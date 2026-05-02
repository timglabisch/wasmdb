use std::collections::HashMap;

use sql_engine::execute::{value_to_cell, resolve_value, ExecuteError, Params};
use sql_engine::storage::{CellValue, Table};
use sql_parser::ast::{AstExpr, AstInsert, Value};

use crate::error::DbError;

pub(crate) fn execute_insert(
    tables: &mut HashMap<String, Table>,
    insert: &AstInsert,
    params: &Params,
) -> Result<(), DbError> {
    let table = tables.get_mut(&insert.table)
        .ok_or_else(|| DbError::TableNotFound(insert.table.clone()))?;
    let schema = table.schema.clone();

    for value_row in &insert.values {
        let cells = if insert.columns.is_empty() {
            value_row.iter().map(|e| eval_literal(e, params)).collect::<Result<Vec<_>, _>>()?
        } else {
            let mut cells = vec![CellValue::Null; schema.columns.len()];
            for (i, col_name) in insert.columns.iter().enumerate() {
                let col_idx = schema.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| DbError::Parse(format!("unknown column: {col_name}")))?;
                cells[col_idx] = eval_literal(&value_row[i], params)?;
            }
            cells
        };
        table.insert(&cells)
            .map_err(|e| DbError::Execute(ExecuteError::TableNotFound(format!("{e}"))))?;
    }
    Ok(())
}

fn eval_literal(expr: &AstExpr, params: &Params) -> Result<CellValue, DbError> {
    match expr {
        AstExpr::Literal(Value::Placeholder(name)) => {
            let resolved = resolve_value(&Value::Placeholder(name.clone()), params)
                .map_err(|e| DbError::Parse(e.to_string()))?;
            Ok(value_to_cell(&resolved))
        }
        AstExpr::Literal(v) => Ok(value_to_cell(v)),
        other => Err(DbError::Parse(format!("expected literal value, got {other:?}"))),
    }
}
