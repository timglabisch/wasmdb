use std::collections::HashMap;

use sql_engine::schema::{DataType, TableSchema};
use sql_engine::storage::{CellValue, ZSet};
use sqlx::mysql::MySqlArguments;
use sqlx::query::Query;
use sqlx::{MySql, Transaction};
use sync::command::CommandError;

/// Replay a `ZSet` into a sqlx transaction. Positive weights become
/// `INSERT`s, negative weights become `DELETE`s keyed on the schema's
/// primary key.
///
/// Rows are inserted in full-column order (matching `schema.columns`);
/// deletes reuse the same row layout to pull PK values by column index.
pub async fn apply_zset(
    tx: &mut Transaction<'static, MySql>,
    zset: &ZSet,
    schemas: &HashMap<String, TableSchema>,
) -> Result<(), CommandError> {
    for entry in &zset.entries {
        let schema = schemas.get(&entry.table).ok_or_else(|| {
            CommandError::ExecutionFailed(format!("unknown table `{}`", entry.table))
        })?;

        if entry.weight > 0 {
            apply_insert(tx, schema, &entry.row).await?;
        } else if entry.weight < 0 {
            apply_delete(tx, schema, &entry.row).await?;
        }
    }
    Ok(())
}

async fn apply_insert(
    tx: &mut Transaction<'static, MySql>,
    schema: &TableSchema,
    row: &[CellValue],
) -> Result<(), CommandError> {
    if row.len() != schema.columns.len() {
        return Err(CommandError::ExecutionFailed(format!(
            "row length {} mismatches schema `{}` column count {}",
            row.len(),
            schema.name,
            schema.columns.len(),
        )));
    }

    let cols = schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; row.len()].join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        schema.name, cols, placeholders,
    );

    let mut q = sqlx::query::<MySql>(&sql);
    for (i, cell) in row.iter().enumerate() {
        q = bind_cell(q, cell, schema.columns[i].data_type);
    }

    q.execute(&mut **tx).await.map_err(|e| {
        CommandError::ExecutionFailed(format!(
            "INSERT into `{}` failed: {e}",
            schema.name,
        ))
    })?;
    Ok(())
}

async fn apply_delete(
    tx: &mut Transaction<'static, MySql>,
    schema: &TableSchema,
    row: &[CellValue],
) -> Result<(), CommandError> {
    if schema.primary_key.is_empty() {
        return Err(CommandError::ExecutionFailed(format!(
            "cannot DELETE from `{}`: schema has no primary key",
            schema.name,
        )));
    }
    if row.len() != schema.columns.len() {
        return Err(CommandError::ExecutionFailed(format!(
            "row length {} mismatches schema `{}` column count {}",
            row.len(),
            schema.name,
            schema.columns.len(),
        )));
    }

    let where_sql = schema
        .primary_key
        .iter()
        .map(|&i| format!("{} = ?", schema.columns[i].name))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("DELETE FROM {} WHERE {}", schema.name, where_sql);

    let mut q = sqlx::query::<MySql>(&sql);
    for &col_idx in &schema.primary_key {
        q = bind_cell(q, &row[col_idx], schema.columns[col_idx].data_type);
    }

    q.execute(&mut **tx).await.map_err(|e| {
        CommandError::ExecutionFailed(format!(
            "DELETE from `{}` failed: {e}",
            schema.name,
        ))
    })?;
    Ok(())
}

fn bind_cell<'q>(
    q: Query<'q, MySql, MySqlArguments>,
    cell: &CellValue,
    hint: DataType,
) -> Query<'q, MySql, MySqlArguments> {
    match cell {
        CellValue::I64(v) => q.bind(*v),
        CellValue::Str(s) => q.bind(s.clone()),
        CellValue::Null => match hint {
            DataType::I64 => q.bind(Option::<i64>::None),
            DataType::String => q.bind(Option::<String>::None),
        },
    }
}
