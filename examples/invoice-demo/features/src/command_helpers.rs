//! Helpers used by `Command::execute_optimistic` (client-side, in-memory
//! `Database`). Always compiles — both wasm and native server builds use
//! these to keep the optimistic path consistent across targets.

use database::{Database, MutResult};
use sql_engine::execute::{Params, ParamValue};
use sql_engine::storage::{CellValue, Uuid};
use sqlbuilder::{SqlStmt, Value};
use sync::command::CommandError;
use sync::zset::ZSet;

/// Run a mutation and return the resulting `ZSet`. Non-mutations produce
/// an empty delta.
pub fn execute_sql(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Result<ZSet, CommandError> {
    match db.execute_mut_with_params(sql, params) {
        Ok(MutResult::Mutation(z)) => Ok(z),
        Ok(MutResult::Rows(_)) | Ok(MutResult::Ddl) => Ok(ZSet::new()),
        Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
    }
}

pub fn p_int(k: &str, v: i64) -> (String, ParamValue) {
    (k.into(), ParamValue::Int(v))
}

pub fn p_str(k: &str, v: &str) -> (String, ParamValue) {
    (k.into(), ParamValue::Text(v.to_string()))
}

pub fn p_uuid(k: &str, v: &Uuid) -> (String, ParamValue) {
    (k.into(), ParamValue::Uuid(v.0))
}

pub fn p_uuid_opt(k: &str, v: &Option<Uuid>) -> (String, ParamValue) {
    let pv = match v {
        Some(u) => ParamValue::Uuid(u.0),
        None => ParamValue::Null,
    };
    (k.into(), pv)
}

pub fn read_uuid_col(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Result<Vec<Uuid>, CommandError> {
    let cols = db
        .execute_with_params(sql, params)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    Ok(cols
        .into_iter()
        .next()
        .map(|col| {
            col.into_iter()
                .filter_map(|v| match v {
                    CellValue::Uuid(b) => Some(Uuid(b)),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default())
}

pub fn read_i64_col(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Result<Vec<i64>, CommandError> {
    let cols = db
        .execute_with_params(sql, params)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    Ok(cols
        .into_iter()
        .next()
        .map(|col| {
            col.into_iter()
                .filter_map(|v| match v {
                    CellValue::I64(n) => Some(n),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default())
}

// ── sqlbuilder integration ────────────────────────────────────────────────

fn to_param_value(v: Value) -> ParamValue {
    match v {
        Value::Int(n) => ParamValue::Int(n),
        Value::Text(s) => ParamValue::Text(s),
        Value::Uuid(b) => ParamValue::Uuid(b),
        Value::Null => ParamValue::Null,
        Value::IntList(xs) => ParamValue::IntList(xs),
        Value::TextList(xs) => ParamValue::TextList(xs),
        Value::UuidList(xs) => ParamValue::UuidList(xs),
    }
}

/// Render an [`SqlStmt`] and run it through the optimistic [`Database`].
pub fn execute_stmt(db: &mut Database, stmt: SqlStmt) -> Result<ZSet, CommandError> {
    let rendered = stmt
        .render()
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    let params: Params = rendered
        .params
        .into_iter()
        .map(|(k, v)| (k, to_param_value(v)))
        .collect();
    execute_sql(db, &rendered.sql, params)
}

pub fn read_str_col(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Result<Vec<String>, CommandError> {
    let cols = db
        .execute_with_params(sql, params)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    Ok(cols
        .into_iter()
        .next()
        .map(|col| {
            col.into_iter()
                .map(|v| match v {
                    CellValue::Str(s) => s,
                    _ => String::new(),
                })
                .collect()
        })
        .unwrap_or_default())
}
