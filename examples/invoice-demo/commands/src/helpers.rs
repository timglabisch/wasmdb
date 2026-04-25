use database::{Database, MutResult};
use sql_engine::execute::{Params, ParamValue};
use sql_engine::storage::{CellValue, Uuid};
use sync::command::CommandError;
use sync::zset::ZSet;

/// Hardcoded tenant for the demo. Server-side TiDB queries bind this on every
/// INSERT/UPDATE/DELETE/SELECT. Client-side `Database` does not know about
/// tenants — the column lives in TiDB only.
pub const DEMO_TENANT_ID: i64 = 0;

/// Run a mutation and return the resulting `ZSet`. Non-mutations produce
/// an empty delta — helpers.rs stays sync because the client-side
/// `Database` is an in-memory store.
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
