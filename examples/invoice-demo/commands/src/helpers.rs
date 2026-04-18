use database::{Database, MutResult};
use sql_engine::execute::{Params, ParamValue};
use sync::command::CommandError;
use sync::zset::ZSet;

pub fn execute_sql(db: &mut Database, sql: &str, params: Params) -> Result<ZSet, CommandError> {
    match db.execute_mut_with_params(sql, params) {
        Ok(MutResult::Mutation(zset)) => Ok(zset),
        Ok(_) => Ok(ZSet::new()),
        Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
    }
}

pub fn p_int(k: &str, v: i64) -> (String, ParamValue) {
    (k.into(), ParamValue::Int(v))
}

pub fn p_str(k: &str, v: &str) -> (String, ParamValue) {
    (k.into(), ParamValue::Text(v.to_string()))
}

pub fn read_i64_col(db: &mut Database, sql: &str, params: Params) -> Result<Vec<i64>, CommandError> {
    let cols = db.execute_with_params(sql, params)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    Ok(cols.into_iter().next().map(|col| {
        col.into_iter().filter_map(|v| match v {
            sql_engine::storage::CellValue::I64(n) => Some(n),
            _ => None,
        }).collect()
    }).unwrap_or_default())
}

pub fn read_str_col(db: &mut Database, sql: &str, params: Params) -> Result<Vec<String>, CommandError> {
    let cols = db.execute_with_params(sql, params)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    Ok(cols.into_iter().next().map(|col| {
        col.into_iter().map(|v| match v {
            sql_engine::storage::CellValue::Str(s) => s,
            _ => String::new(),
        }).collect()
    }).unwrap_or_default())
}
