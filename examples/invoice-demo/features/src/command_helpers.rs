//! Helpers used by `Command::execute_optimistic` (client-side, in-memory
//! `Database`). Always compiles — both wasm and native server builds use
//! these to keep the optimistic path consistent across targets.
//!
//! All helpers hang off [`SqlStmtExt`] as fluent methods on the `sql!` return
//! value: `sql!("...").execute(db)?` for mutations,
//! `sql!("...").read_*_col(db)?` for first-column projections.

use database::{Database, MutResult};
use sql_engine::execute::{ParamValue, Params};
use sql_engine::storage::{CellValue, Uuid};
use sqlbuilder::{SqlStmt, Value};
use sync::command::CommandError;
use sync::zset::ZSet;

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

fn render(stmt: SqlStmt) -> Result<(String, Params), CommandError> {
    let r = stmt
        .render()
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    let params: Params = r
        .params
        .into_iter()
        .map(|(k, v)| (k, to_param_value(v)))
        .collect();
    Ok((r.sql, params))
}

/// Fluent runners on the `sql!` return value.
///
/// `sql!("...").execute(db)?` runs a mutation; the `read_*_col` helpers
/// project the first column of the result into a typed `Vec`, skipping
/// cells of the wrong shape.
pub trait SqlStmtExt {
    fn execute(self, db: &mut Database) -> Result<ZSet, CommandError>;
    fn read_uuid_col(self, db: &mut Database) -> Result<Vec<Uuid>, CommandError>;
    fn read_i64_col(self, db: &mut Database) -> Result<Vec<i64>, CommandError>;
    fn read_str_col(self, db: &mut Database) -> Result<Vec<String>, CommandError>;
}

impl SqlStmtExt for SqlStmt {
    fn execute(self, db: &mut Database) -> Result<ZSet, CommandError> {
        let (sql, params) = render(self)?;
        match db.execute_mut_with_params(&sql, params) {
            Ok(MutResult::Mutation(z)) => Ok(z),
            Ok(MutResult::Rows(_)) | Ok(MutResult::Ddl) => Ok(ZSet::new()),
            Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
        }
    }

    fn read_uuid_col(self, db: &mut Database) -> Result<Vec<Uuid>, CommandError> {
        let (sql, params) = render(self)?;
        let cols = db
            .execute_with_params(&sql, params)
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

    fn read_i64_col(self, db: &mut Database) -> Result<Vec<i64>, CommandError> {
        let (sql, params) = render(self)?;
        let cols = db
            .execute_with_params(&sql, params)
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

    fn read_str_col(self, db: &mut Database) -> Result<Vec<String>, CommandError> {
        let (sql, params) = render(self)?;
        let cols = db
            .execute_with_params(&sql, params)
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
}
