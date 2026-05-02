//! Inherent runners on [`SqlStmt`] that drive the in-repo [`Database`].
//!
//! Gated by the `sql-engine` feature so pure DSL consumers don't pull in
//! `database` / `sync`.

use database::{Database, MutResult};
use sql_engine::execute::{ParamValue, Params};
use sql_engine::storage::{CellValue, Uuid};
use sync::command::CommandError;
use sync::zset::ZSet;

use crate::from_row::FromRow;
use crate::{SqlStmt, Value};

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

impl SqlStmt {
    /// Run a mutation through the optimistic [`Database`]. Non-mutations
    /// produce an empty delta.
    pub fn execute(self, db: &mut Database) -> Result<ZSet, CommandError> {
        let (sql, params) = render(self)?;
        match db.execute_mut_with_params(&sql, params) {
            Ok(MutResult::Mutation(z)) => Ok(z),
            Ok(MutResult::Rows(_)) | Ok(MutResult::Ddl) => Ok(ZSet::new()),
            Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
        }
    }

    /// Project the first result column as `Vec<Uuid>`, skipping cells of the
    /// wrong shape.
    pub fn read_uuid_col(self, db: &mut Database) -> Result<Vec<Uuid>, CommandError> {
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

    /// Project the first result column as `Vec<i64>`, skipping cells of the
    /// wrong shape.
    pub fn read_i64_col(self, db: &mut Database) -> Result<Vec<i64>, CommandError> {
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

    /// Project the first result column as `Vec<String>`. Non-text cells map
    /// to `String::new()`.
    pub fn read_str_col(self, db: &mut Database) -> Result<Vec<String>, CommandError> {
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

    /// Read all result rows as raw `Vec<CellValue>` (column-major output of
    /// the engine transposed to row-major).
    pub fn read_rows_untyped(
        self,
        db: &mut Database,
    ) -> Result<Vec<Vec<CellValue>>, CommandError> {
        let (sql, params) = render(self)?;
        let cols = db
            .execute_with_params(&sql, params)
            .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
        let n_cols = cols.len();
        let n_rows = cols.first().map(|c| c.len()).unwrap_or(0);
        let mut rows: Vec<Vec<CellValue>> =
            (0..n_rows).map(|_| Vec::with_capacity(n_cols)).collect();
        for col in cols {
            for (i, cell) in col.into_iter().enumerate() {
                if let Some(row) = rows.get_mut(i) {
                    row.push(cell);
                }
            }
        }
        Ok(rows)
    }

    /// Read at most one result row as raw `Vec<CellValue>`. Extra rows are
    /// silently dropped — caller decides whether that's an error.
    pub fn read_row_untyped(
        self,
        db: &mut Database,
    ) -> Result<Option<Vec<CellValue>>, CommandError> {
        Ok(self.read_rows_untyped(db)?.into_iter().next())
    }

    /// Read all result rows into `R`. Each row is mapped positionally —
    /// `SELECT a, b, c` lines up with the first three fields of `R`.
    pub fn read_rows<R: FromRow>(self, db: &mut Database) -> Result<Vec<R>, CommandError> {
        self.read_rows_untyped(db)?
            .into_iter()
            .map(R::from_row)
            .collect()
    }

    /// Read at most one row into `R`. Returns `None` if the result is empty.
    pub fn read_row<R: FromRow>(self, db: &mut Database) -> Result<Option<R>, CommandError> {
        match self.read_row_untyped(db)? {
            Some(row) => R::from_row(row).map(Some),
            None => Ok(None),
        }
    }
}
