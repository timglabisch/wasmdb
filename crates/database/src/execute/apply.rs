use sql_engine::execute::ExecuteError;
use sql_engine::storage::{CellValue, ZSet};

use crate::Database;
use crate::error::DbError;

impl Database {
    /// Apply a ZSet to the database: PK-aware upsert for weight > 0,
    /// delete-by-row for weight < 0.
    ///
    /// Positive weights go through `upsert_by_pk` so re-applying a snapshot
    /// (e.g. a fetcher re-running after the requirement slot was dropped
    /// and recreated) is idempotent — the existing row with the same PK is
    /// replaced rather than duplicated. Tables without a primary key fall
    /// back to plain insert (no dedup possible).
    pub fn apply_zset(&mut self, zset: &ZSet) -> Result<(), DbError> {
        for entry in &zset.entries {
            if entry.weight > 0 {
                let t = self.tables.get_mut(&entry.table)
                    .ok_or_else(|| DbError::TableNotFound(entry.table.clone()))?;
                t.upsert_by_pk(&entry.row)
                    .map_err(|e| DbError::Execute(ExecuteError::TableNotFound(format!("{e}"))))?;
            } else if entry.weight < 0 {
                self.delete_by_row(&entry.table, &entry.row)?;
            }
        }
        Ok(())
    }

    /// Delete a row by matching all column values.
    fn delete_by_row(&mut self, table_name: &str, row: &[CellValue]) -> Result<(), DbError> {
        let table = self.table(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.into()))?;
        let col_count = row.len();
        let row_idx = table.row_ids()
            .find(|&r| (0..col_count).all(|c| table.get(r, c) == row[c]));
        if let Some(idx) = row_idx {
            let t = self.table_mut(table_name).unwrap();
            t.delete(idx).map_err(|e| DbError::Execute(ExecuteError::TableNotFound(format!("{e}"))))?;
        }
        Ok(())
    }
}
