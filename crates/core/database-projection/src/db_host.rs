//! [`ProjectionHost`] adapter over the real [`database::Database`].
//! Partition reads use a single-column index when one exists, otherwise a
//! scan; deltas go through `Database::apply_zset` (PK-upsert for +,
//! tolerant full-row delete for −).

use database::Database;
use sql_engine::storage::{CellValue, ZSet};

use crate::spec::{ProjectionHost, RowReader};

pub struct DatabaseHost<'a> {
    db: &'a mut Database,
}

impl<'a> DatabaseHost<'a> {
    pub fn new(db: &'a mut Database) -> Self {
        Self { db }
    }
}

impl RowReader for DatabaseHost<'_> {
    fn rows_for_partition(
        &self,
        table: &str,
        partition_column: usize,
        partition: &CellValue,
    ) -> Vec<Vec<CellValue>> {
        let Some(t) = self.db.table(table) else {
            return Vec::new();
        };
        let ncols = t.schema.columns.len();
        if partition_column >= ncols {
            return Vec::new();
        }
        let cells_of = |r: usize| (0..ncols).map(|c| t.get(r, c)).collect::<Vec<_>>();

        if let Some(idx) = t.index_for_column(partition_column) {
            let Some(ids) = idx.lookup_eq(std::slice::from_ref(partition)) else {
                return Vec::new();
            };
            return ids
                .iter()
                .filter(|&&r| !t.is_deleted(r))
                .map(|&r| cells_of(r))
                .collect();
        }

        t.row_ids()
            .filter(|&r| t.get(r, partition_column) == *partition)
            .map(cells_of)
            .collect()
    }

    fn all_rows(&self, table: &str) -> Vec<Vec<CellValue>> {
        let Some(t) = self.db.table(table) else {
            return Vec::new();
        };
        let ncols = t.schema.columns.len();
        t.row_ids()
            .map(|r| (0..ncols).map(|c| t.get(r, c)).collect())
            .collect()
    }

    fn rows_matching(&self, table: &str, keys: &[(usize, CellValue)]) -> Vec<Vec<CellValue>> {
        let Some(t) = self.db.table(table) else {
            return Vec::new();
        };
        let ncols = t.schema.columns.len();
        if keys.iter().any(|(col, _)| *col >= ncols) {
            return Vec::new();
        }
        let cells_of = |r: usize| (0..ncols).map(|c| t.get(r, c)).collect::<Vec<_>>();
        let matches = |r: usize| keys.iter().all(|(col, v)| t.get(r, *col) == *v);

        // Narrow via a single-column index on the first key when one
        // exists; the remaining keys stay as filters.
        if let Some(&(first_col, ref first_val)) = keys.first() {
            if let Some(idx) = t.index_for_column(first_col) {
                let Some(ids) = idx.lookup_eq(std::slice::from_ref(first_val)) else {
                    return Vec::new();
                };
                return ids
                    .iter()
                    .filter(|&&r| !t.is_deleted(r) && matches(r))
                    .map(|&r| cells_of(r))
                    .collect();
            }
        }

        t.row_ids().filter(|&r| matches(r)).map(cells_of).collect()
    }
}

impl ProjectionHost for DatabaseHost<'_> {
    fn apply_delta(&mut self, delta: &ZSet) -> Result<(), String> {
        self.db.apply_zset(delta).map_err(|e| e.to_string())
    }
}
