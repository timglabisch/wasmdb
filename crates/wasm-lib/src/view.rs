use std::collections::HashMap;
use crate::diff_log::{DiffEntry, DiffLog};
use crate::query::Row;

pub struct MaterializedView {
    pub tables: Vec<HashMap<String, Row>>,
}

impl MaterializedView {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn add_table(&mut self) {
        self.tables.push(HashMap::new());
    }

    pub fn apply_diff(&mut self, diff: &DiffEntry) {
        self.apply_diff_with_sign(diff, diff.diff);
    }

    pub fn unapply_diff(&mut self, diff: &DiffEntry) {
        self.apply_diff_with_sign(diff, -diff.diff);
    }

    fn apply_diff_with_sign(&mut self, diff: &DiffEntry, sign: i8) {
        if sign > 0 {
            self.tables[diff.table_id as usize]
                .entry(diff.row_id.clone()).or_default()
                .insert(diff.field_id, diff.value.clone());
        } else {
            if let Some(row) = self.tables[diff.table_id as usize].get_mut(&diff.row_id) {
                row.remove(&diff.field_id);
                if row.is_empty() {
                    self.tables[diff.table_id as usize].remove(&diff.row_id);
                }
            }
        }
    }

    pub fn rebuild_from(&mut self, diff_log: &DiffLog) {
        for t in &mut self.tables { t.clear(); }
        for diff in diff_log.iter() {
            self.apply_diff(diff);
        }
    }
}
