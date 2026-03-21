use std::collections::VecDeque;
use crate::query::Row;

pub struct DiffEntry {
    pub tx_id: u64,
    pub table_id: u16,
    pub row_id: String,
    pub field_id: u16,
    pub value: String,
    pub diff: i8, // +1 = field has this value, -1 = field removed
}

pub struct DiffLog {
    diffs: VecDeque<DiffEntry>,
    start_id: u64, // global ID of diffs[0]; position of diff at index i = start_id + i
}

impl DiffLog {
    pub fn new() -> Self {
        Self {
            diffs: VecDeque::new(),
            start_id: 0,
        }
    }

    pub fn next_id(&self) -> u64 {
        self.start_id + self.diffs.len() as u64
    }

    /// Compare old vs new row, push field-level DiffEntries.
    pub fn emit_row_diffs(&mut self, tx_id: u64, table_id: u16, row_id: &str, old: &Row, new: &Row) {
        for (&field_id, old_val) in old {
            if new.get(&field_id) != Some(old_val) {
                self.diffs.push_back(DiffEntry {
                    tx_id, table_id, row_id: row_id.to_string(),
                    field_id, value: old_val.clone(), diff: -1,
                });
            }
        }
        for (&field_id, new_val) in new {
            if old.get(&field_id) != Some(new_val) {
                self.diffs.push_back(DiffEntry {
                    tx_id, table_id, row_id: row_id.to_string(),
                    field_id, value: new_val.clone(), diff: 1,
                });
            }
        }
    }

    pub fn since(&self, global_id: u64) -> impl Iterator<Item = &DiffEntry> {
        let offset = (global_id - self.start_id) as usize;
        self.diffs.iter().skip(offset)
    }

    pub fn iter(&self) -> impl Iterator<Item = &DiffEntry> {
        self.diffs.iter()
    }

    pub fn back(&self) -> Option<&DiffEntry> {
        self.diffs.back()
    }

    pub fn pop_back(&mut self) -> Option<DiffEntry> {
        self.diffs.pop_back()
    }

    pub fn retain(&mut self, f: impl FnMut(&DiffEntry) -> bool) {
        self.diffs.retain(f);
    }

    pub fn clear(&mut self) {
        self.diffs.clear();
        self.start_id = 0;
    }
}
