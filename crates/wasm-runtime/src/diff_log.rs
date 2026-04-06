use std::collections::VecDeque;
use fnv::FnvHashMap;
use crate::query::Row;

#[inline]
pub fn tf_key(table_id: u16, field_id: u16) -> u32 {
    (table_id as u32) << 16 | field_id as u32
}

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
    start_id: u64,
    field_counts: FnvHashMap<u32, u32>,
}

impl DiffLog {
    pub fn new() -> Self {
        Self {
            diffs: VecDeque::new(),
            start_id: 0,
            field_counts: FnvHashMap::default(),
        }
    }

    fn increment(&mut self, table_id: u16, field_id: u16) {
        *self.field_counts.entry(tf_key(table_id, field_id)).or_default() += 1;
    }

    fn decrement(&mut self, table_id: u16, field_id: u16) {
        let key = tf_key(table_id, field_id);
        if let Some(count) = self.field_counts.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.field_counts.remove(&key);
            }
        }
    }

    fn rebuild_field_counts(&mut self) {
        self.field_counts.clear();
        for d in &self.diffs {
            *self.field_counts.entry(tf_key(d.table_id, d.field_id)).or_default() += 1;
        }
    }

    pub fn has_changes(&self, key: u32) -> bool {
        self.field_counts.contains_key(&key)
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
                self.increment(table_id, field_id);
            }
        }
        for (&field_id, new_val) in new {
            if old.get(&field_id) != Some(new_val) {
                self.diffs.push_back(DiffEntry {
                    tx_id, table_id, row_id: row_id.to_string(),
                    field_id, value: new_val.clone(), diff: 1,
                });
                self.increment(table_id, field_id);
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
        let entry = self.diffs.pop_back()?;
        self.decrement(entry.table_id, entry.field_id);
        Some(entry)
    }

    pub fn retain(&mut self, f: impl FnMut(&DiffEntry) -> bool) {
        self.diffs.retain(f);
        self.rebuild_field_counts();
    }

    pub fn clear(&mut self) {
        self.diffs.clear();
        self.field_counts.clear();
        self.start_id = 0;
    }
}
