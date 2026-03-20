use std::collections::HashMap;
use js_sys::Function;
use crate::diff::Diff;
use crate::projection::Projection;
use crate::query::{ProjectionConfig, Row};

type Db = HashMap<String, HashMap<String, Row>>;

pub struct DiffLog {
    diffs: Vec<Diff>,
    next_version: u32,
    db: Db,
    projections: HashMap<u32, Projection>,
    next_projection_id: u32,
}

impl DiffLog {
    pub fn new() -> Self {
        Self {
            diffs: Vec::new(),
            next_version: 1,
            db: HashMap::new(),
            projections: HashMap::new(),
            next_projection_id: 0,
        }
    }

    pub fn append(&mut self, table: &str, id: &str, key: &str, value: &str, diff: i8) {
        self.diffs.push(Diff {
            version: self.next_version,
            table: table.to_string(),
            id: id.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            diff,
        });
        self.next_version += 1;

        if diff > 0 {
            self.db.entry(table.to_string())
                .or_default()
                .entry(id.to_string())
                .or_default()
                .insert(key.to_string(), value.to_string());
        } else {
            if let Some(t) = self.db.get_mut(table) {
                if let Some(row) = t.get_mut(id) {
                    row.remove(key);
                    if row.is_empty() { t.remove(id); }
                }
                if t.is_empty() { self.db.remove(table); }
            }
        }
    }

    pub fn since(&self, version: u32) -> &[Diff] {
        match self.diffs.iter().position(|d| d.version >= version) {
            Some(pos) => &self.diffs[pos..],
            None => &[],
        }
    }

    pub fn next_version(&self) -> u32 {
        self.next_version
    }

    pub fn get_row(&self, table: &str, id: &str) -> Option<&Row> {
        self.db.get(table).and_then(|t| t.get(id))
    }

    pub fn register_projection(&mut self, config: ProjectionConfig, callback: Function) -> u32 {
        let id = self.next_projection_id;
        self.next_projection_id += 1;

        let mut proj = Projection::new(config.query, config.fields, callback);

        for (table, rows) in &self.db {
            for (row_id, row) in rows {
                let empty = HashMap::new();
                proj.evaluate(table, row_id, &empty, row);
            }
        }

        self.projections.insert(id, proj);
        id
    }

    pub fn unregister_projection(&mut self, id: u32) {
        self.projections.remove(&id);
    }

    pub fn evaluate_projections(&mut self, table: &str, id: &str, old_row: &Row, new_row: &Row) {
        let ids: Vec<u32> = self.projections.keys().cloned().collect();
        for proj_id in &ids {
            if let Some(proj) = self.projections.get_mut(proj_id) {
                proj.evaluate(table, id, old_row, new_row);
            }
        }
    }

    pub fn flush_projections(&mut self) {
        let ids: Vec<u32> = self.projections.keys().cloned().collect();
        for proj_id in &ids {
            if let Some(proj) = self.projections.get_mut(proj_id) {
                proj.flush();
            }
        }
    }
}
