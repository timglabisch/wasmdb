use std::collections::HashMap;
use js_sys::Function;
use crate::projection::Projection;
use crate::query::{ProjectionConfig, Row};

type Db = HashMap<String, HashMap<String, Row>>;

pub struct DiffLog {
    db: Db,
    projections: HashMap<u32, Projection>,
    next_projection_id: u32,
}

impl DiffLog {
    pub fn new() -> Self {
        Self {
            db: HashMap::new(),
            projections: HashMap::new(),
            next_projection_id: 0,
        }
    }

    pub fn set_row(&mut self, table: String, id: String, new_row: Row) {
        if self.projections.is_empty() {
            // Fast path: no projections, move everything, zero clones
            self.db.entry(table).or_default().insert(id, new_row);
        } else {
            let old_row = self.db
                .entry(table.clone())
                .or_default()
                .insert(id.clone(), new_row)
                .unwrap_or_default();
            let new_row_ref = &self.db[&table][&id];
            for proj in self.projections.values_mut() {
                proj.evaluate(&table, &id, &old_row, new_row_ref);
            }
        }
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

    pub fn reset(&mut self) {
        self.db.clear();
        self.projections.clear();
        self.next_projection_id = 0;
    }

    pub fn flush_projections(&mut self) {
        for proj in self.projections.values_mut() {
            proj.flush();
        }
    }
}
