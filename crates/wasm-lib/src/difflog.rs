use std::collections::HashMap;
use js_sys::Function;
use crate::projection::Projection;
use crate::query::{ProjectionConfig, ResolvedQuery, Row};

pub struct TableDef {
    pub name: String,
    pub field_ids: Vec<u16>,
}

pub struct DiffLog {
    db: Vec<HashMap<String, Row>>,  // table_id → (row_id → Row)
    projections: HashMap<u32, Projection>,
    next_projection_id: u32,
    pub tables: Vec<TableDef>,
    pub field_names: Vec<String>,
    field_ids: HashMap<String, u16>,
    table_name_to_id: HashMap<String, u16>,
}

impl DiffLog {
    pub fn new() -> Self {
        let mut s = Self {
            db: Vec::new(),
            projections: HashMap::new(),
            next_projection_id: 0,
            tables: Vec::new(),
            field_names: Vec::new(),
            field_ids: HashMap::new(),
            table_name_to_id: HashMap::new(),
        };
        s.intern_field("_table"); // FIELD_TABLE = 0
        s.intern_field("_id");    // FIELD_ID = 1
        s
    }

    fn intern_field(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.field_ids.get(name) {
            return id;
        }
        let id = self.field_names.len() as u16;
        self.field_names.push(name.to_string());
        self.field_ids.insert(name.to_string(), id);
        id
    }

    pub fn register_table(&mut self, name: String, fields: Vec<String>) -> u16 {
        let field_ids: Vec<u16> = fields.iter().map(|f| self.intern_field(f)).collect();
        let id = self.tables.len() as u16;
        self.table_name_to_id.insert(name.clone(), id);
        self.tables.push(TableDef { name, field_ids });
        self.db.push(HashMap::new());
        id
    }

    pub fn set_row(&mut self, table_id: u16, id: String, new_row: Row) {
        if self.projections.is_empty() {
            self.db[table_id as usize].insert(id, new_row);
        } else {
            let old_row = self.db[table_id as usize]
                .insert(id.clone(), new_row)
                .unwrap_or_default();
            let new_row_ref = &self.db[table_id as usize][&id];
            for proj in self.projections.values_mut() {
                proj.evaluate(table_id, &id, &old_row, new_row_ref);
            }
        }
    }

    pub fn register_projection(&mut self, config: ProjectionConfig, callback: Function) -> u32 {
        let id = self.next_projection_id;
        self.next_projection_id += 1;

        let resolved_query = ResolvedQuery::resolve(&config.query, &self.field_ids, &self.table_name_to_id);
        let resolved_fields = config.fields.map(|fields| {
            fields.iter()
                .map(|f| self.field_ids.get(f.as_str()).copied().unwrap_or(u16::MAX))
                .collect::<Vec<_>>()
        });
        let field_names_snapshot = self.field_names.clone();

        let mut proj = Projection::new(resolved_query, resolved_fields, field_names_snapshot, callback);

        for (table_idx, rows) in self.db.iter().enumerate() {
            for (row_id, row) in rows {
                let empty = Row::default();
                proj.evaluate(table_idx as u16, row_id, &empty, row);
            }
        }

        self.projections.insert(id, proj);
        id
    }

    pub fn unregister_projection(&mut self, id: u32) {
        self.projections.remove(&id);
    }

    pub fn reset(&mut self) {
        for table_rows in &mut self.db {
            table_rows.clear();
        }
        self.projections.clear();
        self.next_projection_id = 0;
    }

    pub fn flush_projections(&mut self) {
        for proj in self.projections.values_mut() {
            proj.flush();
        }
    }
}
