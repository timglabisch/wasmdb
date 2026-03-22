use js_sys::Function;
use crate::buffer::{read_u16_le, read_str};
use crate::schema::Schema;
use crate::diff_log::DiffLog;
use crate::view::MaterializedView;
use crate::projection::ProjectionManager;
use crate::query::{new_row, ProjectionQuery, Query, Row};

pub struct Database {
    pub schema: Schema,
    pub diff_log: DiffLog,
    view: MaterializedView,

    // TX State
    tx_current: u64,
    tx_active: bool,

    projection_manager: ProjectionManager,
}

impl Database {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(),
            diff_log: DiffLog::new(),
            view: MaterializedView::new(),
            tx_current: 0,
            tx_active: false,
            projection_manager: ProjectionManager::new(),
        }
    }

    pub fn register_table(&mut self, name: String, fields: Vec<String>) -> u16 {
        let id = self.schema.register_table(name, fields);
        self.view.add_table();
        id
    }

    pub fn set_row(&mut self, table_id: u16, id: String, new_row: Row) {
        let tx_id = self.tx_current;
        let old_row = self.view.tables[table_id as usize]
            .get(&id).cloned().unwrap_or_default();
        self.diff_log.emit_row_diffs(tx_id, table_id, &id, &old_row, &new_row);
        self.view.tables[table_id as usize].insert(id, new_row);
        if !self.tx_active {
            self.tx_current += 1;
        }
    }

    pub fn process_buffer(&mut self, buf: &[u8], to: usize) {
        let mut pos = 8;
        while pos < to {
            if pos + 2 > to { break; }
            let table_id = read_u16_le(buf, pos);
            pos += 2;

            let id = match read_str(buf, &mut pos, to) { Some(s) => s, None => break };

            let field_ids = self.schema.tables[table_id as usize].field_ids.clone();

            let mut row = new_row(field_ids.len());
            for fi in 0..field_ids.len() {
                let value = match read_str(buf, &mut pos, to) { Some(s) => s, None => break };
                row.insert(field_ids[fi], value);
            }

            self.set_row(table_id, id, row);
        }
    }

    pub fn register_projection(&mut self, projection_query: ProjectionQuery, fields: Option<Vec<String>>, callback: Function) -> u32 {
        let query = Query::new(&projection_query, &self.schema.field_ids, &self.schema.table_name_to_id);
        let resolved_fields = fields.map(|fields| {
            fields.iter()
                .map(|f| self.schema.field_ids.get(f.as_str()).copied().unwrap_or(u16::MAX))
                .collect::<Vec<_>>()
        });
        self.projection_manager.register(query, resolved_fields, callback)
    }

    pub fn unregister_projection(&mut self, id: u32) {
        self.projection_manager.unregister(id);
    }

    pub fn reset(&mut self) {
        self.diff_log.clear();
        for t in &mut self.view.tables { t.clear(); }
        self.tx_current = 0;
        self.tx_active = false;
        self.projection_manager.clear();
    }

    pub fn sync(&mut self) {
        // TODO: projection evaluation
    }

    pub fn begin_tx(&mut self) -> u64 {
        self.tx_active = true;
        self.tx_current
    }

    pub fn revert_tx(&mut self, tx_id: u64) {
        let first_tx_diff = self.diff_log.iter().position(|d| d.tx_id == tx_id);
        let is_tail = first_tx_diff.map_or(true, |start| {
            self.diff_log.iter().skip(start).all(|d| d.tx_id == tx_id)
        });

        if is_tail {
            while self.diff_log.back().map_or(false, |d| d.tx_id == tx_id) {
                let diff = self.diff_log.pop_back().unwrap();
                self.view.unapply_diff(&diff);
            }
        } else {
            self.diff_log.retain(|d| d.tx_id != tx_id);
            self.view.rebuild_from(&self.diff_log);
        }

        if self.tx_active && self.tx_current == tx_id {
            self.tx_active = false;
            self.tx_current += 1;
        }
    }
}
