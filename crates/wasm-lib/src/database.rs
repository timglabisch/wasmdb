use std::collections::HashMap;
use js_sys::Function;
use crate::buffer::{read_u16_le, read_str};
use crate::schema::Schema;
use crate::diff_log::DiffLog;
use crate::view::MaterializedView;
use crate::projection::{Projection, ProjectionConfig, ProjectionIndex, ProjectionIndexKey};
use crate::query::{new_row, ResolvedQuery, Row};

pub struct Database {
    // Komponenten
    pub schema: Schema,
    pub diff_log: DiffLog,
    current: MaterializedView,

    // TX State
    current_tx: u64,
    in_transaction: bool,

    // Projection sync
    last_synced_id: u64,
    pending_reeval: Vec<(u16, String)>,

    // Projections
    projections: HashMap<u32, Projection>,
    next_projection_id: u32,
    pending_init: Vec<u32>,
    index: ProjectionIndex,
}

impl Database {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(),
            diff_log: DiffLog::new(),
            current: MaterializedView::new(),
            current_tx: 0,
            in_transaction: false,
            last_synced_id: 0,
            pending_reeval: Vec::new(),
            projections: HashMap::new(),
            next_projection_id: 0,
            pending_init: Vec::new(),
            index: ProjectionIndex::new(),
        }
    }

    pub fn register_table(&mut self, name: String, fields: Vec<String>) -> u16 {
        let id = self.schema.register_table(name, fields);
        self.current.add_table();
        id
    }

    pub fn set_row(&mut self, table_id: u16, id: String, new_row: Row) {
        let tx_id = self.current_tx;
        let old_row = self.current.tables[table_id as usize]
            .get(&id).cloned().unwrap_or_default();
        self.diff_log.emit_row_diffs(tx_id, table_id, &id, &old_row, &new_row);
        self.current.tables[table_id as usize].insert(id, new_row);
        if !self.in_transaction {
            self.current_tx += 1;
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

    pub fn register_projection(&mut self, config: ProjectionConfig, callback: Function) -> u32 {
        let id = self.next_projection_id;
        self.next_projection_id += 1;

        let resolved_query = ResolvedQuery::resolve(&config.query, &self.schema.field_ids, &self.schema.table_name_to_id);
        let resolved_fields = config.fields.map(|fields| {
            fields.iter()
                .map(|f| self.schema.field_ids.get(f.as_str()).copied().unwrap_or(u16::MAX))
                .collect::<Vec<_>>()
        });
        let index_key = ProjectionIndexKey::from_query(&resolved_query);

        let proj = Projection::new(resolved_query, resolved_fields, callback, index_key.clone());

        self.index.add(id, &index_key);
        self.projections.insert(id, proj);
        self.pending_init.push(id);
        id
    }

    pub fn unregister_projection(&mut self, id: u32) {
        if let Some(proj) = self.projections.remove(&id) {
            self.index.remove(id, &proj.index_key);
        }
    }

    pub fn reset(&mut self) {
        self.diff_log.clear();
        for t in &mut self.current.tables { t.clear(); }
        self.current_tx = 0;
        self.in_transaction = false;
        self.last_synced_id = 0;
        self.pending_reeval.clear();
        self.projections.clear();
        self.next_projection_id = 0;
        self.pending_init.clear();
        self.index = ProjectionIndex::new();
    }

    pub fn sync(&mut self) {
        // 1. Init pending projections
        let pending = std::mem::take(&mut self.pending_init);
        Self::init_pending(&self.current.tables, &self.schema.field_names, &mut self.projections, pending);

        // 2. Changed keys since last sync + pending reeval
        let mut changed_keys: Vec<(u16, String)> = self.diff_log.since(self.last_synced_id)
            .map(|d| (d.table_id, d.row_id.clone()))
            .collect();
        changed_keys.append(&mut self.pending_reeval);
        changed_keys.sort();
        changed_keys.dedup();

        // 3. Evaluate
        if !changed_keys.is_empty() {
            Self::evaluate_changes(&self.current.tables, &self.schema.field_names,
                                   &self.index, &mut self.projections, &changed_keys);
        }

        // 4. Flush to JS
        for proj in self.projections.values_mut() {
            proj.flush();
        }

        // 5. Advance sync position
        self.last_synced_id = self.diff_log.next_id();
    }

    fn init_pending(
        current: &[HashMap<String, Row>],
        field_names: &[String],
        projections: &mut HashMap<u32, Projection>,
        pending_ids: Vec<u32>,
    ) {
        for proj_id in pending_ids {
            let proj = match projections.get_mut(&proj_id) {
                Some(p) => p,
                None => continue,
            };

            let index_key = proj.index_key.clone();
            match &index_key {
                ProjectionIndexKey::ExactId { table_id, row_id } => {
                    if let Some(row) = current.get(*table_id as usize).and_then(|t| t.get(row_id)) {
                        proj.evaluate(*table_id, row_id, Some(row), field_names);
                    }
                }
                ProjectionIndexKey::TableBroadcast { table_id } => {
                    if let Some(table) = current.get(*table_id as usize) {
                        for (row_id, row) in table {
                            proj.evaluate(*table_id, row_id, Some(row), field_names);
                        }
                    }
                }
                ProjectionIndexKey::GlobalBroadcast => {
                    for (table_idx, table) in current.iter().enumerate() {
                        for (row_id, row) in table {
                            proj.evaluate(table_idx as u16, row_id, Some(row), field_names);
                        }
                    }
                }
            }
        }
    }

    fn evaluate_changes(
        current: &[HashMap<String, Row>],
        field_names: &[String],
        index: &ProjectionIndex,
        projections: &mut HashMap<u32, Projection>,
        changed_keys: &[(u16, String)],
    ) {
        for (table_id, row_id) in changed_keys {
            let candidates = index.lookup(*table_id, row_id);
            let new_row = current.get(*table_id as usize).and_then(|t| t.get(row_id));

            for proj_id in candidates {
                if let Some(proj) = projections.get_mut(&proj_id) {
                    proj.evaluate(*table_id, row_id, new_row, field_names);
                }
            }
        }
    }

    pub fn begin_tx(&mut self) -> u64 {
        self.in_transaction = true;
        self.current_tx
    }

    pub fn revert_tx(&mut self, tx_id: u64) {
        let first_tx_diff = self.diff_log.iter().position(|d| d.tx_id == tx_id);
        let is_tail = first_tx_diff.map_or(true, |start| {
            self.diff_log.iter().skip(start).all(|d| d.tx_id == tx_id)
        });

        if is_tail {
            // Fast path: unapply from tail in reverse, O(diffs in TX)
            while self.diff_log.back().map_or(false, |d| d.tx_id == tx_id) {
                let diff = self.diff_log.pop_back().unwrap();
                self.current.unapply_diff(&diff);
                self.pending_reeval.push((diff.table_id, diff.row_id));
            }
        } else {
            // Slow path: diffs interleaved, must rebuild
            let affected: Vec<(u16, String)> = self.diff_log.iter()
                .filter(|d| d.tx_id == tx_id)
                .map(|d| (d.table_id, d.row_id.clone()))
                .collect();
            self.diff_log.retain(|d| d.tx_id != tx_id);
            self.current.rebuild_from(&self.diff_log);
            self.pending_reeval.extend(affected);
        }

        if self.in_transaction && self.current_tx == tx_id {
            self.in_transaction = false;
            self.current_tx += 1;
        }
    }
}
