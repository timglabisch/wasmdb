use std::collections::{HashMap, VecDeque};
use js_sys::Function;
use crate::projection::{Projection, ProjectionIndexKey};
use crate::query::{ProjectionConfig, ResolvedQuery, Row};

pub struct TableDef {
    pub name: String,
    pub field_ids: Vec<u16>,
}

pub struct DiffEntry {
    pub tx_id: u64,
    pub table_id: u16,
    pub row_id: String,
    pub field_id: u16,
    pub value: String,
    pub diff: i8, // +1 = field has this value, -1 = field removed
}

pub struct MaterializedView {
    pub tables: Vec<HashMap<String, Row>>,
}

impl MaterializedView {
    fn new() -> Self {
        Self { tables: Vec::new() }
    }

    fn add_table(&mut self) {
        self.tables.push(HashMap::new());
    }

    fn apply_diff(&mut self, diff: &DiffEntry) {
        self.apply_diff_with_sign(diff, diff.diff);
    }

    fn unapply_diff(&mut self, diff: &DiffEntry) {
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
}

pub struct ProjectionIndex {
    id_index: HashMap<(u16, String), Vec<u32>>,
    table_broadcast: HashMap<u16, Vec<u32>>,
    global_broadcast: Vec<u32>,
}

impl ProjectionIndex {
    fn new() -> Self {
        Self {
            id_index: HashMap::new(),
            table_broadcast: HashMap::new(),
            global_broadcast: Vec::new(),
        }
    }

    fn add(&mut self, proj_id: u32, key: &ProjectionIndexKey) {
        match key {
            ProjectionIndexKey::ExactId { table_id, row_id } => {
                self.id_index.entry((*table_id, row_id.clone())).or_default().push(proj_id);
            }
            ProjectionIndexKey::TableBroadcast { table_id } => {
                self.table_broadcast.entry(*table_id).or_default().push(proj_id);
            }
            ProjectionIndexKey::GlobalBroadcast => {
                self.global_broadcast.push(proj_id);
            }
        }
    }

    fn remove(&mut self, proj_id: u32, key: &ProjectionIndexKey) {
        match key {
            ProjectionIndexKey::ExactId { table_id, row_id } => {
                if let Some(vec) = self.id_index.get_mut(&(*table_id, row_id.clone())) {
                    vec.retain(|&id| id != proj_id);
                    if vec.is_empty() {
                        self.id_index.remove(&(*table_id, row_id.clone()));
                    }
                }
            }
            ProjectionIndexKey::TableBroadcast { table_id } => {
                if let Some(vec) = self.table_broadcast.get_mut(table_id) {
                    vec.retain(|&id| id != proj_id);
                    if vec.is_empty() {
                        self.table_broadcast.remove(table_id);
                    }
                }
            }
            ProjectionIndexKey::GlobalBroadcast => {
                self.global_broadcast.retain(|&id| id != proj_id);
            }
        }
    }

    fn lookup(&self, table_id: u16, row_id: &str) -> Vec<u32> {
        let mut result = Vec::new();
        if let Some(ids) = self.id_index.get(&(table_id, row_id.to_string())) {
            result.extend(ids);
        }
        if let Some(ids) = self.table_broadcast.get(&table_id) {
            result.extend(ids);
        }
        result.extend(&self.global_broadcast);
        result
    }
}

pub struct DiffLog {
    // --- Diffs: Source of Truth ---
    diffs: VecDeque<DiffEntry>,
    start_id: u64, // global ID of diffs[0]; position of diff at index i = start_id + i

    // --- Materialized View (derived, eagerly maintained) ---
    current: MaterializedView,

    // --- Transaction State ---
    next_tx_id: u64,
    auto_tx: Option<u64>,

    // --- Sync Tracking ---
    last_synced_id: u64,
    pending_reeval: Vec<(u16, String)>,

    // --- Projections ---
    projections: HashMap<u32, Projection>,
    next_projection_id: u32,
    pending_init: Vec<u32>,

    // --- Index ---
    index: ProjectionIndex,

    // --- Schema ---
    pub tables: Vec<TableDef>,
    pub field_names: Vec<String>,
    field_ids: HashMap<String, u16>,
    table_name_to_id: HashMap<String, u16>,
}

impl DiffLog {
    pub fn new() -> Self {
        let mut s = Self {
            diffs: VecDeque::new(),
            start_id: 0,
            current: MaterializedView::new(),
            next_tx_id: 0,
            auto_tx: None,
            last_synced_id: 0,
            pending_reeval: Vec::new(),
            projections: HashMap::new(),
            next_projection_id: 0,
            pending_init: Vec::new(),
            index: ProjectionIndex::new(),
            tables: Vec::new(),
            field_names: Vec::new(),
            field_ids: HashMap::new(),
            table_name_to_id: HashMap::new(),
        };
        s.intern_field("_table"); // FIELD_TABLE = 0
        s.intern_field("_id");    // FIELD_ID = 1
        s
    }

    fn next_id(&self) -> u64 {
        self.start_id + self.diffs.len() as u64
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
        self.current.add_table();
        id
    }

    fn ensure_auto_tx(&mut self) -> u64 {
        if let Some(id) = self.auto_tx {
            id
        } else {
            let id = self.next_tx_id;
            self.next_tx_id += 1;
            self.auto_tx = Some(id);
            id
        }
    }

    /// Compare old vs new row, push field-level DiffEntries.
    fn emit_row_diffs(&mut self, tx_id: u64, table_id: u16, row_id: &str, old: &Row, new: &Row) {
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

    pub fn set_row(&mut self, table_id: u16, id: String, new_row: Row) {
        let tx_id = self.ensure_auto_tx();
        let old_row = self.current.tables[table_id as usize]
            .get(&id).cloned().unwrap_or_default();
        self.emit_row_diffs(tx_id, table_id, &id, &old_row, &new_row);
        self.current.tables[table_id as usize].insert(id, new_row);
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
        let index_key = resolved_query.extract_index_key();

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
        self.diffs.clear();
        self.start_id = 0;
        for t in &mut self.current.tables { t.clear(); }
        self.next_tx_id = 0;
        self.auto_tx = None;
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
        Self::init_pending(&self.current.tables, &self.field_names, &mut self.projections, pending);

        // 2. Changed keys since last sync + pending reeval
        let since = (self.last_synced_id - self.start_id) as usize;
        let mut changed_keys: Vec<(u16, String)> = self.diffs.iter()
            .skip(since)
            .map(|d| (d.table_id, d.row_id.clone()))
            .collect();
        changed_keys.append(&mut self.pending_reeval);
        changed_keys.sort();
        changed_keys.dedup();

        // 3. Evaluate
        if !changed_keys.is_empty() {
            Self::evaluate_changes(&self.current.tables, &self.field_names,
                                   &self.index, &mut self.projections, &changed_keys);
        }

        // 4. Flush to JS
        for proj in self.projections.values_mut() {
            proj.flush();
        }

        // 5. Advance sync position, consume auto-TX
        self.last_synced_id = self.next_id();
        self.auto_tx = None;
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

    fn rebuild_from_diffs(view: &mut MaterializedView, diffs: &VecDeque<DiffEntry>) {
        for t in &mut view.tables { t.clear(); }
        for diff in diffs {
            view.apply_diff(diff);
        }
    }

    pub fn begin_tx(&mut self) -> u64 {
        let id = self.next_tx_id;
        self.next_tx_id += 1;
        id
    }

    pub fn revert_tx(&mut self, tx_id: u64) {
        let first_tx_diff = self.diffs.iter().position(|d| d.tx_id == tx_id);
        let is_tail = first_tx_diff.map_or(true, |start| {
            self.diffs.iter().skip(start).all(|d| d.tx_id == tx_id)
        });

        if is_tail {
            // Fast path: unapply from tail in reverse, O(diffs in TX)
            while self.diffs.back().map_or(false, |d| d.tx_id == tx_id) {
                let diff = self.diffs.pop_back().unwrap();
                self.current.unapply_diff(&diff);
                self.pending_reeval.push((diff.table_id, diff.row_id));
            }
        } else {
            // Slow path: diffs interleaved, must rebuild
            let affected: Vec<(u16, String)> = self.diffs.iter()
                .filter(|d| d.tx_id == tx_id)
                .map(|d| (d.table_id, d.row_id.clone()))
                .collect();
            self.diffs.retain(|d| d.tx_id != tx_id);
            Self::rebuild_from_diffs(&mut self.current, &self.diffs);
            self.pending_reeval.extend(affected);
        }

        if self.auto_tx == Some(tx_id) { self.auto_tx = None; }
    }
}
