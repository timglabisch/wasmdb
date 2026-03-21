use std::collections::HashMap;
use js_sys::Function;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use crate::query::{Query, ResolvedQuery, Row, FIELD_TABLE, FIELD_ID};

#[derive(Deserialize)]
pub struct ProjectionConfig {
    pub query: Query,
    pub fields: Option<Vec<String>>,
}

#[derive(Serialize, Clone)]
pub struct ProjectionChange {
    pub version: u32,
    pub id: String,
    pub key: String,
    pub value: String,
    pub diff: i8,
}

#[derive(Clone)]
pub enum ProjectionIndexKey {
    ExactId { table_id: u16, row_id: String },
    TableBroadcast { table_id: u16 },
    GlobalBroadcast,
}

impl ProjectionIndexKey {
    pub fn from_query(query: &ResolvedQuery) -> Self {
        let mut table_id: Option<u16> = None;
        let mut row_id: Option<String> = None;
        Self::collect_term_constraints(query, &mut table_id, &mut row_id);
        match (table_id, row_id) {
            (Some(tid), Some(rid)) => ProjectionIndexKey::ExactId { table_id: tid, row_id: rid },
            (Some(tid), None) => ProjectionIndexKey::TableBroadcast { table_id: tid },
            _ => ProjectionIndexKey::GlobalBroadcast,
        }
    }

    fn collect_term_constraints(query: &ResolvedQuery, table_id: &mut Option<u16>, row_id: &mut Option<String>) {
        match query {
            ResolvedQuery::Term(pairs) => {
                for &(fid, ref val) in pairs {
                    if fid == FIELD_TABLE {
                        *table_id = val.parse().ok();
                    }
                    if fid == FIELD_ID {
                        *row_id = Some(val.clone());
                    }
                }
            }
            ResolvedQuery::Bool { must, .. } => {
                for q in must {
                    Self::collect_term_constraints(q, table_id, row_id);
                }
            }
        }
    }
}

pub struct Projection {
    query: ResolvedQuery,
    fields: Option<Vec<u16>>,
    /// What was last sent to JS. Serves as diff basis.
    last_sent: HashMap<String, Row>,
    /// Temporary changes accumulated during sync(), flushed at end.
    pending_diffs: Vec<ProjectionChange>,
    callback: Function,
    pub(crate) index_key: ProjectionIndexKey,
    next_version: u32,
}

impl Projection {
    pub fn new(
        query: ResolvedQuery,
        fields: Option<Vec<u16>>,
        callback: Function,
        index_key: ProjectionIndexKey,
    ) -> Self {
        Self {
            query,
            fields,
            last_sent: HashMap::new(),
            pending_diffs: Vec::new(),
            callback,
            index_key,
            next_version: 1,
        }
    }

    fn project(&self, full_row: &Row) -> Row {
        match &self.fields {
            None => full_row.clone(),
            Some(fields) => fields.iter()
                .filter_map(|&fid| full_row.get(&fid).map(|v| (fid, v.clone())))
                .collect(),
        }
    }

    fn push_change(&mut self, composite_id: &str, field_names: &[String],
                    field_id: u16, value: String, sign: i8) {
        self.pending_diffs.push(ProjectionChange {
            version: self.next_version,
            id: composite_id.to_string(),
            key: field_names[field_id as usize].clone(),
            value,
            diff: sign,
        });
        self.next_version += 1;
    }

    /// Evaluate a single row change against this projection.
    /// Uses last_sent as the "old" state instead of requiring an old_row parameter.
    pub fn evaluate(&mut self, table_id: u16, id: &str, new_row: Option<&Row>, field_names: &[String]) {
        let composite_id = format!("{}:{}", table_id, id);
        let table_id_str = table_id.to_string();

        // Build full row (with meta-fields) once if new_row exists
        let new_full = new_row.map(|row| {
            let mut full = row.clone();
            full.insert(FIELD_TABLE, table_id_str);
            full.insert(FIELD_ID, id.to_string());
            full
        });

        let is_in = new_full.as_ref().map_or(false, |full| self.query.matches(full));
        let was_sent = self.last_sent.contains_key(&composite_id);

        match (was_sent, is_in) {
            (false, false) => {}
            (false, true) => {
                let projected = self.project(new_full.as_ref().unwrap());
                for (&field_id, value) in &projected {
                    self.push_change(&composite_id, field_names, field_id, value.clone(), 1);
                }
                self.last_sent.insert(composite_id, projected);
            }
            (true, false) => {
                if let Some(old_projected) = self.last_sent.remove(&composite_id) {
                    for (&field_id, _) in &old_projected {
                        self.push_change(&composite_id, field_names, field_id, String::new(), -1);
                    }
                }
            }
            (true, true) => {
                let new_projected = self.project(new_full.as_ref().unwrap());
                let old_projected = self.last_sent.get(&composite_id)
                    .cloned()
                    .unwrap_or_default();

                for (&field_id, old_val) in &old_projected {
                    if new_projected.get(&field_id) != Some(old_val) {
                        self.push_change(&composite_id, field_names, field_id, String::new(), -1);
                    }
                }
                for (&field_id, value) in &new_projected {
                    if old_projected.get(&field_id) != Some(value) {
                        self.push_change(&composite_id, field_names, field_id, value.clone(), 1);
                    }
                }

                self.last_sent.insert(composite_id, new_projected);
            }
        }
    }

    pub fn flush(&mut self) {
        if self.pending_diffs.is_empty() {
            return;
        }
        let js_diffs = serde_wasm_bindgen::to_value(&self.pending_diffs).unwrap();
        self.pending_diffs.clear();
        self.callback.call1(&JsValue::NULL, &js_diffs).ok();
    }
}

pub struct ProjectionIndex {
    id_index: HashMap<(u16, String), Vec<u32>>,
    table_broadcast: HashMap<u16, Vec<u32>>,
    global_broadcast: Vec<u32>,
}

impl ProjectionIndex {
    pub fn new() -> Self {
        Self {
            id_index: HashMap::new(),
            table_broadcast: HashMap::new(),
            global_broadcast: Vec::new(),
        }
    }

    pub fn add(&mut self, proj_id: u32, key: &ProjectionIndexKey) {
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

    pub fn remove(&mut self, proj_id: u32, key: &ProjectionIndexKey) {
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

    pub fn lookup(&self, table_id: u16, row_id: &str) -> Vec<u32> {
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
