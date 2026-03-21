use std::collections::HashMap;
use js_sys::Function;
use wasm_bindgen::JsValue;
use crate::diff::Diff;
use crate::query::{ResolvedQuery, Row, FIELD_TABLE, FIELD_ID};

#[derive(Clone)]
pub enum ProjectionIndexKey {
    ExactId { table_id: u16, row_id: String },
    TableBroadcast { table_id: u16 },
    GlobalBroadcast,
}

pub struct Projection {
    pub query: ResolvedQuery,
    pub fields: Option<Vec<u16>>,
    /// What was last sent to JS. Serves as diff basis.
    pub last_sent: HashMap<String, Row>,
    /// Temporary diffs accumulated during sync(), flushed at end.
    pub pending_diffs: Vec<Diff>,
    pub callback: Function,
    pub index_key: ProjectionIndexKey,
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

    /// Evaluate a single row change against this projection.
    /// Uses last_sent as the "old" state instead of requiring an old_row parameter.
    pub fn evaluate(&mut self, table_id: u16, id: &str, new_row: Option<&Row>, field_names: &[String]) {
        let composite_id = format!("{}:{}", table_id, id);
        let table_id_str = table_id.to_string();

        let was_sent = self.last_sent.contains_key(&composite_id);

        let is_in = if let Some(row) = new_row {
            let mut full = row.clone();
            full.insert(FIELD_TABLE, table_id_str.clone());
            full.insert(FIELD_ID, id.to_string());
            self.query.matches(&full)
        } else {
            false
        };

        match (was_sent, is_in) {
            (false, false) => {}
            (false, true) => {
                let mut new_full = new_row.unwrap().clone();
                new_full.insert(FIELD_TABLE, table_id_str);
                new_full.insert(FIELD_ID, id.to_string());
                let projected = self.project(&new_full);
                for (&field_id, value) in &projected {
                    self.pending_diffs.push(Diff {
                        version: self.next_version,
                        table: String::new(),
                        id: composite_id.clone(),
                        key: field_names[field_id as usize].clone(),
                        value: value.clone(),
                        diff: 1,
                    });
                    self.next_version += 1;
                }
                self.last_sent.insert(composite_id, projected);
            }
            (true, false) => {
                if let Some(old_projected) = self.last_sent.remove(&composite_id) {
                    for (&field_id, _) in &old_projected {
                        self.pending_diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: field_names[field_id as usize].clone(),
                            value: String::new(),
                            diff: -1,
                        });
                        self.next_version += 1;
                    }
                }
            }
            (true, true) => {
                let mut new_full = new_row.unwrap().clone();
                new_full.insert(FIELD_TABLE, table_id_str);
                new_full.insert(FIELD_ID, id.to_string());
                let new_projected = self.project(&new_full);
                let old_projected = self.last_sent.get(&composite_id)
                    .cloned()
                    .unwrap_or_default();

                for (&field_id, old_val) in &old_projected {
                    if new_projected.get(&field_id) != Some(old_val) {
                        self.pending_diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: field_names[field_id as usize].clone(),
                            value: String::new(),
                            diff: -1,
                        });
                        self.next_version += 1;
                    }
                }

                for (&field_id, value) in &new_projected {
                    if old_projected.get(&field_id) != Some(value) {
                        self.pending_diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: field_names[field_id as usize].clone(),
                            value: value.clone(),
                            diff: 1,
                        });
                        self.next_version += 1;
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
