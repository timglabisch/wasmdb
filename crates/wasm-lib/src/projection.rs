use std::collections::HashMap;
use js_sys::Function;
use wasm_bindgen::JsValue;
use crate::diff::Diff;
use crate::query::{ResolvedQuery, Row, FIELD_TABLE, FIELD_ID};

pub struct Projection {
    query: ResolvedQuery,
    fields: Option<Vec<u16>>,
    field_names: Vec<String>,           // global field_id → name snapshot
    materialized: HashMap<String, Row>,
    diffs: Vec<Diff>,
    next_version: u32,
    callback: Function,
    last_synced_version: u32,
}

impl Projection {
    pub fn new(
        query: ResolvedQuery,
        fields: Option<Vec<u16>>,
        field_names: Vec<String>,
        callback: Function,
    ) -> Self {
        Self {
            query,
            fields,
            field_names,
            materialized: HashMap::new(),
            diffs: Vec::new(),
            next_version: 1,
            callback,
            last_synced_version: 1,
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

    pub fn evaluate(&mut self, table_id: u16, id: &str, old_row: &Row, new_row: &Row) {
        let composite_id = format!("{}:{}", table_id, id);
        let table_id_str = table_id.to_string();

        let mut old_full = old_row.clone();
        old_full.insert(FIELD_TABLE, table_id_str.clone());
        old_full.insert(FIELD_ID, id.to_string());

        let mut new_full = new_row.clone();
        new_full.insert(FIELD_TABLE, table_id_str);
        new_full.insert(FIELD_ID, id.to_string());

        let was_in = !old_row.is_empty() && self.query.matches(&old_full);
        let is_in = !new_row.is_empty() && self.query.matches(&new_full);

        match (was_in, is_in) {
            (false, false) => {}
            (false, true) => {
                let projected = self.project(&new_full);
                for (&field_id, value) in &projected {
                    self.diffs.push(Diff {
                        version: self.next_version,
                        table: String::new(),
                        id: composite_id.clone(),
                        key: self.field_names[field_id as usize].clone(),
                        value: value.clone(),
                        diff: 1,
                    });
                    self.next_version += 1;
                }
                self.materialized.insert(composite_id, projected);
            }
            (true, false) => {
                if let Some(old_projected) = self.materialized.remove(&composite_id) {
                    for (&field_id, _) in &old_projected {
                        self.diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: self.field_names[field_id as usize].clone(),
                            value: String::new(),
                            diff: -1,
                        });
                        self.next_version += 1;
                    }
                }
            }
            (true, true) => {
                let new_projected = self.project(&new_full);
                let old_projected = self.materialized.get(&composite_id)
                    .cloned()
                    .unwrap_or_default();

                for (&field_id, old_val) in &old_projected {
                    if new_projected.get(&field_id) != Some(old_val) {
                        self.diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: self.field_names[field_id as usize].clone(),
                            value: String::new(),
                            diff: -1,
                        });
                        self.next_version += 1;
                    }
                }

                for (&field_id, value) in &new_projected {
                    if old_projected.get(&field_id) != Some(value) {
                        self.diffs.push(Diff {
                            version: self.next_version,
                            table: String::new(),
                            id: composite_id.clone(),
                            key: self.field_names[field_id as usize].clone(),
                            value: value.clone(),
                            diff: 1,
                        });
                        self.next_version += 1;
                    }
                }

                self.materialized.insert(composite_id, new_projected);
            }
        }
    }

    fn since(&self, version: u32) -> &[Diff] {
        match self.diffs.iter().position(|d| d.version >= version) {
            Some(pos) => &self.diffs[pos..],
            None => &[],
        }
    }

    pub fn flush(&mut self) {
        let js_diffs = {
            let diffs = self.since(self.last_synced_version);
            if diffs.is_empty() { return; }
            serde_wasm_bindgen::to_value(diffs).unwrap()
        };
        self.last_synced_version = self.next_version;
        self.callback.call1(&JsValue::NULL, &js_diffs).ok();
    }
}
