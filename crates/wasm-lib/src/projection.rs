use std::collections::HashMap;
use js_sys::Function;
use wasm_bindgen::JsValue;
use crate::diff::Diff;
use crate::query::{Query, Row};

pub struct Projection {
    query: Query,
    fields: Option<Vec<String>>,
    materialized: HashMap<String, Row>,
    diffs: Vec<Diff>,
    next_version: u32,
    callback: Function,
    last_synced_version: u32,
}

impl Projection {
    pub fn new(query: Query, fields: Option<Vec<String>>, callback: Function) -> Self {
        Self {
            query,
            fields,
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
                .filter_map(|f| full_row.get(f).map(|v| (f.clone(), v.clone())))
                .collect(),
        }
    }

    pub fn evaluate(&mut self, table: &str, id: &str, old_row: &Row, new_row: &Row) {
        let composite_id = format!("{}:{}", table, id);

        let mut old_full = old_row.clone();
        old_full.insert("_table".to_string(), table.to_string());
        old_full.insert("_id".to_string(), id.to_string());

        let mut new_full = new_row.clone();
        new_full.insert("_table".to_string(), table.to_string());
        new_full.insert("_id".to_string(), id.to_string());

        let was_in = !old_row.is_empty() && self.query.matches(&old_full);
        let is_in = !new_row.is_empty() && self.query.matches(&new_full);

        match (was_in, is_in) {
            (false, false) => {}
            (false, true) => {
                let projected = self.project(&new_full);
                for (key, value) in &projected {
                    self.append(&composite_id, key, value, 1);
                }
                self.materialized.insert(composite_id, projected);
            }
            (true, false) => {
                if let Some(old_projected) = self.materialized.remove(&composite_id) {
                    for (key, _) in &old_projected {
                        self.append(&composite_id, key, "", -1);
                    }
                }
            }
            (true, true) => {
                let new_projected = self.project(&new_full);
                let old_projected = self.materialized.get(&composite_id)
                    .cloned()
                    .unwrap_or_default();

                for (key, old_val) in &old_projected {
                    if new_projected.get(key) != Some(old_val) {
                        self.append(&composite_id, key, "", -1);
                    }
                }

                for (key, value) in &new_projected {
                    if old_projected.get(key) != Some(value) {
                        self.append(&composite_id, key, value, 1);
                    }
                }

                self.materialized.insert(composite_id, new_projected);
            }
        }
    }

    fn append(&mut self, id: &str, key: &str, value: &str, diff: i8) {
        self.diffs.push(Diff {
            version: self.next_version,
            table: String::new(),
            id: id.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            diff,
        });
        self.next_version += 1;
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
