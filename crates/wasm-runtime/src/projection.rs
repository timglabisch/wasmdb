use fnv::FnvHashMap;
use js_sys::Function;
use crate::diff_log::DiffLog;
use crate::query::Query;

pub struct Projection {
    pub id: u32,
    pub query: Query,
    pub fields: Option<Vec<u16>>,
    pub callback: Function,
    pub relevant_keys: Vec<u32>,
}

impl Projection {
    pub fn is_dirty(&self, diff_log: &DiffLog) -> bool {
        self.relevant_keys.iter().any(|k| diff_log.has_changes(*k))
    }
}

pub struct ProjectionManager {
    projections: FnvHashMap<u32, Projection>,
    projection_counter: u32,
}

impl ProjectionManager {
    pub fn new() -> Self {
        Self {
            projections: FnvHashMap::default(),
            projection_counter: 0,
        }
    }

    pub fn register(&mut self, query: Query, fields: Option<Vec<u16>>, callback: Function, relevant_keys: Vec<u32>) -> u32 {
        let id = self.projection_counter;
        self.projection_counter += 1;
        self.projections.insert(id, Projection { id, query, fields, callback, relevant_keys });
        id
    }

    pub fn unregister(&mut self, id: u32) {
        self.projections.remove(&id);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Projection> {
        self.projections.values()
    }

    pub fn clear(&mut self) {
        self.projections.clear();
        self.projection_counter = 0;
    }
}
