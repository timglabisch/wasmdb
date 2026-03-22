use fnv::FnvHashMap;
use js_sys::Function;
use crate::query::Query;

pub struct Projection {
    pub id: u32,
    pub query: Query,
    pub fields: Option<Vec<u16>>,
    pub callback: Function,
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

    pub fn register(&mut self, query: Query, fields: Option<Vec<u16>>, callback: Function) -> u32 {
        let id = self.projection_counter;
        self.projection_counter += 1;
        self.projections.insert(id, Projection { id, query, fields, callback });
        id
    }

    pub fn unregister(&mut self, id: u32) {
        self.projections.remove(&id);
    }

    pub fn clear(&mut self) {
        self.projections.clear();
        self.projection_counter = 0;
    }
}
