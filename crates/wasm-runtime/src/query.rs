use std::collections::HashMap;
use fnv::FnvHashMap;
use serde::Deserialize;

pub type Row = FnvHashMap<u16, String>;

pub const FIELD_TABLE: u16 = 0;
pub const FIELD_ID: u16 = 1;

pub fn new_row(capacity: usize) -> Row {
    Row::with_capacity_and_hasher(capacity, Default::default())
}

// Deserialized from JS — string field names
#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionQuery {
    Term(HashMap<String, String>),
    Bool {
        #[serde(default)]
        must: Vec<ProjectionQuery>,
        #[serde(default)]
        must_not: Vec<ProjectionQuery>,
    },
}

// Field names resolved to u16 IDs
pub enum Query {
    Term(Vec<(u16, String)>),
    Bool {
        must: Vec<Query>,
        must_not: Vec<Query>,
    },
}

impl Query {
    pub fn new(
        projection_query: &ProjectionQuery,
        field_ids: &HashMap<String, u16>,
        table_name_to_id: &HashMap<String, u16>,
    ) -> Self {
        match projection_query {
            ProjectionQuery::Term(map) => Query::Term(
                map.iter()
                    .map(|(k, v)| {
                        let fid = field_ids.get(k).copied().unwrap_or(u16::MAX);
                        let val = if fid == FIELD_TABLE {
                            table_name_to_id.get(v)
                                .map(|tid| tid.to_string())
                                .unwrap_or_else(|| v.clone())
                        } else {
                            v.clone()
                        };
                        (fid, val)
                    })
                    .collect()
            ),
            ProjectionQuery::Bool { must, must_not } => Query::Bool {
                must: must.iter().map(|q| Self::new(q, field_ids, table_name_to_id)).collect(),
                must_not: must_not.iter().map(|q| Self::new(q, field_ids, table_name_to_id)).collect(),
            },
        }
    }

    pub fn extract_scope(&self) -> (Vec<u16>, Vec<u16>) {
        let mut table_ids = Vec::new();
        let mut field_ids = Vec::new();
        self.collect_scope(&mut table_ids, &mut field_ids);
        table_ids.sort_unstable();
        table_ids.dedup();
        field_ids.sort_unstable();
        field_ids.dedup();
        (table_ids, field_ids)
    }

    fn collect_scope(&self, table_ids: &mut Vec<u16>, field_ids: &mut Vec<u16>) {
        match self {
            Query::Term(pairs) => {
                for &(fid, ref val) in pairs {
                    if fid == FIELD_TABLE {
                        if let Ok(tid) = val.parse::<u16>() {
                            table_ids.push(tid);
                        }
                    } else if fid > FIELD_ID {
                        field_ids.push(fid);
                    }
                }
            }
            Query::Bool { must, must_not } => {
                for q in must.iter().chain(must_not.iter()) {
                    q.collect_scope(table_ids, field_ids);
                }
            }
        }
    }

    pub fn matches(&self, row: &Row) -> bool {
        match self {
            Query::Term(pairs) => pairs.iter().all(|(id, v)| row.get(id) == Some(v)),
            Query::Bool { must, must_not } => {
                must.iter().all(|q| q.matches(row))
                && must_not.iter().all(|q| !q.matches(row))
            }
        }
    }
}
