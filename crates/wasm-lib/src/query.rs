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
pub enum Query {
    Term(HashMap<String, String>),
    Bool {
        #[serde(default)]
        must: Vec<Query>,
        #[serde(default)]
        must_not: Vec<Query>,
    },
}

// Pre-resolved query with u16 field IDs
pub enum ResolvedQuery {
    Term(Vec<(u16, String)>),
    Bool {
        must: Vec<ResolvedQuery>,
        must_not: Vec<ResolvedQuery>,
    },
}

impl ResolvedQuery {
    pub fn resolve(query: &Query, field_ids: &HashMap<String, u16>) -> Self {
        match query {
            Query::Term(map) => ResolvedQuery::Term(
                map.iter()
                    .map(|(k, v)| (field_ids.get(k).copied().unwrap_or(u16::MAX), v.clone()))
                    .collect()
            ),
            Query::Bool { must, must_not } => ResolvedQuery::Bool {
                must: must.iter().map(|q| Self::resolve(q, field_ids)).collect(),
                must_not: must_not.iter().map(|q| Self::resolve(q, field_ids)).collect(),
            },
        }
    }

    pub fn matches(&self, row: &Row) -> bool {
        match self {
            ResolvedQuery::Term(pairs) => pairs.iter().all(|(id, v)| row.get(id) == Some(v)),
            ResolvedQuery::Bool { must, must_not } => {
                must.iter().all(|q| q.matches(row))
                && must_not.iter().all(|q| !q.matches(row))
            }
        }
    }
}

#[derive(Deserialize)]
pub struct ProjectionConfig {
    pub query: Query,
    pub fields: Option<Vec<String>>,
}
