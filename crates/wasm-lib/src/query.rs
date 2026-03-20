use std::collections::HashMap;
use serde::Deserialize;

pub type Row = HashMap<String, String>;

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

impl Query {
    pub fn matches(&self, row: &Row) -> bool {
        match self {
            Query::Term(map) => map.iter().all(|(k, v)| row.get(k) == Some(v)),
            Query::Bool { must, must_not } => {
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
