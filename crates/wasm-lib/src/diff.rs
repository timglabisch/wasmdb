use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Diff {
    pub version: u32,
    pub table: String,
    pub id: String,
    pub key: String,
    pub value: String,
    pub diff: i8,
}
