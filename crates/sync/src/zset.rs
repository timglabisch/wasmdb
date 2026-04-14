use borsh::{BorshSerialize, BorshDeserialize};
use sql_engine::storage::CellValue;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ZSetEntry {
    pub table: String,
    pub row: Vec<CellValue>,
    pub weight: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ZSet {
    pub entries: Vec<ZSetEntry>,
}

impl ZSet {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn insert(&mut self, table: String, row: Vec<CellValue>) {
        self.entries.push(ZSetEntry { table, row, weight: 1 });
    }

    pub fn delete(&mut self, table: String, row: Vec<CellValue>) {
        self.entries.push(ZSetEntry { table, row, weight: -1 });
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn extend(&mut self, other: ZSet) {
        self.entries.extend(other.entries);
    }

    pub fn invert(&self) -> ZSet {
        ZSet {
            entries: self.entries.iter().map(|e| ZSetEntry {
                table: e.table.clone(),
                row: e.row.clone(),
                weight: -e.weight,
            }).collect(),
        }
    }
}
