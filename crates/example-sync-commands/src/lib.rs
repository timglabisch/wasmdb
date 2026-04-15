use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::storage::CellValue;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum UserCommand {
    Insert {
        #[ts(type = "number")]
        id: i64,
        name: String,
        #[ts(type = "number")]
        age: i64,
    },
    Delete {
        #[ts(type = "number")]
        id: i64,
    },
}

impl Command for UserCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let mut zset = ZSet::new();
        match self {
            UserCommand::Insert { id, name, age } => {
                let row = vec![
                    CellValue::I64(*id),
                    CellValue::Str(name.clone()),
                    CellValue::I64(*age),
                ];
                db.insert("users", &row)
                    .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
                zset.insert("users".into(), row);
            }
            UserCommand::Delete { id } => {
                let table = db.table("users")
                    .ok_or_else(|| CommandError::ExecutionFailed("table users not found".into()))?;
                // Find the row by scanning for matching id (column 0 = PK)
                let row_idx = table.row_ids()
                    .find(|&r| table.get(r, 0) == CellValue::I64(*id))
                    .ok_or_else(|| CommandError::ExecutionFailed(format!("user {} not found", id)))?;
                let row: Vec<CellValue> = (0..3).map(|c| table.get(row_idx, c)).collect();
                let table = db.table_mut("users")
                    .ok_or_else(|| CommandError::ExecutionFailed("table users not found".into()))?;
                table.delete(row_idx)
                    .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
                zset.delete("users".into(), row);
            }
        }
        Ok(zset)
    }
}
