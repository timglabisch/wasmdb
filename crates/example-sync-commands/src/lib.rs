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
                let _ = id;
            }
        }
        Ok(zset)
    }
}
