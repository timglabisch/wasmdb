use borsh::{BorshSerialize, BorshDeserialize};
use database::Database;
use sql_engine::storage::CellValue;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum UserCommand {
    Insert { id: i64, name: String, age: i64 },
    Delete { id: i64 },
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
