use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::{Database, MutationResult};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum UserCommand {
    Sql {
        sql: String,
    },
}

impl Command for UserCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            UserCommand::Sql { sql } => {
                let result = db.execute_mut(sql)
                    .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
                Ok(mutation_result_to_zset(result))
            }
        }
    }
}

fn mutation_result_to_zset(result: MutationResult) -> ZSet {
    let mut zset = ZSet::new();
    match result {
        MutationResult::Inserted { table, rows } => {
            for row in rows {
                zset.insert(table.clone(), row);
            }
        }
        MutationResult::Deleted { table, rows } => {
            for row in rows {
                zset.delete(table.clone(), row);
            }
        }
        MutationResult::Updated { table, old_new } => {
            for (old, new) in old_new {
                zset.delete(table.clone(), old);
                zset.insert(table.clone(), new);
            }
        }
        MutationResult::Rows(_) | MutationResult::Ddl => {}
    }
    zset
}
