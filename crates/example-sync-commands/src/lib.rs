use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::{Database, MutationResult};
use sql_engine::execute::{Params, ParamValue};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum UserCommand {
    InsertUser { id: i64, name: String, age: i64 },
    UpdateUser { id: i64, name: String, age: i64 },
    DeleteUsers { ids: Vec<i64> },
    InsertOrder { id: i64, user_id: i64, amount: i64, status: String },
    DeleteOrders { ids: Vec<i64> },
}

impl Command for UserCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let result = match self {
            UserCommand::InsertUser { id, name, age } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("name".into(), ParamValue::Text(name.clone())),
                    ("age".into(), ParamValue::Int(*age)),
                ]);
                db.execute_mut_with_params(
                    "INSERT INTO users VALUES (:id, :name, :age)",
                    params,
                )
            }
            UserCommand::UpdateUser { id, name, age } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("name".into(), ParamValue::Text(name.clone())),
                    ("age".into(), ParamValue::Int(*age)),
                ]);
                db.execute_mut_with_params(
                    "UPDATE users SET name = :name, age = :age WHERE users.id = :id",
                    params,
                )
            }
            UserCommand::DeleteUsers { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                let params = Params::from([
                    ("ids".into(), ParamValue::IntList(ids.clone())),
                ]);
                db.execute_mut_with_params(
                    "DELETE FROM users WHERE users.id IN (:ids)",
                    params,
                )
            }
            UserCommand::InsertOrder { id, user_id, amount, status } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("user_id".into(), ParamValue::Int(*user_id)),
                    ("amount".into(), ParamValue::Int(*amount)),
                    ("status".into(), ParamValue::Text(status.clone())),
                ]);
                db.execute_mut_with_params(
                    "INSERT INTO orders VALUES (:id, :user_id, :amount, :status)",
                    params,
                )
            }
            UserCommand::DeleteOrders { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                let params = Params::from([
                    ("ids".into(), ParamValue::IntList(ids.clone())),
                ]);
                db.execute_mut_with_params(
                    "DELETE FROM orders WHERE orders.id IN (:ids)",
                    params,
                )
            }
        };
        let result = result.map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
        Ok(mutation_result_to_zset(result))
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
