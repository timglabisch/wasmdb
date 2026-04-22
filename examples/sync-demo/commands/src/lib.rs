use borsh::{BorshSerialize, BorshDeserialize};
use database::{Database, MutResult};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
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

fn execute_sql(
    db: &mut Database,
    sql: &str,
    params: Params,
) -> Result<ZSet, CommandError> {
    match db.execute_mut_with_params(sql, params) {
        Ok(MutResult::Mutation(z)) => Ok(z),
        Ok(MutResult::Rows(_)) | Ok(MutResult::Ddl) => Ok(ZSet::new()),
        Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
    }
}

impl Command for UserCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            UserCommand::InsertUser { id, name, age } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("name".into(), ParamValue::Text(name.clone())),
                    ("age".into(), ParamValue::Int(*age)),
                ]);
                execute_sql(db, "INSERT INTO users VALUES (:id, :name, :age)", params)
            }
            UserCommand::UpdateUser { id, name, age } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("name".into(), ParamValue::Text(name.clone())),
                    ("age".into(), ParamValue::Int(*age)),
                ]);
                execute_sql(db, "UPDATE users SET name = :name, age = :age WHERE users.id = :id", params)
            }
            UserCommand::DeleteUsers { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                let params = Params::from([
                    ("ids".into(), ParamValue::IntList(ids.clone())),
                ]);
                execute_sql(db, "DELETE FROM users WHERE users.id IN (:ids)", params)
            }
            UserCommand::InsertOrder { id, user_id, amount, status } => {
                let params = Params::from([
                    ("id".into(), ParamValue::Int(*id)),
                    ("user_id".into(), ParamValue::Int(*user_id)),
                    ("amount".into(), ParamValue::Int(*amount)),
                    ("status".into(), ParamValue::Text(status.clone())),
                ]);
                execute_sql(db, "INSERT INTO orders VALUES (:id, :user_id, :amount, :status)", params)
            }
            UserCommand::DeleteOrders { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                let params = Params::from([
                    ("ids".into(), ParamValue::IntList(ids.clone())),
                ]);
                execute_sql(db, "DELETE FROM orders WHERE orders.id IN (:ids)", params)
            }
        }
    }
}
