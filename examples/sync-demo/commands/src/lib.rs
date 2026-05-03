use borsh::{BorshSerialize, BorshDeserialize};
use database::{Database, MutResult};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use sql_engine::execute::{Params, ParamValue};
use sqlbuilder::{sql, SqlStmt, Value};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum UserCommand {
    InsertUser {
        #[ts(type = "number")] id: i64,
        name: String,
        #[ts(type = "number")] age: i64,
    },
    UpdateUser {
        #[ts(type = "number")] id: i64,
        name: String,
        #[ts(type = "number")] age: i64,
    },
    DeleteUsers {
        #[ts(type = "number[]")] ids: Vec<i64>,
    },
    InsertOrder {
        #[ts(type = "number")] id: i64,
        #[ts(type = "number")] user_id: i64,
        #[ts(type = "number")] amount: i64,
        status: String,
    },
    DeleteOrders {
        #[ts(type = "number[]")] ids: Vec<i64>,
    },
}

fn to_param_value(v: Value) -> ParamValue {
    match v {
        Value::Int(n) => ParamValue::Int(n),
        Value::Text(s) => ParamValue::Text(s),
        Value::Uuid(b) => ParamValue::Uuid(b),
        Value::Null => ParamValue::Null,
        Value::IntList(xs) => ParamValue::IntList(xs),
        Value::TextList(xs) => ParamValue::TextList(xs),
        Value::UuidList(xs) => ParamValue::UuidList(xs),
    }
}

fn execute_stmt(db: &mut Database, stmt: SqlStmt) -> Result<ZSet, CommandError> {
    let rendered = stmt
        .render()
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    let params: Params = rendered
        .params
        .into_iter()
        .map(|(k, v)| (k, to_param_value(v)))
        .collect();
    match db.execute_mut_with_params(&rendered.sql, params) {
        Ok(MutResult::Mutation(z)) => Ok(z),
        Ok(MutResult::Rows(_)) | Ok(MutResult::Ddl) => Ok(ZSet::new()),
        Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
    }
}

impl Command for UserCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            UserCommand::InsertUser { id, name, age } => {
                execute_stmt(db, sql!(
                    "INSERT INTO users VALUES ({id}, {name}, {age})",
                    id = id,
                    name = name,
                    age = age,
                ))
            }
            UserCommand::UpdateUser { id, name, age } => {
                execute_stmt(db, sql!(
                    "UPDATE users SET name = {name}, age = {age} WHERE users.id = {id}",
                    id = id,
                    name = name,
                    age = age,
                ))
            }
            UserCommand::DeleteUsers { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                execute_stmt(db, sql!(
                    "DELETE FROM users WHERE users.id IN ({ids})",
                    ids = ids,
                ))
            }
            UserCommand::InsertOrder { id, user_id, amount, status } => {
                execute_stmt(db, sql!(
                    "INSERT INTO orders VALUES ({id}, {user_id}, {amount}, {status})",
                    id = id,
                    user_id = user_id,
                    amount = amount,
                    status = status,
                ))
            }
            UserCommand::DeleteOrders { ids } => {
                if ids.is_empty() {
                    return Ok(ZSet::new());
                }
                execute_stmt(db, sql!(
                    "DELETE FROM orders WHERE orders.id IN ({ids})",
                    ids = ids,
                ))
            }
        }
    }
}
