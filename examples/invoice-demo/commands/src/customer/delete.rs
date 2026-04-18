use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteCustomer {
    pub id: i64,
}

impl DeleteCustomer {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = Params::from([p_int("id", self.id)]);
        execute_sql(db, "DELETE FROM customers WHERE customers.id = :id", params)
    }
}
