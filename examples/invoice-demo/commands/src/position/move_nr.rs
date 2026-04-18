use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct MovePosition {
    pub id: i64,
    pub new_position_nr: i64,
}

impl MovePosition {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("position_nr", self.new_position_nr),
        ]);
        execute_sql(db,
            "UPDATE positions SET position_nr = :position_nr WHERE positions.id = :id",
            params)
    }
}
