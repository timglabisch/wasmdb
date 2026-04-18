use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

pub fn run(db: &mut Database, id: i64, new_position_nr: i64) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("position_nr", new_position_nr),
    ]);
    execute_sql(db,
        "UPDATE positions SET position_nr = :position_nr WHERE positions.id = :id",
        params)
}
