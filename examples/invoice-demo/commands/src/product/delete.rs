use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

pub fn run(db: &mut Database, id: i64) -> Result<ZSet, CommandError> {
    let params = Params::from([p_int("id", id)]);
    execute_sql(db, "DELETE FROM products WHERE products.id = :id", params)
}
