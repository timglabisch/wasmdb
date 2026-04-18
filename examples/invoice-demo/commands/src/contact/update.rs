use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64,
    name: &str, email: &str, phone: &str, role: &str,
    is_primary: i64,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_str("name", name), p_str("email", email),
        p_str("phone", phone), p_str("role", role),
        p_int("is_primary", is_primary),
    ]);
    execute_sql(db,
        "UPDATE contacts SET name = :name, email = :email, phone = :phone, role = :role, is_primary = :is_primary WHERE contacts.id = :id",
        params)
}
