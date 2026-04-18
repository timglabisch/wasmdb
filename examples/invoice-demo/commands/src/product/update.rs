use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Database,
    id: i64, sku: &str, name: &str, description: &str,
    unit: &str, unit_price: i64, tax_rate: i64, cost_price: i64,
    active: i64,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id),
        p_str("sku", sku), p_str("name", name), p_str("description", description),
        p_str("unit", unit),
        p_int("unit_price", unit_price), p_int("tax_rate", tax_rate),
        p_int("cost_price", cost_price),
        p_int("active", active),
    ]);
    execute_sql(db,
        "UPDATE products SET sku = :sku, name = :name, description = :description, unit = :unit, unit_price = :unit_price, tax_rate = :tax_rate, cost_price = :cost_price, active = :active WHERE products.id = :id",
        params)
}
