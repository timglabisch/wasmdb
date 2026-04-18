use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64,
    description: &str, quantity: i64, unit_price: i64, tax_rate: i64,
    unit: &str, item_number: &str, discount_pct: i64,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id),
        p_str("description", description),
        p_int("quantity", quantity), p_int("unit_price", unit_price),
        p_int("tax_rate", tax_rate),
        p_str("unit", unit), p_str("item_number", item_number),
        p_int("discount_pct", discount_pct),
    ]);
    execute_sql(db,
        "UPDATE recurring_positions SET description = :description, quantity = :quantity, unit_price = :unit_price, tax_rate = :tax_rate, unit = :unit, item_number = :item_number, discount_pct = :discount_pct WHERE recurring_positions.id = :id",
        params)
}
