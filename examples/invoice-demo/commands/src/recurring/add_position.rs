use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Database,
    id: i64, recurring_id: i64, position_nr: i64,
    description: &str, quantity: i64, unit_price: i64, tax_rate: i64,
    unit: &str, item_number: &str, discount_pct: i64,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("recurring_id", recurring_id),
        p_int("position_nr", position_nr),
        p_str("description", description),
        p_int("quantity", quantity), p_int("unit_price", unit_price),
        p_int("tax_rate", tax_rate),
        p_str("unit", unit), p_str("item_number", item_number),
        p_int("discount_pct", discount_pct),
    ]);
    execute_sql(db,
        "INSERT INTO recurring_positions (id, recurring_id, position_nr, description, quantity, unit_price, tax_rate, unit, item_number, discount_pct) \
         VALUES (:id, :recurring_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :unit, :item_number, :discount_pct)",
        params)
}
