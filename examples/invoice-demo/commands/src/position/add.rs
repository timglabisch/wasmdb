use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Database,
    id: i64, invoice_id: i64, position_nr: i64,
    description: &str, quantity: i64, unit_price: i64, tax_rate: i64,
    product_id: i64, item_number: &str, unit: &str,
    discount_pct: i64, cost_price: i64, position_type: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("invoice_id", invoice_id),
        p_int("position_nr", position_nr),
        p_str("description", description),
        p_int("quantity", quantity), p_int("unit_price", unit_price),
        p_int("tax_rate", tax_rate),
        p_int("product_id", product_id),
        p_str("item_number", item_number),
        p_str("unit", unit),
        p_int("discount_pct", discount_pct),
        p_int("cost_price", cost_price),
        p_str("position_type", position_type),
    ]);
    execute_sql(db,
        "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
         VALUES (:id, :invoice_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :product_id, :item_number, :unit, :discount_pct, :cost_price, :position_type)",
        params)
}
