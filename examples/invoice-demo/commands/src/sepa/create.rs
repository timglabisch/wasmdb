use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, customer_id: i64, mandate_ref: &str,
    iban: &str, bic: &str, holder_name: &str,
    signed_at: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("customer_id", customer_id),
        p_str("mandate_ref", mandate_ref),
        p_str("iban", iban), p_str("bic", bic),
        p_str("holder_name", holder_name),
        p_str("signed_at", signed_at),
        p_str("status", "active"),
    ]);
    execute_sql(db,
        "INSERT INTO sepa_mandates (id, customer_id, mandate_ref, iban, bic, holder_name, signed_at, status) \
         VALUES (:id, :customer_id, :mandate_ref, :iban, :bic, :holder_name, :signed_at, :status)",
        params)
}
