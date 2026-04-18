use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, mandate_ref: &str,
    iban: &str, bic: &str, holder_name: &str,
    signed_at: &str, status: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id),
        p_str("mandate_ref", mandate_ref),
        p_str("iban", iban), p_str("bic", bic),
        p_str("holder_name", holder_name),
        p_str("signed_at", signed_at),
        p_str("status", status),
    ]);
    execute_sql(db,
        "UPDATE sepa_mandates SET mandate_ref = :mandate_ref, iban = :iban, bic = :bic, holder_name = :holder_name, signed_at = :signed_at, status = :status WHERE sepa_mandates.id = :id",
        params)
}
