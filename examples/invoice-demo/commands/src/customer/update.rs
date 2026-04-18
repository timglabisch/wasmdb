use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Database,
    id: i64,
    name: &str, email: &str,
    company_type: &str, tax_id: &str, vat_id: &str,
    payment_terms_days: i64, default_discount_pct: i64,
    billing_street: &str, billing_zip: &str, billing_city: &str, billing_country: &str,
    shipping_street: &str, shipping_zip: &str, shipping_city: &str, shipping_country: &str,
    default_iban: &str, default_bic: &str, notes: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_str("name", name), p_str("email", email),
        p_str("company_type", company_type),
        p_str("tax_id", tax_id), p_str("vat_id", vat_id),
        p_int("payment_terms_days", payment_terms_days),
        p_int("default_discount_pct", default_discount_pct),
        p_str("billing_street", billing_street),
        p_str("billing_zip", billing_zip),
        p_str("billing_city", billing_city),
        p_str("billing_country", billing_country),
        p_str("shipping_street", shipping_street),
        p_str("shipping_zip", shipping_zip),
        p_str("shipping_city", shipping_city),
        p_str("shipping_country", shipping_country),
        p_str("default_iban", default_iban),
        p_str("default_bic", default_bic),
        p_str("notes", notes),
    ]);
    execute_sql(db,
        "UPDATE customers SET name = :name, email = :email, \
         company_type = :company_type, tax_id = :tax_id, vat_id = :vat_id, \
         payment_terms_days = :payment_terms_days, default_discount_pct = :default_discount_pct, \
         billing_street = :billing_street, billing_zip = :billing_zip, billing_city = :billing_city, billing_country = :billing_country, \
         shipping_street = :shipping_street, shipping_zip = :shipping_zip, shipping_city = :shipping_city, shipping_country = :shipping_country, \
         default_iban = :default_iban, default_bic = :default_bic, notes = :notes \
         WHERE customers.id = :id",
        params)
}
