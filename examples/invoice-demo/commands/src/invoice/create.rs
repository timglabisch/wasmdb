use database::Database;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::execute_sql;
use super::params::invoice_params;

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Database,
    id: i64, customer_id: i64,
    number: &str, status: &str, date_issued: &str, date_due: &str, notes: &str,
    doc_type: &str, parent_id: i64, service_date: &str,
    cash_allowance_pct: i64, cash_allowance_days: i64, discount_pct: i64,
    payment_method: &str, sepa_mandate_id: i64, currency: &str, language: &str,
    project_ref: &str, external_id: &str,
    billing_street: &str, billing_zip: &str, billing_city: &str, billing_country: &str,
    shipping_street: &str, shipping_zip: &str, shipping_city: &str, shipping_country: &str,
) -> Result<ZSet, CommandError> {
    let params = invoice_params(
        id, Some(customer_id), number, status, date_issued, date_due, notes,
        doc_type, parent_id, service_date,
        cash_allowance_pct, cash_allowance_days, discount_pct,
        payment_method, sepa_mandate_id, currency, language,
        project_ref, external_id,
        billing_street, billing_zip, billing_city, billing_country,
        shipping_street, shipping_zip, shipping_city, shipping_country,
    );
    execute_sql(db,
        "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
         VALUES (:id, :customer_id, :number, :status, :date_issued, :date_due, :notes, :doc_type, :parent_id, :service_date, :cash_allowance_pct, :cash_allowance_days, :discount_pct, :payment_method, :sepa_mandate_id, :currency, :language, :project_ref, :external_id, :billing_street, :billing_zip, :billing_city, :billing_country, :shipping_street, :shipping_zip, :shipping_city, :shipping_country)",
        params)
}
