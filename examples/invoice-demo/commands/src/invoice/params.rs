use sql_engine::execute::{Params, ParamValue};

/// Shared builder for the invoices-table parameter map.
/// `customer_id = None` is used for header updates (UPDATE doesn't touch the FK).
#[allow(clippy::too_many_arguments)]
pub fn invoice_params(
    id: i64,
    customer_id: Option<i64>,
    number: &str, status: &str, date_issued: &str, date_due: &str, notes: &str,
    doc_type: &str, parent_id: i64, service_date: &str,
    cash_allowance_pct: i64, cash_allowance_days: i64, discount_pct: i64,
    payment_method: &str, sepa_mandate_id: i64, currency: &str, language: &str,
    project_ref: &str, external_id: &str,
    billing_street: &str, billing_zip: &str, billing_city: &str, billing_country: &str,
    shipping_street: &str, shipping_zip: &str, shipping_city: &str, shipping_country: &str,
) -> Params {
    let mut p = Params::new();
    p.insert("id".into(), ParamValue::Int(id));
    if let Some(cid) = customer_id {
        p.insert("customer_id".into(), ParamValue::Int(cid));
    }
    p.insert("number".into(), ParamValue::Text(number.into()));
    p.insert("status".into(), ParamValue::Text(status.into()));
    p.insert("date_issued".into(), ParamValue::Text(date_issued.into()));
    p.insert("date_due".into(), ParamValue::Text(date_due.into()));
    p.insert("notes".into(), ParamValue::Text(notes.into()));
    p.insert("doc_type".into(), ParamValue::Text(doc_type.into()));
    p.insert("parent_id".into(), ParamValue::Int(parent_id));
    p.insert("service_date".into(), ParamValue::Text(service_date.into()));
    p.insert("cash_allowance_pct".into(), ParamValue::Int(cash_allowance_pct));
    p.insert("cash_allowance_days".into(), ParamValue::Int(cash_allowance_days));
    p.insert("discount_pct".into(), ParamValue::Int(discount_pct));
    p.insert("payment_method".into(), ParamValue::Text(payment_method.into()));
    p.insert("sepa_mandate_id".into(), ParamValue::Int(sepa_mandate_id));
    p.insert("currency".into(), ParamValue::Text(currency.into()));
    p.insert("language".into(), ParamValue::Text(language.into()));
    p.insert("project_ref".into(), ParamValue::Text(project_ref.into()));
    p.insert("external_id".into(), ParamValue::Text(external_id.into()));
    p.insert("billing_street".into(), ParamValue::Text(billing_street.into()));
    p.insert("billing_zip".into(), ParamValue::Text(billing_zip.into()));
    p.insert("billing_city".into(), ParamValue::Text(billing_city.into()));
    p.insert("billing_country".into(), ParamValue::Text(billing_country.into()));
    p.insert("shipping_street".into(), ParamValue::Text(shipping_street.into()));
    p.insert("shipping_zip".into(), ParamValue::Text(shipping_zip.into()));
    p.insert("shipping_city".into(), ParamValue::Text(shipping_city.into()));
    p.insert("shipping_country".into(), ParamValue::Text(shipping_country.into()));
    p
}
