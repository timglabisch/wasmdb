use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str, read_i64_col, read_str_col};
use crate::invoice::params::invoice_params;

/// Creates a new invoice with positions copied from the recurring template.
/// `position_ids` must have as many entries as the template has positions.
/// Updates last_run + next_run on the recurring row.
pub fn run(
    db: &mut Database,
    recurring_id: i64,
    new_invoice_id: i64,
    position_ids: &[i64],
    new_number: &str,
    issue_date: &str,
    due_date: &str,
    new_next_run: &str,
) -> Result<ZSet, CommandError> {
    // Load template header fields we need.
    let customer_id = read_i64_col(db,
        "SELECT customer_id FROM recurring_invoices WHERE id = :rid",
        Params::from([p_int("rid", recurring_id)]))?
        .into_iter().next()
        .ok_or_else(|| CommandError::ExecutionFailed(format!("recurring #{recurring_id} not found")))?;
    let status_templates = read_str_col(db,
        "SELECT status_template FROM recurring_invoices WHERE id = :rid",
        Params::from([p_int("rid", recurring_id)]))?;
    let notes_templates = read_str_col(db,
        "SELECT notes_template FROM recurring_invoices WHERE id = :rid",
        Params::from([p_int("rid", recurring_id)]))?;
    let status = status_templates.into_iter().next().unwrap_or_else(|| "draft".into());
    let notes = notes_templates.into_iter().next().unwrap_or_default();

    // Load template positions (ordered).
    let descs = read_str_col(db,
        "SELECT description FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let qtys = read_i64_col(db,
        "SELECT quantity FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let prices = read_i64_col(db,
        "SELECT unit_price FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let taxes = read_i64_col(db,
        "SELECT tax_rate FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let units = read_str_col(db,
        "SELECT unit FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let items = read_str_col(db,
        "SELECT item_number FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;
    let discounts = read_i64_col(db,
        "SELECT discount_pct FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
        Params::from([p_int("rid", recurring_id)]))?;

    if descs.len() != position_ids.len() {
        return Err(CommandError::ExecutionFailed(format!(
            "RunRecurringOnce: template has {} positions but got {} ids",
            descs.len(), position_ids.len(),
        )));
    }

    let mut acc = ZSet::new();

    // 1. Create the new invoice.
    let inv_params = invoice_params(
        new_invoice_id, Some(customer_id), new_number, &status,
        issue_date, due_date, &notes,
        "invoice", 0, "",
        0, 0, 0,
        "transfer", 0, "EUR", "de",
        "", "",
        "", "", "", "",
        "", "", "", "",
    );
    acc.extend(execute_sql(db,
        "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
         VALUES (:id, :customer_id, :number, :status, :date_issued, :date_due, :notes, :doc_type, :parent_id, :service_date, :cash_allowance_pct, :cash_allowance_days, :discount_pct, :payment_method, :sepa_mandate_id, :currency, :language, :project_ref, :external_id, :billing_street, :billing_zip, :billing_city, :billing_country, :shipping_street, :shipping_zip, :shipping_city, :shipping_country)",
        inv_params)?);

    // 2. Copy positions.
    for (i, pid) in position_ids.iter().enumerate() {
        let params = Params::from([
            p_int("id", *pid),
            p_int("invoice_id", new_invoice_id),
            p_int("position_nr", (i as i64 + 1) * 1000),
            p_str("description", &descs[i]),
            p_int("quantity", qtys[i]),
            p_int("unit_price", prices[i]),
            p_int("tax_rate", taxes[i]),
            p_int("product_id", 0),
            p_str("item_number", &items[i]),
            p_str("unit", &units[i]),
            p_int("discount_pct", discounts[i]),
            p_int("cost_price", 0),
            p_str("position_type", "service"),
        ]);
        acc.extend(execute_sql(db,
            "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
             VALUES (:id, :invoice_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :product_id, :item_number, :unit, :discount_pct, :cost_price, :position_type)",
            params)?);
    }

    // 3. Advance next_run + last_run on recurring template.
    let params = Params::from([
        p_int("id", recurring_id),
        p_str("last_run", issue_date),
        p_str("next_run", new_next_run),
    ]);
    acc.extend(execute_sql(db,
        "UPDATE recurring_invoices SET last_run = :last_run, next_run = :next_run WHERE recurring_invoices.id = :id",
        params)?);

    Ok(acc)
}
