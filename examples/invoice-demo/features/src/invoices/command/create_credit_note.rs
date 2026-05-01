use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid, p_uuid_opt, read_i64_col, read_str_col, read_uuid_col};
use crate::shared::DEMO_TENANT_ID;
use super::invoice_params::invoice_params;

/// Intent-Command: create a credit-note (Gutschrift) referencing an existing
/// invoice. Copies all positions with negated quantities, sets `doc_type =
/// 'credit_note'` and `parent_id` to the source invoice. Emits an activity
/// row in the same atomic step.
///
/// All UUIDs are pre-computed by the client wrapper for idempotent re-apply.
#[rpc_command]
pub struct CreateCreditNote {
    /// The source invoice to create a credit note for.
    #[ts(type = "string")]
    pub source_invoice_id: Uuid,
    /// Pre-computed id for the new credit-note invoice.
    #[ts(type = "string")]
    pub new_invoice_id: Uuid,
    /// Pre-computed ids for the new positions (negated quantities), one per
    /// source position in `position_nr` order.
    #[ts(type = "string[]")]
    pub new_position_ids: Vec<Uuid>,
    /// The number string for the new invoice (e.g. `"CN-<source>"`).
    pub new_number: String,
    /// ISO date for `date_issued` on the new draft.
    pub date_issued: String,
    /// ISO date for `date_due` on the new draft.
    pub date_due: String,
    /// Pre-computed id for the activity_log row.
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(source_number: &str, source_id: &Uuid, new_id: &Uuid) -> String {
    format!("Gutschrift zu \"{source_number}\" (#{source_id}) als #{new_id} angelegt")
}

impl Command for CreateCreditNote {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let src = self.source_invoice_id;
        let new_id = self.new_invoice_id;

        // --- read source invoice header ---
        let numbers = read_str_col(db,
            "SELECT invoices.number FROM invoices WHERE invoices.id = :id",
            Params::from([p_uuid("id", &src)]))?;
        let src_number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&src_number, &src, &new_id);

        let customer_ids = read_uuid_col(db,
            "SELECT invoices.customer_id FROM invoices WHERE invoices.id = :id",
            Params::from([p_uuid("id", &src)]))?;
        let customer_id: Option<Uuid> = customer_ids.into_iter().next();

        macro_rules! str_field {
            ($col:literal) => {{
                read_str_col(db,
                    concat!("SELECT invoices.", $col, " FROM invoices WHERE invoices.id = :id"),
                    Params::from([p_uuid("id", &src)]))?
                    .into_iter().next().unwrap_or_default()
            }};
        }
        macro_rules! int_field {
            ($col:literal) => {{
                read_i64_col(db,
                    concat!("SELECT invoices.", $col, " FROM invoices WHERE invoices.id = :id"),
                    Params::from([p_uuid("id", &src)]))?
                    .into_iter().next().unwrap_or_default()
            }};
        }

        let notes           = str_field!("notes");
        let service_date    = str_field!("service_date");
        let cash_pct        = int_field!("cash_allowance_pct");
        let cash_days       = int_field!("cash_allowance_days");
        let discount_pct    = int_field!("discount_pct");
        let payment_method  = str_field!("payment_method");
        let currency        = str_field!("currency");
        let language        = str_field!("language");
        let project_ref     = str_field!("project_ref");
        let external_id     = str_field!("external_id");
        let billing_street  = str_field!("billing_street");
        let billing_zip     = str_field!("billing_zip");
        let billing_city    = str_field!("billing_city");
        let billing_country = str_field!("billing_country");
        let shipping_street  = str_field!("shipping_street");
        let shipping_zip     = str_field!("shipping_zip");
        let shipping_city    = str_field!("shipping_city");
        let shipping_country = str_field!("shipping_country");

        let sepa_mandate_id: Option<Uuid> = read_uuid_col(db,
            "SELECT invoices.sepa_mandate_id FROM invoices WHERE invoices.id = :id",
            Params::from([p_uuid("id", &src)]))?
            .into_iter().next();

        // credit note: doc_type = 'credit_note', parent_id = source
        let parent_id: Option<Uuid> = Some(src);

        let inv_params = invoice_params(
            &new_id, Some(&customer_id),
            &self.new_number, "draft", &self.date_issued, &self.date_due, &notes,
            "credit_note", &parent_id, &service_date,
            cash_pct, cash_days, discount_pct,
            &payment_method, &sepa_mandate_id, &currency, &language,
            &project_ref, &external_id,
            &billing_street, &billing_zip, &billing_city, &billing_country,
            &shipping_street, &shipping_zip, &shipping_city, &shipping_country,
        );

        let mut acc = execute_sql(db,
            "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES (:id, :customer_id, :number, :status, :date_issued, :date_due, :notes, :doc_type, :parent_id, :service_date, :cash_allowance_pct, :cash_allowance_days, :discount_pct, :payment_method, :sepa_mandate_id, :currency, :language, :project_ref, :external_id, :billing_street, :billing_zip, :billing_city, :billing_country, :shipping_street, :shipping_zip, :shipping_city, :shipping_country)",
            inv_params)?;

        // --- copy positions with negated quantities ---
        let descs     = read_str_col(db, "SELECT description FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr", Params::from([p_uuid("iid", &src)]))?;
        let qtys      = read_i64_col(db, "SELECT quantity FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",    Params::from([p_uuid("iid", &src)]))?;
        let prices    = read_i64_col(db, "SELECT unit_price FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",  Params::from([p_uuid("iid", &src)]))?;
        let taxes     = read_i64_col(db, "SELECT tax_rate FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",     Params::from([p_uuid("iid", &src)]))?;
        let items     = read_str_col(db, "SELECT item_number FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",  Params::from([p_uuid("iid", &src)]))?;
        let units     = read_str_col(db, "SELECT unit FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",         Params::from([p_uuid("iid", &src)]))?;
        let discounts = read_i64_col(db, "SELECT discount_pct FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr", Params::from([p_uuid("iid", &src)]))?;
        let costs     = read_i64_col(db, "SELECT cost_price FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",   Params::from([p_uuid("iid", &src)]))?;
        let pos_types = read_str_col(db, "SELECT position_type FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",Params::from([p_uuid("iid", &src)]))?;
        let pos_nrs   = read_i64_col(db, "SELECT position_nr FROM positions WHERE positions.invoice_id = :iid ORDER BY positions.position_nr",  Params::from([p_uuid("iid", &src)]))?;

        if descs.len() != self.new_position_ids.len() {
            return Err(CommandError::ExecutionFailed(format!(
                "CreateCreditNote: source has {} positions but got {} ids",
                descs.len(), self.new_position_ids.len(),
            )));
        }

        for (i, pid) in self.new_position_ids.iter().enumerate() {
            let params = Params::from([
                p_uuid("id", pid),
                p_uuid("invoice_id", &new_id),
                p_int("position_nr", pos_nrs.get(i).copied().unwrap_or((i as i64 + 1) * 1000)),
                p_str("description", &descs[i]),
                p_int("quantity", -qtys[i]),      // negated for credit note
                p_int("unit_price", prices[i]),
                p_int("tax_rate", taxes[i]),
                p_uuid_opt("product_id", &None),
                p_str("item_number", items.get(i).map(|s| s.as_str()).unwrap_or("")),
                p_str("unit", units.get(i).map(|s| s.as_str()).unwrap_or("")),
                p_int("discount_pct", discounts.get(i).copied().unwrap_or(0)),
                p_int("cost_price", costs.get(i).copied().unwrap_or(0)),
                p_str("position_type", pos_types.get(i).map(|s| s.as_str()).unwrap_or("service")),
            ]);
            acc.extend(execute_sql(db,
                "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                 VALUES (:id, :invoice_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :product_id, :item_number, :unit, :discount_pct, :cost_price, :position_type)",
                params)?);
        }

        // --- activity row ---
        acc.extend(execute_sql(db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'invoice', :eid, 'credit_note_created', 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("eid", &new_id),
                p_str("detail", &detail),
            ]))?);

        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for CreateCreditNote {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let src = self.source_invoice_id;
            let new_id = self.new_invoice_id;

            // --- read source invoice header ---
            #[derive(sqlx::FromRow)]
            struct SrcHeader {
                number: String,
                customer_id: Option<Vec<u8>>,
                notes: String,
                service_date: String,
                cash_allowance_pct: i64,
                cash_allowance_days: i64,
                discount_pct: i64,
                payment_method: String,
                sepa_mandate_id: Option<Vec<u8>>,
                currency: String,
                language: String,
                project_ref: String,
                external_id: String,
                billing_street: String,
                billing_zip: String,
                billing_city: String,
                billing_country: String,
                shipping_street: String,
                shipping_zip: String,
                shipping_city: String,
                shipping_country: String,
            }

            let hdr: SrcHeader = sqlx::query_as(
                "SELECT number, customer_id, notes, service_date, \
                 cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, \
                 sepa_mandate_id, currency, language, project_ref, external_id, \
                 billing_street, billing_zip, billing_city, billing_country, \
                 shipping_street, shipping_zip, shipping_city, shipping_country \
                 FROM invoices WHERE tenant_id = ? AND id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&src.0[..])
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup source invoice {src}: {e}",
            )))?;

            let detail = detail_for(&hdr.number, &src, &new_id);

            // --- insert new credit-note invoice ---
            sqlx::query(
                "INSERT INTO invoices (tenant_id, id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&new_id.0[..])
            .bind(hdr.customer_id.as_deref())
            .bind(&self.new_number)
            .bind("draft")
            .bind(&self.date_issued)
            .bind(&self.date_due)
            .bind(&hdr.notes)
            .bind("credit_note")
            .bind(&src.0[..])           // parent_id = source
            .bind(&hdr.service_date)
            .bind(hdr.cash_allowance_pct)
            .bind(hdr.cash_allowance_days)
            .bind(hdr.discount_pct)
            .bind(&hdr.payment_method)
            .bind(hdr.sepa_mandate_id.as_deref())
            .bind(&hdr.currency)
            .bind(&hdr.language)
            .bind(&hdr.project_ref)
            .bind(&hdr.external_id)
            .bind(&hdr.billing_street)
            .bind(&hdr.billing_zip)
            .bind(&hdr.billing_city)
            .bind(&hdr.billing_country)
            .bind(&hdr.shipping_street)
            .bind(&hdr.shipping_zip)
            .bind(&hdr.shipping_city)
            .bind(&hdr.shipping_country)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT credit note invoice {new_id}: {e}",
            )))?;

            // --- read source positions ---
            #[derive(sqlx::FromRow)]
            struct PosRow {
                position_nr: i64,
                description: String,
                quantity: i64,
                unit_price: i64,
                tax_rate: i64,
                item_number: String,
                unit: String,
                discount_pct: i64,
                cost_price: i64,
                position_type: String,
            }

            let positions: Vec<PosRow> = sqlx::query_as(
                "SELECT position_nr, description, quantity, unit_price, tax_rate, \
                 item_number, unit, discount_pct, cost_price, position_type \
                 FROM positions WHERE tenant_id = ? AND invoice_id = ? ORDER BY position_nr",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&src.0[..])
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup positions for source invoice {src}: {e}",
            )))?;

            if positions.len() != self.new_position_ids.len() {
                return Err(CommandError::ExecutionFailed(format!(
                    "CreateCreditNote: source has {} positions but got {} ids",
                    positions.len(), self.new_position_ids.len(),
                )));
            }

            for (i, pid) in self.new_position_ids.iter().enumerate() {
                let pos = &positions[i];
                sqlx::query(
                    "INSERT INTO positions (tenant_id, id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE id = id",
                )
                .bind(DEMO_TENANT_ID)
                .bind(&pid.0[..])
                .bind(&new_id.0[..])
                .bind(pos.position_nr)
                .bind(&pos.description)
                .bind(-pos.quantity)        // negated for credit note
                .bind(pos.unit_price)
                .bind(pos.tax_rate)
                .bind(Option::<Vec<u8>>::None)
                .bind(&pos.item_number)
                .bind(&pos.unit)
                .bind(pos.discount_pct)
                .bind(pos.cost_price)
                .bind(&pos.position_type)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT position {pid} for credit note {new_id}: {e}",
                )))?;
            }

            // --- activity row ---
            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'invoice', ?, 'credit_note_created', 'demo', ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.activity_id.0[..])
            .bind(&self.timestamp)
            .bind(&new_id.0[..])
            .bind(&detail)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT activity {} for credit note {new_id}: {e}", self.activity_id,
            )))?;

            Ok(client_zset.clone())
        }
    }
}
