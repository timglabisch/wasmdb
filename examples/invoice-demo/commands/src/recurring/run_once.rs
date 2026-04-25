use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str, p_uuid, read_i64_col, read_str_col, read_uuid_col, DEMO_TENANT_ID};
use crate::invoice::params::invoice_params;

/// Creates a new invoice with positions copied from the recurring template.
/// `position_ids` must have as many entries as the template has positions.
/// Updates last_run + next_run on the recurring row.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct RunRecurringOnce {
    #[ts(type = "string")]
    pub recurring_id: Uuid,
    #[ts(type = "string")]
    pub new_invoice_id: Uuid,
    #[ts(type = "string[]")]
    pub position_ids: Vec<Uuid>,
    pub new_number: String,
    pub issue_date: String,
    pub due_date: String,
    pub new_next_run: String,
}

impl Command for RunRecurringOnce {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let recurring_id = self.recurring_id;
        let new_invoice_id = self.new_invoice_id;
        let position_ids = &self.position_ids;
        let new_number = &self.new_number;
        let issue_date = &self.issue_date;
        let due_date = &self.due_date;
        let new_next_run = &self.new_next_run;

        let customer_id = read_uuid_col(db,
            "SELECT customer_id FROM recurring_invoices WHERE id = :rid",
            Params::from([p_uuid("rid", &recurring_id)]))?
            .into_iter().next()
            .ok_or_else(|| CommandError::ExecutionFailed(format!("recurring #{recurring_id} not found")))?;
        let status_templates = read_str_col(db,
            "SELECT status_template FROM recurring_invoices WHERE id = :rid",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let notes_templates = read_str_col(db,
            "SELECT notes_template FROM recurring_invoices WHERE id = :rid",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let status = status_templates.into_iter().next().unwrap_or_else(|| "draft".into());
        let notes = notes_templates.into_iter().next().unwrap_or_default();

        let descs = read_str_col(db,
            "SELECT description FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let qtys = read_i64_col(db,
            "SELECT quantity FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let prices = read_i64_col(db,
            "SELECT unit_price FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let taxes = read_i64_col(db,
            "SELECT tax_rate FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let units = read_str_col(db,
            "SELECT unit FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let items = read_str_col(db,
            "SELECT item_number FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;
        let discounts = read_i64_col(db,
            "SELECT discount_pct FROM recurring_positions WHERE recurring_id = :rid ORDER BY position_nr",
            Params::from([p_uuid("rid", &recurring_id)]))?;

        if descs.len() != position_ids.len() {
            return Err(CommandError::ExecutionFailed(format!(
                "RunRecurringOnce: template has {} positions but got {} ids",
                descs.len(), position_ids.len(),
            )));
        }

        let mut acc = ZSet::new();

        let nil = Uuid([0u8; 16]);
        let inv_params = invoice_params(
            &new_invoice_id, Some(&customer_id), new_number, &status,
            issue_date, due_date, &notes,
            "invoice", &nil, "",
            0, 0, 0,
            "transfer", &nil, "EUR", "de",
            "", "",
            "", "", "", "",
            "", "", "", "",
        );
        acc.extend(execute_sql(db,
            "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES (:id, :customer_id, :number, :status, :date_issued, :date_due, :notes, :doc_type, :parent_id, :service_date, :cash_allowance_pct, :cash_allowance_days, :discount_pct, :payment_method, :sepa_mandate_id, :currency, :language, :project_ref, :external_id, :billing_street, :billing_zip, :billing_city, :billing_country, :shipping_street, :shipping_zip, :shipping_city, :shipping_country)",
            inv_params)?);

        for (i, pid) in position_ids.iter().enumerate() {
            let params = Params::from([
                p_uuid("id", pid),
                p_uuid("invoice_id", &new_invoice_id),
                p_int("position_nr", (i as i64 + 1) * 1000),
                p_str("description", &descs[i]),
                p_int("quantity", qtys[i]),
                p_int("unit_price", prices[i]),
                p_int("tax_rate", taxes[i]),
                p_uuid("product_id", &nil),
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

        let params = Params::from([
            p_uuid("id", &recurring_id),
            p_str("last_run", issue_date),
            p_str("next_run", new_next_run),
        ]);
        acc.extend(execute_sql(db,
            "UPDATE recurring_invoices SET last_run = :last_run, next_run = :next_run WHERE recurring_invoices.id = :id",
            params)?);

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
    impl ServerCommand for RunRecurringOnce {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let recurring_id = self.recurring_id;
            let new_invoice_id = self.new_invoice_id;

            let (customer_id_bytes, status_template, notes_template): (Vec<u8>, String, String) =
                sqlx::query_as(
                    "SELECT customer_id, status_template, notes_template \
                     FROM recurring_invoices WHERE tenant_id = ? AND id = ?",
                )
                .bind(DEMO_TENANT_ID)
                .bind(&recurring_id.0[..])
                .fetch_optional(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup recurring_invoice {recurring_id}: {e}",
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "recurring #{recurring_id} not found",
                )))?;

            let status = if status_template.is_empty() { "draft".to_string() } else { status_template };
            let notes = notes_template;

            #[derive(sqlx::FromRow)]
            struct PosRow {
                description: String,
                quantity: i64,
                unit_price: i64,
                tax_rate: i64,
                unit: String,
                item_number: String,
                discount_pct: i64,
            }
            let positions: Vec<PosRow> = sqlx::query_as(
                "SELECT description, quantity, unit_price, tax_rate, unit, item_number, discount_pct \
                 FROM recurring_positions WHERE tenant_id = ? AND recurring_id = ? ORDER BY position_nr",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&recurring_id.0[..])
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup recurring_positions for {recurring_id}: {e}",
            )))?;

            if positions.len() != self.position_ids.len() {
                return Err(CommandError::ExecutionFailed(format!(
                    "RunRecurringOnce: template has {} positions but got {} ids",
                    positions.len(), self.position_ids.len(),
                )));
            }

            let nil_uuid = [0u8; 16];

            sqlx::query(
                "INSERT INTO invoices (tenant_id, id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&new_invoice_id.0[..])
            .bind(customer_id_bytes.as_slice())
            .bind(&self.new_number)
            .bind(&status)
            .bind(&self.issue_date)
            .bind(&self.due_date)
            .bind(&notes)
            .bind("invoice")
            .bind(&nil_uuid[..])
            .bind("")
            .bind(0_i64)
            .bind(0_i64)
            .bind(0_i64)
            .bind("transfer")
            .bind(&nil_uuid[..])
            .bind("EUR")
            .bind("de")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .bind("")
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT invoice {new_invoice_id}: {e}",
            )))?;

            for (i, pid) in self.position_ids.iter().enumerate() {
                let pos = &positions[i];
                sqlx::query(
                    "INSERT INTO positions (tenant_id, id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE id = id",
                )
                .bind(DEMO_TENANT_ID)
                .bind(&pid.0[..])
                .bind(&new_invoice_id.0[..])
                .bind((i as i64 + 1) * 1000)
                .bind(&pos.description)
                .bind(pos.quantity)
                .bind(pos.unit_price)
                .bind(pos.tax_rate)
                .bind(&nil_uuid[..])
                .bind(&pos.item_number)
                .bind(&pos.unit)
                .bind(pos.discount_pct)
                .bind(0_i64)
                .bind("service")
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT position {pid} for invoice {new_invoice_id}: {e}",
                )))?;
            }

            sqlx::query(
                "UPDATE recurring_invoices SET last_run = ?, next_run = ? WHERE tenant_id = ? AND id = ?",
            )
            .bind(&self.issue_date)
            .bind(&self.new_next_run)
            .bind(DEMO_TENANT_ID)
            .bind(&recurring_id.0[..])
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE recurring {recurring_id} last_run/next_run: {e}",
            )))?;

            Ok(client_zset.clone())
        }
    }
}
