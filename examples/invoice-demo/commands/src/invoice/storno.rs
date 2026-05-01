use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command;
use serde::{Deserialize, Serialize};
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use crate::helpers::{
    execute_sql, p_int, p_str, p_uuid, p_uuid_opt, read_str_col, DEMO_TENANT_ID,
};

/// A position embedded in the Storno credit note (pre-negated quantity,
/// pre-assigned ID). Idempotent: re-applying the command inserts the same rows.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct StornoPosition {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub position_nr: i64,
    pub description: String,
    #[ts(type = "number")]
    pub quantity: i64,
    #[ts(type = "number")]
    pub unit_price: i64,
    #[ts(type = "number")]
    pub tax_rate: i64,
    #[ts(type = "string | null")]
    pub product_id: Option<Uuid>,
    pub item_number: String,
    pub unit: String,
    #[ts(type = "number")]
    pub discount_pct: i64,
    #[ts(type = "number")]
    pub cost_price: i64,
    pub position_type: String,
}

/// Intent-Command: storno (cancel) an invoice and emit a mirror credit note
/// in a single atomic operation.
///
/// Replaces the old multi-step stream of `updateInvoiceHeader` + `createInvoice`
/// + N×`addPosition` + `logActivity`. The client wrapper pre-computes the credit
/// note UUID and positions so both the optimistic client apply and the
/// server-authoritative apply are deterministic and idempotent.
#[rpc_command]
pub struct Storno {
    /// Original invoice to cancel.
    #[ts(type = "string")]
    pub id: Uuid,

    /// Pre-assigned UUID for the new credit-note document.
    #[ts(type = "string")]
    pub credit_note_id: Uuid,

    // ── credit-note header fields (client computes these from peekInvoice) ──
    #[ts(type = "string | null")]
    pub customer_id: Option<Uuid>,
    pub credit_note_number: String,
    pub date_issued: String,
    pub date_due: String,
    pub notes: String,
    pub service_date: String,
    #[ts(type = "number")]
    pub cash_allowance_pct: i64,
    #[ts(type = "number")]
    pub cash_allowance_days: i64,
    #[ts(type = "number")]
    pub discount_pct: i64,
    pub payment_method: String,
    #[ts(type = "string | null")]
    pub sepa_mandate_id: Option<Uuid>,
    pub currency: String,
    pub language: String,
    pub project_ref: String,
    pub external_id: String,
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,

    /// Pre-negated positions for the credit note (IDs pre-assigned by client).
    pub positions: Vec<StornoPosition>,

    /// Activity log fields (shared PK keeps client + server inserts idempotent).
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(number: &str, credit_note_id: &Uuid) -> String {
    format!("\"{number}\" storniert, Gutschrift #{credit_note_id} erstellt")
}

impl Command for Storno {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        // 1. Cancel the original invoice.
        let mut acc = execute_sql(
            db,
            "UPDATE invoices SET status = 'cancelled' WHERE invoices.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;

        // 2. Look up the original invoice number for the activity detail.
        let numbers = read_str_col(
            db,
            "SELECT invoices.number FROM invoices WHERE invoices.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;
        let number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&number, &self.credit_note_id);

        // 3. Insert the credit note header.
        acc.extend(execute_sql(
            db,
            "INSERT INTO invoices \
             (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, \
              service_date, cash_allowance_pct, cash_allowance_days, discount_pct, \
              payment_method, sepa_mandate_id, currency, language, project_ref, external_id, \
              billing_street, billing_zip, billing_city, billing_country, \
              shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES \
             (:id, :customer_id, :number, 'draft', :date_issued, :date_due, :notes, 'credit_note', :parent_id, \
              :service_date, :cash_allowance_pct, :cash_allowance_days, :discount_pct, \
              :payment_method, :sepa_mandate_id, :currency, :language, :project_ref, :external_id, \
              :billing_street, :billing_zip, :billing_city, :billing_country, \
              :shipping_street, :shipping_zip, :shipping_city, :shipping_country)",
            Params::from([
                p_uuid("id", &self.credit_note_id),
                p_uuid_opt("customer_id", &self.customer_id),
                p_str("number", &self.credit_note_number),
                p_str("date_issued", &self.date_issued),
                p_str("date_due", &self.date_due),
                p_str("notes", &self.notes),
                p_uuid("parent_id", &self.id),
                p_str("service_date", &self.service_date),
                p_int("cash_allowance_pct", self.cash_allowance_pct),
                p_int("cash_allowance_days", self.cash_allowance_days),
                p_int("discount_pct", self.discount_pct),
                p_str("payment_method", &self.payment_method),
                p_uuid_opt("sepa_mandate_id", &self.sepa_mandate_id),
                p_str("currency", &self.currency),
                p_str("language", &self.language),
                p_str("project_ref", &self.project_ref),
                p_str("external_id", &self.external_id),
                p_str("billing_street", &self.billing_street),
                p_str("billing_zip", &self.billing_zip),
                p_str("billing_city", &self.billing_city),
                p_str("billing_country", &self.billing_country),
                p_str("shipping_street", &self.shipping_street),
                p_str("shipping_zip", &self.shipping_zip),
                p_str("shipping_city", &self.shipping_city),
                p_str("shipping_country", &self.shipping_country),
            ]),
        )?);

        // 4. Insert credit-note positions.
        for pos in &self.positions {
            acc.extend(execute_sql(
                db,
                "INSERT INTO positions \
                 (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, \
                  product_id, item_number, unit, discount_pct, cost_price, position_type) \
                 VALUES \
                 (:id, :invoice_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, \
                  :product_id, :item_number, :unit, :discount_pct, :cost_price, :position_type)",
                Params::from([
                    p_uuid("id", &pos.id),
                    p_uuid("invoice_id", &self.credit_note_id),
                    p_int("position_nr", pos.position_nr),
                    p_str("description", &pos.description),
                    p_int("quantity", pos.quantity),
                    p_int("unit_price", pos.unit_price),
                    p_int("tax_rate", pos.tax_rate),
                    p_uuid_opt("product_id", &pos.product_id),
                    p_str("item_number", &pos.item_number),
                    p_str("unit", &pos.unit),
                    p_int("discount_pct", pos.discount_pct),
                    p_int("cost_price", pos.cost_price),
                    p_str("position_type", &pos.position_type),
                ]),
            )?);
        }

        // 5. Activity log on the original invoice.
        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log \
             (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'invoice', :id, 'storno', 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("id", &self.id),
                p_str("detail", &detail),
            ]),
        )?);

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
    impl ServerCommand for Storno {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            // 1. Cancel original invoice.
            sqlx::query(
                "UPDATE invoices SET status = 'cancelled' \
                 WHERE invoices.tenant_id = ? AND invoices.id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} -> cancelled: {e}", self.id,
            )))?;

            // 2. Look up original invoice number for the activity detail.
            let number: String = sqlx::query_scalar(
                "SELECT number FROM invoices WHERE tenant_id = ? AND id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup number for invoice {}: {e}", self.id,
            )))?;
            let detail = detail_for(&number, &self.credit_note_id);

            // 3. Insert credit note header.
            sqlx::query(
                "INSERT INTO invoices \
                 (tenant_id, id, customer_id, number, status, date_issued, date_due, notes, \
                  doc_type, parent_id, service_date, \
                  cash_allowance_pct, cash_allowance_days, discount_pct, \
                  payment_method, sepa_mandate_id, currency, language, \
                  project_ref, external_id, \
                  billing_street, billing_zip, billing_city, billing_country, \
                  shipping_street, shipping_zip, shipping_city, shipping_country) \
                 VALUES (?, ?, ?, ?, 'draft', ?, ?, ?, 'credit_note', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.credit_note_id.0[..])
            .bind(self.customer_id.as_ref().map(|u| u.0.to_vec()))
            .bind(&self.credit_note_number)
            .bind(&self.date_issued)
            .bind(&self.date_due)
            .bind(&self.notes)
            .bind(&self.id.0[..])
            .bind(&self.service_date)
            .bind(self.cash_allowance_pct)
            .bind(self.cash_allowance_days)
            .bind(self.discount_pct)
            .bind(&self.payment_method)
            .bind(self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec()))
            .bind(&self.currency)
            .bind(&self.language)
            .bind(&self.project_ref)
            .bind(&self.external_id)
            .bind(&self.billing_street)
            .bind(&self.billing_zip)
            .bind(&self.billing_city)
            .bind(&self.billing_country)
            .bind(&self.shipping_street)
            .bind(&self.shipping_zip)
            .bind(&self.shipping_city)
            .bind(&self.shipping_country)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT credit note {}: {e}", self.credit_note_id,
            )))?;

            // 4. Insert credit-note positions.
            for pos in &self.positions {
                sqlx::query(
                    "INSERT INTO positions \
                     (tenant_id, id, invoice_id, position_nr, description, quantity, unit_price, \
                      tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE id = id",
                )
                .bind(DEMO_TENANT_ID)
                .bind(&pos.id.0[..])
                .bind(&self.credit_note_id.0[..])
                .bind(pos.position_nr)
                .bind(&pos.description)
                .bind(pos.quantity)
                .bind(pos.unit_price)
                .bind(pos.tax_rate)
                .bind(pos.product_id.as_ref().map(|u| u.0.to_vec()))
                .bind(&pos.item_number)
                .bind(&pos.unit)
                .bind(pos.discount_pct)
                .bind(pos.cost_price)
                .bind(&pos.position_type)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT storno position {}: {e}", pos.id,
                )))?;
            }

            // 5. Activity log.
            sqlx::query(
                "INSERT INTO activity_log \
                 (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'invoice', ?, 'storno', 'demo', ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.activity_id.0[..])
            .bind(&self.timestamp)
            .bind(&self.id.0[..])
            .bind(&detail)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT activity {}: {e}", self.activity_id,
            )))?;

            Ok(client_zset.clone())
        }
    }
}
