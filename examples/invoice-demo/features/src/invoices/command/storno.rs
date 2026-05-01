use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command;
use serde::{Deserialize, Serialize};
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use crate::command_helpers::{
    execute_sql, p_int, p_str, p_uuid, p_uuid_opt, read_str_col,
};
use crate::shared::DEMO_TENANT_ID;

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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;
    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for Storno {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            // 1. Cancel original invoice.
            invoice_entity::Entity::update_many()
                .col_expr(invoice_entity::Column::Status, "cancelled".to_string().into())
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE invoice {} -> cancelled: {e}", self.id,
                )))?;

            // 2. Look up original invoice number for the activity detail.
            let number: String = invoice_entity::Entity::find()
                .select_only()
                .column(invoice_entity::Column::Number)
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .into_tuple()
                .one(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup number for invoice {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "invoice {} not found", self.id,
                )))?;
            let detail = detail_for(&number, &self.credit_note_id);

            // 3. Insert credit note header.
            let am = invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.credit_note_id.0.to_vec()),
                customer_id: Set(self.customer_id.as_ref().map(|u| u.0.to_vec())),
                number: Set(self.credit_note_number.clone()),
                status: Set("draft".to_string()),
                date_issued: Set(self.date_issued.clone()),
                date_due: Set(self.date_due.clone()),
                notes: Set(self.notes.clone()),
                doc_type: Set("credit_note".to_string()),
                parent_id: Set(Some(self.id.0.to_vec())),
                service_date: Set(self.service_date.clone()),
                cash_allowance_pct: Set(self.cash_allowance_pct),
                cash_allowance_days: Set(self.cash_allowance_days),
                discount_pct: Set(self.discount_pct),
                payment_method: Set(self.payment_method.clone()),
                sepa_mandate_id: Set(self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec())),
                currency: Set(self.currency.clone()),
                language: Set(self.language.clone()),
                project_ref: Set(self.project_ref.clone()),
                external_id: Set(self.external_id.clone()),
                billing_street: Set(self.billing_street.clone()),
                billing_zip: Set(self.billing_zip.clone()),
                billing_city: Set(self.billing_city.clone()),
                billing_country: Set(self.billing_country.clone()),
                shipping_street: Set(self.shipping_street.clone()),
                shipping_zip: Set(self.shipping_zip.clone()),
                shipping_city: Set(self.shipping_city.clone()),
                shipping_country: Set(self.shipping_country.clone()),
            };
            invoice_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT credit note {}: {e}", self.credit_note_id,
                )))?;

            // 4. Insert credit-note positions.
            for pos in &self.positions {
                let pam = position_entity::ActiveModel {
                    tenant_id: Set(DEMO_TENANT_ID),
                    id: Set(pos.id.0.to_vec()),
                    invoice_id: Set(self.credit_note_id.0.to_vec()),
                    position_nr: Set(pos.position_nr),
                    description: Set(pos.description.clone()),
                    quantity: Set(pos.quantity),
                    unit_price: Set(pos.unit_price),
                    tax_rate: Set(pos.tax_rate),
                    product_id: Set(pos.product_id.as_ref().map(|u| u.0.to_vec())),
                    item_number: Set(pos.item_number.clone()),
                    unit: Set(pos.unit.clone()),
                    discount_pct: Set(pos.discount_pct),
                    cost_price: Set(pos.cost_price),
                    position_type: Set(pos.position_type.clone()),
                };
                position_entity::Entity::insert(pam)
                    .on_conflict_do_nothing()
                    .exec_without_returning(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "INSERT storno position {}: {e}", pos.id,
                    )))?;
            }

            // 5. Activity log.
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "storno",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
