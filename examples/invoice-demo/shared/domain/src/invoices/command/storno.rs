use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command;
use serde::{Deserialize, Serialize};
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use crate::shared::DEMO_TENANT_ID;

/// A position embedded in the Storno credit note (pre-negated quantity,
/// pre-assigned ID). Idempotent: re-applying the command inserts the same rows.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[ts(export_to = "../../../frontend/packages/generated/src/")]
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
        let mut acc = sql!(
            "UPDATE invoices SET status = 'cancelled' WHERE invoices.id = {self.id}"
        )
        .execute(db)?;

        // 2. Look up the original invoice number for the activity detail.
        let numbers = sql!(
            "SELECT invoices.number FROM invoices WHERE invoices.id = {self.id}"
        )
        .read_str_col(db)?;
        let number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&number, &self.credit_note_id);

        // 3. Insert the credit note header.
        acc.extend(
            sql!(
                "INSERT INTO invoices \
                 (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, \
                  service_date, cash_allowance_pct, cash_allowance_days, discount_pct, \
                  payment_method, sepa_mandate_id, currency, language, project_ref, external_id, \
                  billing_street, billing_zip, billing_city, billing_country, \
                  shipping_street, shipping_zip, shipping_city, shipping_country) \
                 VALUES \
                 ({self.credit_note_id}, {self.customer_id}, {self.credit_note_number}, 'draft', {self.date_issued}, {self.date_due}, {self.notes}, 'credit_note', {self.id}, \
                  {self.service_date}, {self.cash_allowance_pct}, {self.cash_allowance_days}, {self.discount_pct}, \
                  {self.payment_method}, {self.sepa_mandate_id}, {self.currency}, {self.language}, {self.project_ref}, {self.external_id}, \
                  {self.billing_street}, {self.billing_zip}, {self.billing_city}, {self.billing_country}, \
                  {self.shipping_street}, {self.shipping_zip}, {self.shipping_city}, {self.shipping_country})"
            )
            .execute(db)?,
        );

        // 4. Insert credit-note positions.
        for pos in &self.positions {
            acc.extend(
                sql!(
                    "INSERT INTO positions \
                     (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, \
                      product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES \
                     ({pos.id}, {self.credit_note_id}, {pos.position_nr}, {pos.description}, {pos.quantity}, {pos.unit_price}, {pos.tax_rate}, \
                      {pos.product_id}, {pos.item_number}, {pos.unit}, {pos.discount_pct}, {pos.cost_price}, {pos.position_type})"
                )
                .execute(db)?,
            );
        }

        // 5. Activity log on the original invoice.
        acc.extend(
            sql!(
                "INSERT INTO activity_log \
                 (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {self.id}, 'storno', 'demo', {detail})"
            )
            .execute(db)?,
        );

        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
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
            let model = invoice_entity::Entity::find()
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load invoice {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "invoice {} not found", self.id,
                )))?;

            let number = model.number.clone();
            let detail = detail_for(&number, &self.credit_note_id);

            let mut am: invoice_entity::ActiveModel = model.into();
            am.status = Set("cancelled".to_string());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} -> cancelled: {e}", self.id,
            )))?;

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
