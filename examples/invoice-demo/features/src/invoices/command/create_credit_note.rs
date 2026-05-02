use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::invoices::Invoice;
use crate::positions::Position;
use crate::shared::DEMO_TENANT_ID;

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

        // Read the full source row. SELECT column order matches the field
        // order in `Invoice` (the `#[row]` schema), which is what
        // `#[derive(FromRow)]` consumes.
        let hdr: Invoice = sql!(
            "SELECT id, customer_id, number, status, date_issued, date_due, \
                    notes, doc_type, parent_id, service_date, \
                    cash_allowance_pct, cash_allowance_days, discount_pct, \
                    payment_method, sepa_mandate_id, currency, language, \
                    project_ref, external_id, \
                    billing_street, billing_zip, billing_city, billing_country, \
                    shipping_street, shipping_zip, shipping_city, shipping_country \
             FROM invoices WHERE id = {src}"
        )
        .read_row(db)?
        .ok_or_else(|| {
            CommandError::ExecutionFailed(format!("source invoice {src} not found"))
        })?;

        let detail = detail_for(&hdr.number, &src, &new_id);
        let parent_id: Option<Uuid> = Some(src);

        let mut acc = sql!(
            "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES ({new_id}, {hdr.customer_id}, {self.new_number}, 'draft', {self.date_issued}, {self.date_due}, {hdr.notes}, 'credit_note', {parent_id}, {hdr.service_date}, {hdr.cash_allowance_pct}, {hdr.cash_allowance_days}, {hdr.discount_pct}, {hdr.payment_method}, {hdr.sepa_mandate_id}, {hdr.currency}, {hdr.language}, {hdr.project_ref}, {hdr.external_id}, {hdr.billing_street}, {hdr.billing_zip}, {hdr.billing_city}, {hdr.billing_country}, {hdr.shipping_street}, {hdr.shipping_zip}, {hdr.shipping_city}, {hdr.shipping_country})"
        )
        .execute(db)?;

        let positions: Vec<Position> = sql!(
            "SELECT id, invoice_id, position_nr, description, quantity, \
                    unit_price, tax_rate, product_id, item_number, unit, \
                    discount_pct, cost_price, position_type \
             FROM positions WHERE invoice_id = {src} ORDER BY position_nr"
        )
        .read_rows(db)?;

        if positions.len() != self.new_position_ids.len() {
            return Err(CommandError::ExecutionFailed(format!(
                "CreateCreditNote: source has {} positions but got {} ids",
                positions.len(),
                self.new_position_ids.len(),
            )));
        }

        let product_id: Option<Uuid> = None;
        for (pid, pos) in self.new_position_ids.iter().zip(positions.iter()) {
            let quantity = -pos.quantity; // negated for credit note

            acc.extend(
                sql!(
                    "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES ({pid}, {new_id}, {pos.position_nr}, {pos.description}, {quantity}, {pos.unit_price}, {pos.tax_rate}, {product_id}, {pos.item_number}, {pos.unit}, {pos.discount_pct}, {pos.cost_price}, {pos.position_type})"
                )
                .execute(db)?,
            );
        }

        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {new_id}, 'credit_note_created', 'demo', {detail})"
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;
    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for CreateCreditNote {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let src = self.source_invoice_id;
            let new_id = self.new_invoice_id;

            // --- read source invoice header ---
            let hdr = invoice_entity::Entity::find()
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(src.0.to_vec()))
                .one(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup source invoice {src}: {e}",
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "source invoice {src} not found",
                )))?;

            let detail = detail_for(&hdr.number, &src, &new_id);

            // --- insert new credit-note invoice ---
            let am = invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(new_id.0.to_vec()),
                customer_id: Set(hdr.customer_id.clone()),
                number: Set(self.new_number.clone()),
                status: Set("draft".to_string()),
                date_issued: Set(self.date_issued.clone()),
                date_due: Set(self.date_due.clone()),
                notes: Set(hdr.notes.clone()),
                doc_type: Set("credit_note".to_string()),
                parent_id: Set(Some(src.0.to_vec())),
                service_date: Set(hdr.service_date.clone()),
                cash_allowance_pct: Set(hdr.cash_allowance_pct),
                cash_allowance_days: Set(hdr.cash_allowance_days),
                discount_pct: Set(hdr.discount_pct),
                payment_method: Set(hdr.payment_method.clone()),
                sepa_mandate_id: Set(hdr.sepa_mandate_id.clone()),
                currency: Set(hdr.currency.clone()),
                language: Set(hdr.language.clone()),
                project_ref: Set(hdr.project_ref.clone()),
                external_id: Set(hdr.external_id.clone()),
                billing_street: Set(hdr.billing_street.clone()),
                billing_zip: Set(hdr.billing_zip.clone()),
                billing_city: Set(hdr.billing_city.clone()),
                billing_country: Set(hdr.billing_country.clone()),
                shipping_street: Set(hdr.shipping_street.clone()),
                shipping_zip: Set(hdr.shipping_zip.clone()),
                shipping_city: Set(hdr.shipping_city.clone()),
                shipping_country: Set(hdr.shipping_country.clone()),
            };
            invoice_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT credit note invoice {new_id}: {e}",
                )))?;

            // --- read source positions ---
            let positions = position_entity::Entity::find()
                .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(position_entity::Column::InvoiceId.eq(src.0.to_vec()))
                .order_by_asc(position_entity::Column::PositionNr)
                .all(tx)
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
                let pam = position_entity::ActiveModel {
                    tenant_id: Set(DEMO_TENANT_ID),
                    id: Set(pid.0.to_vec()),
                    invoice_id: Set(new_id.0.to_vec()),
                    position_nr: Set(pos.position_nr),
                    description: Set(pos.description.clone()),
                    quantity: Set(-pos.quantity), // negated for credit note
                    unit_price: Set(pos.unit_price),
                    tax_rate: Set(pos.tax_rate),
                    product_id: Set(None),
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
                        "INSERT position {pid} for credit note {new_id}: {e}",
                    )))?;
            }

            // --- activity row ---
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &new_id,
                "credit_note_created",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
