use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::{sql, FromRow};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

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

/// Header columns we copy from the source invoice into the new credit-note
/// row. SELECT order below MUST match field order.
#[derive(FromRow)]
struct InvoiceHdr {
    number: String,
    customer_id: Option<Uuid>,
    notes: String,
    service_date: String,
    cash_allowance_pct: i64,
    cash_allowance_days: i64,
    discount_pct: i64,
    payment_method: String,
    sepa_mandate_id: Option<Uuid>,
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

/// Position columns we copy (with negated quantity) from the source.
#[derive(FromRow)]
struct PositionRow {
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

impl Command for CreateCreditNote {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let src = self.source_invoice_id;
        let new_id = self.new_invoice_id;

        let hdr: InvoiceHdr = sql!(
            "SELECT number, customer_id, notes, service_date, \
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
             VALUES ({new_id}, {customer_id}, {self.new_number}, 'draft', {self.date_issued}, {self.date_due}, {notes}, 'credit_note', {parent_id}, {service_date}, {cash_pct}, {cash_days}, {discount_pct}, {payment_method}, {sepa_mandate_id}, {currency}, {language}, {project_ref}, {external_id}, {billing_street}, {billing_zip}, {billing_city}, {billing_country}, {shipping_street}, {shipping_zip}, {shipping_city}, {shipping_country})",
            customer_id      = hdr.customer_id,
            notes            = hdr.notes,
            service_date     = hdr.service_date,
            cash_pct         = hdr.cash_allowance_pct,
            cash_days        = hdr.cash_allowance_days,
            discount_pct     = hdr.discount_pct,
            payment_method   = hdr.payment_method,
            sepa_mandate_id  = hdr.sepa_mandate_id,
            currency         = hdr.currency,
            language         = hdr.language,
            project_ref      = hdr.project_ref,
            external_id      = hdr.external_id,
            billing_street   = hdr.billing_street,
            billing_zip      = hdr.billing_zip,
            billing_city     = hdr.billing_city,
            billing_country  = hdr.billing_country,
            shipping_street  = hdr.shipping_street,
            shipping_zip     = hdr.shipping_zip,
            shipping_city    = hdr.shipping_city,
            shipping_country = hdr.shipping_country
        )
        .execute(db)?;

        let positions: Vec<PositionRow> = sql!(
            "SELECT position_nr, description, quantity, unit_price, tax_rate, \
                    item_number, unit, discount_pct, cost_price, position_type \
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

        for (pid, pos) in self.new_position_ids.iter().zip(positions.iter()) {
            let product_id: Option<Uuid> = None;
            let position_nr = pos.position_nr;
            let description = &pos.description;
            let quantity = -pos.quantity; // negated for credit note
            let unit_price = pos.unit_price;
            let tax_rate = pos.tax_rate;
            let item_number = &pos.item_number;
            let unit = &pos.unit;
            let discount_pct = pos.discount_pct;
            let cost_price = pos.cost_price;
            let position_type = &pos.position_type;

            acc.extend(
                sql!(
                    "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES ({pid}, {new_id}, {position_nr}, {description}, {quantity}, {unit_price}, {tax_rate}, {product_id}, {item_number}, {unit}, {discount_pct}, {cost_price}, {position_type})"
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
