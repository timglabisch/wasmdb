use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::shared::DEMO_TENANT_ID;

/// Intent-Command: duplicate an existing invoice (copy header + all positions)
/// into a new draft, and emit an activity row in the same atomic step.
///
/// The client pre-computes all UUIDs (`new_invoice_id`, one `new_position_id`
/// per source position, `activity_id`) so optimistic and server-confirmed
/// inserts share the same primary keys (idempotent re-apply).
#[rpc_command]
pub struct DuplicateInvoice {
    /// The source invoice to copy from.
    #[ts(type = "string")]
    pub source_invoice_id: Uuid,
    /// Pre-computed id for the new invoice.
    #[ts(type = "string")]
    pub new_invoice_id: Uuid,
    /// Pre-computed ids for the new positions, one per source position in
    /// `position_nr` order. The client must supply exactly as many ids as
    /// the source invoice has positions.
    #[ts(type = "string[]")]
    pub new_position_ids: Vec<Uuid>,
    /// The number string for the new invoice (e.g. `"<source>-KOPIE"`).
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

fn detail_for(source_number: &str, new_number: &str) -> String {
    format!("Kopie von \"{source_number}\" als {new_number} angelegt")
}

impl Command for DuplicateInvoice {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let src = self.source_invoice_id;
        let new_id = self.new_invoice_id;

        // --- read source invoice header ---
        let numbers = sql!("SELECT invoices.number FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?;
        let src_number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&src_number, &self.new_number);

        // Read all fields needed to reproduce the header (same columns as CreateInvoice).
        let customer_id: Option<Uuid> =
            sql!("SELECT invoices.customer_id FROM invoices WHERE invoices.id = {src}")
                .read_uuid_col(db)?
                .into_iter()
                .next();

        let notes           = sql!("SELECT invoices.notes FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let doc_type        = sql!("SELECT invoices.doc_type FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let service_date    = sql!("SELECT invoices.service_date FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let cash_pct        = sql!("SELECT invoices.cash_allowance_pct FROM invoices WHERE invoices.id = {src}")
            .read_i64_col(db)?.into_iter().next().unwrap_or_default();
        let cash_days       = sql!("SELECT invoices.cash_allowance_days FROM invoices WHERE invoices.id = {src}")
            .read_i64_col(db)?.into_iter().next().unwrap_or_default();
        let discount_pct    = sql!("SELECT invoices.discount_pct FROM invoices WHERE invoices.id = {src}")
            .read_i64_col(db)?.into_iter().next().unwrap_or_default();
        let payment_method  = sql!("SELECT invoices.payment_method FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let currency        = sql!("SELECT invoices.currency FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let language        = sql!("SELECT invoices.language FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let project_ref     = sql!("SELECT invoices.project_ref FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let external_id     = sql!("SELECT invoices.external_id FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let billing_street  = sql!("SELECT invoices.billing_street FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let billing_zip     = sql!("SELECT invoices.billing_zip FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let billing_city    = sql!("SELECT invoices.billing_city FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let billing_country = sql!("SELECT invoices.billing_country FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let shipping_street  = sql!("SELECT invoices.shipping_street FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let shipping_zip     = sql!("SELECT invoices.shipping_zip FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let shipping_city    = sql!("SELECT invoices.shipping_city FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();
        let shipping_country = sql!("SELECT invoices.shipping_country FROM invoices WHERE invoices.id = {src}")
            .read_str_col(db)?.into_iter().next().unwrap_or_default();

        // sepa_mandate_id is nullable uuid
        let sepa_mandate_id: Option<Uuid> =
            sql!("SELECT invoices.sepa_mandate_id FROM invoices WHERE invoices.id = {src}")
                .read_uuid_col(db)?
                .into_iter()
                .next();

        // new invoice is always a 'draft', parent_id = null
        let parent_id: Option<Uuid> = None;

        let mut acc = sql!(
            "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES ({new_id}, {customer_id}, {self.new_number}, 'draft', {self.date_issued}, {self.date_due}, {notes}, {doc_type}, {parent_id}, {service_date}, {cash_pct}, {cash_days}, {discount_pct}, {payment_method}, {sepa_mandate_id}, {currency}, {language}, {project_ref}, {external_id}, {billing_street}, {billing_zip}, {billing_city}, {billing_country}, {shipping_street}, {shipping_zip}, {shipping_city}, {shipping_country})"
        )
        .execute(db)?;

        // --- copy positions ---
        let descs     = sql!("SELECT description FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_str_col(db)?;
        let qtys      = sql!("SELECT quantity FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;
        let prices    = sql!("SELECT unit_price FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;
        let taxes     = sql!("SELECT tax_rate FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;
        let items     = sql!("SELECT item_number FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_str_col(db)?;
        let units     = sql!("SELECT unit FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_str_col(db)?;
        let discounts = sql!("SELECT discount_pct FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;
        let costs     = sql!("SELECT cost_price FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;
        let pos_types = sql!("SELECT position_type FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_str_col(db)?;
        let pos_nrs   = sql!("SELECT position_nr FROM positions WHERE positions.invoice_id = {src} ORDER BY positions.position_nr")
            .read_i64_col(db)?;

        if descs.len() != self.new_position_ids.len() {
            return Err(CommandError::ExecutionFailed(format!(
                "DuplicateInvoice: source has {} positions but got {} ids",
                descs.len(), self.new_position_ids.len(),
            )));
        }

        for (i, pid) in self.new_position_ids.iter().enumerate() {
            let position_nr = pos_nrs.get(i).copied().unwrap_or((i as i64 + 1) * 1000);
            let description = &descs[i];
            let quantity = qtys[i];
            let unit_price = prices[i];
            let tax_rate = taxes[i];
            let product_id: Option<Uuid> = None;
            let item_number = items.get(i).map(|s| s.as_str()).unwrap_or("");
            let unit = units.get(i).map(|s| s.as_str()).unwrap_or("");
            let discount_pct = discounts.get(i).copied().unwrap_or(0);
            let cost_price = costs.get(i).copied().unwrap_or(0);
            let position_type = pos_types.get(i).map(|s| s.as_str()).unwrap_or("service");

            acc.extend(
                sql!(
                    "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES ({pid}, {new_id}, {position_nr}, {description}, {quantity}, {unit_price}, {tax_rate}, {product_id}, {item_number}, {unit}, {discount_pct}, {cost_price}, {position_type})"
                )
                .execute(db)?,
            );
        }

        // --- activity row ---
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {new_id}, 'duplicate_from', 'demo', {detail})"
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
    impl ServerCommand for DuplicateInvoice {
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

            let detail = detail_for(&hdr.number, &self.new_number);

            // --- insert new invoice ---
            let am = invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(new_id.0.to_vec()),
                customer_id: Set(hdr.customer_id.clone()),
                number: Set(self.new_number.clone()),
                status: Set("draft".to_string()),
                date_issued: Set(self.date_issued.clone()),
                date_due: Set(self.date_due.clone()),
                notes: Set(hdr.notes.clone()),
                doc_type: Set(hdr.doc_type.clone()),
                parent_id: Set(None),
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
                    "INSERT duplicate invoice {new_id}: {e}",
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
                    "DuplicateInvoice: source has {} positions but got {} ids",
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
                    quantity: Set(pos.quantity),
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
                        "INSERT position {pid} for duplicate invoice {new_id}: {e}",
                    )))?;
            }

            // --- activity row ---
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &new_id,
                "duplicate_from",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
