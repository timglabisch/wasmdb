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
