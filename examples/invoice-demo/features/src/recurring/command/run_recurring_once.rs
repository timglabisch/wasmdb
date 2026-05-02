use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::shared::DEMO_TENANT_ID;

fn activity_detail_for(template_name: &str, new_number: &str) -> String {
    format!("Serie \"{template_name}\" ausgeführt — Rechnung {new_number} erstellt")
}

/// Creates a new invoice with positions copied from the recurring template.
/// `position_ids` must have as many entries as the template has positions.
/// Updates last_run + next_run on the recurring row.
#[rpc_command]
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
    /// Pre-computed id for the activity_log row.
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
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

        let customer_id = sql!(
            "SELECT customer_id FROM recurring_invoices WHERE id = {recurring_id}"
        )
        .read_uuid_col(db)?
        .into_iter()
        .next()
        .ok_or_else(|| CommandError::ExecutionFailed(format!("recurring #{recurring_id} not found")))?;
        let status_templates = sql!(
            "SELECT status_template FROM recurring_invoices WHERE id = {recurring_id}"
        )
        .read_str_col(db)?;
        let notes_templates = sql!(
            "SELECT notes_template FROM recurring_invoices WHERE id = {recurring_id}"
        )
        .read_str_col(db)?;
        let template_names = sql!(
            "SELECT template_name FROM recurring_invoices WHERE id = {recurring_id}"
        )
        .read_str_col(db)?;
        let status = status_templates.into_iter().next().unwrap_or_else(|| "draft".into());
        let notes = notes_templates.into_iter().next().unwrap_or_default();
        let template_name = template_names.into_iter().next().unwrap_or_default();

        let descs = sql!(
            "SELECT description FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_str_col(db)?;
        let qtys = sql!(
            "SELECT quantity FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_i64_col(db)?;
        let prices = sql!(
            "SELECT unit_price FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_i64_col(db)?;
        let taxes = sql!(
            "SELECT tax_rate FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_i64_col(db)?;
        let units = sql!(
            "SELECT unit FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_str_col(db)?;
        let items = sql!(
            "SELECT item_number FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_str_col(db)?;
        let discounts = sql!(
            "SELECT discount_pct FROM recurring_positions WHERE recurring_id = {recurring_id} ORDER BY position_nr"
        )
        .read_i64_col(db)?;

        if descs.len() != position_ids.len() {
            return Err(CommandError::ExecutionFailed(format!(
                "RunRecurringOnce: template has {} positions but got {} ids",
                descs.len(), position_ids.len(),
            )));
        }

        let mut acc = ZSet::new();

        let some_customer: Option<Uuid> = Some(customer_id);
        let parent_id: Option<Uuid> = None;
        let sepa_mandate_id: Option<Uuid> = None;
        acc.extend(
            sql!(
                "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
                 VALUES ({new_invoice_id}, {some_customer}, {new_number}, {status}, {issue_date}, {due_date}, {notes}, 'invoice', {parent_id}, '', 0, 0, 0, 'transfer', {sepa_mandate_id}, 'EUR', 'de', '', '', '', '', '', '', '', '', '', '')"
            )
            .execute(db)?,
        );

        for (i, pid) in position_ids.iter().enumerate() {
            let position_nr = (i as i64 + 1) * 1000;
            let description = &descs[i];
            let quantity = qtys[i];
            let unit_price = prices[i];
            let tax_rate = taxes[i];
            let item_number = &items[i];
            let unit = &units[i];
            let discount_pct = discounts[i];
            let product_id: Option<Uuid> = None;
            acc.extend(
                sql!(
                    "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                     VALUES ({pid}, {new_invoice_id}, {position_nr}, {description}, {quantity}, {unit_price}, {tax_rate}, {product_id}, {item_number}, {unit}, {discount_pct}, 0, 'service')"
                )
                .execute(db)?,
            );
        }

        acc.extend(
            sql!(
                "UPDATE recurring_invoices SET last_run = {issue_date}, next_run = {new_next_run} WHERE recurring_invoices.id = {recurring_id}"
            )
            .execute(db)?,
        );

        let detail = activity_detail_for(&template_name, new_number);
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'recurring', {recurring_id}, 'run', 'demo', {detail})"
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
    use sea_orm::{
        ActiveModelTrait, ColumnTrait, DatabaseTransaction,
        EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    };
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;
    use crate::positions::position_server::entity as position_entity;
    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;
    use crate::recurring::recurring_position_server::entity as recurring_position_entity;

    #[async_trait]
    impl ServerCommand for RunRecurringOnce {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let recurring_id = self.recurring_id;
            let new_invoice_id = self.new_invoice_id;

            let (customer_id_bytes, status_template, notes_template, template_name): (Vec<u8>, String, String, String) =
                recurring_invoice_entity::Entity::find()
                    .select_only()
                    .column(recurring_invoice_entity::Column::CustomerId)
                    .column(recurring_invoice_entity::Column::StatusTemplate)
                    .column(recurring_invoice_entity::Column::NotesTemplate)
                    .column(recurring_invoice_entity::Column::TemplateName)
                    .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_invoice_entity::Column::Id.eq(recurring_id.0.to_vec()))
                    .into_tuple()
                    .one(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "lookup recurring_invoice {recurring_id}: {e}",
                    )))?
                    .ok_or_else(|| CommandError::ExecutionFailed(format!(
                        "recurring #{recurring_id} not found",
                    )))?;

            let status = if status_template.is_empty() { "draft".to_string() } else { status_template };
            let notes = notes_template;

            let positions: Vec<recurring_position_entity::Model> =
                recurring_position_entity::Entity::find()
                    .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_position_entity::Column::RecurringId.eq(recurring_id.0.to_vec()))
                    .order_by_asc(recurring_position_entity::Column::PositionNr)
                    .all(tx)
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

            let am = invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(new_invoice_id.0.to_vec()),
                customer_id: Set(Some(customer_id_bytes)),
                number: Set(self.new_number.clone()),
                status: Set(status),
                date_issued: Set(self.issue_date.clone()),
                date_due: Set(self.due_date.clone()),
                notes: Set(notes),
                doc_type: Set("invoice".to_string()),
                parent_id: Set(None),
                service_date: Set("".to_string()),
                cash_allowance_pct: Set(0_i64),
                cash_allowance_days: Set(0_i64),
                discount_pct: Set(0_i64),
                payment_method: Set("transfer".to_string()),
                sepa_mandate_id: Set(None),
                currency: Set("EUR".to_string()),
                language: Set("de".to_string()),
                project_ref: Set("".to_string()),
                external_id: Set("".to_string()),
                billing_street: Set("".to_string()),
                billing_zip: Set("".to_string()),
                billing_city: Set("".to_string()),
                billing_country: Set("".to_string()),
                shipping_street: Set("".to_string()),
                shipping_zip: Set("".to_string()),
                shipping_city: Set("".to_string()),
                shipping_country: Set("".to_string()),
            };
            invoice_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT invoice {new_invoice_id}: {e}",
                )))?;

            for (i, pid) in self.position_ids.iter().enumerate() {
                let pos = &positions[i];
                let pam = position_entity::ActiveModel {
                    tenant_id: Set(DEMO_TENANT_ID),
                    id: Set(pid.0.to_vec()),
                    invoice_id: Set(new_invoice_id.0.to_vec()),
                    position_nr: Set((i as i64 + 1) * 1000),
                    description: Set(pos.description.clone()),
                    quantity: Set(pos.quantity),
                    unit_price: Set(pos.unit_price),
                    tax_rate: Set(pos.tax_rate),
                    product_id: Set(None),
                    item_number: Set(pos.item_number.clone()),
                    unit: Set(pos.unit.clone()),
                    discount_pct: Set(pos.discount_pct),
                    cost_price: Set(0_i64),
                    position_type: Set("service".to_string()),
                };
                position_entity::Entity::insert(pam)
                    .on_conflict_do_nothing()
                    .exec_without_returning(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "INSERT position {pid} for invoice {new_invoice_id}: {e}",
                    )))?;
            }

            let template = recurring_invoice_entity::Entity::find()
                .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_invoice_entity::Column::Id.eq(recurring_id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load recurring template {recurring_id}: {e}",
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "recurring template {recurring_id} not found",
                )))?;
            let mut tam: recurring_invoice_entity::ActiveModel = template.into();
            tam.last_run = Set(self.issue_date.clone());
            tam.next_run = Set(self.new_next_run.clone());
            tam.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE recurring template {recurring_id}: {e}",
            )))?;

            let detail = activity_detail_for(&template_name, &self.new_number);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "recurring",
                &recurring_id,
                "run",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
