use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::{Params, ParamValue};
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_str, p_uuid, read_uuid_col};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct DeleteCustomerCascade {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
    pub name: String,
}

fn detail_for(name: &str) -> String {
    format!("Kunde \"{name}\" gelöscht (Kaskade)")
}

impl Command for DeleteCustomerCascade {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let detail = detail_for(&self.name);

        let recurring_ids = read_uuid_col(db,
            "SELECT id FROM recurring_invoices WHERE customer_id = :cid",
            Params::from([p_uuid("cid", &id)]))?;
        let invoice_ids = read_uuid_col(db,
            "SELECT id FROM invoices WHERE customer_id = :cid",
            Params::from([p_uuid("cid", &id)]))?;

        let mut acc = ZSet::new();

        if !recurring_ids.is_empty() {
            let bytes: Vec<[u8; 16]> = recurring_ids.iter().map(|u| u.0).collect();
            let p = Params::from([
                ("rids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_positions WHERE recurring_id IN (:rids)", p)?);
            let p = Params::from([
                ("rids".into(), ParamValue::UuidList(bytes)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_invoices WHERE id IN (:rids)", p)?);
        }

        if !invoice_ids.is_empty() {
            let bytes: Vec<[u8; 16]> = invoice_ids.iter().map(|u| u.0).collect();
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM payments WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM positions WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM invoices WHERE id IN (:iids)", p)?);
        }

        let p = Params::from([p_uuid("cid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM sepa_mandates WHERE customer_id = :cid", p)?);
        let p = Params::from([p_uuid("cid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM contacts WHERE customer_id = :cid", p)?);
        let p = Params::from([p_uuid("id", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM customers WHERE id = :id", p)?);

        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'customer', :id, 'delete', 'demo', :detail)",
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::contacts::contact_server::entity as contact_entity;
    use crate::customers::customer_server::entity as customer_entity;
    use crate::invoices::invoice_server::entity as invoice_entity;
    use crate::payments::payment_server::entity as payment_entity;
    use crate::positions::position_server::entity as position_entity;
    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;
    use crate::recurring::recurring_position_server::entity as recurring_position_entity;
    use crate::sepa_mandates::sepa_mandate_server::entity as sepa_mandate_entity;

    #[async_trait]
    impl ServerCommand for DeleteCustomerCascade {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let id = self.id;
            let id_bytes = id.0.to_vec();

            let recurring_ids: Vec<Vec<u8>> = recurring_invoice_entity::Entity::find()
                .select_only()
                .column(recurring_invoice_entity::Column::Id)
                .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_invoice_entity::Column::CustomerId.eq(id_bytes.clone()))
                .into_tuple()
                .all(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup recurring_invoices for customer {id}: {e}",
                )))?;

            let invoice_ids: Vec<Vec<u8>> = invoice_entity::Entity::find()
                .select_only()
                .column(invoice_entity::Column::Id)
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::CustomerId.eq(id_bytes.clone()))
                .into_tuple()
                .all(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup invoices for customer {id}: {e}",
                )))?;

            if !recurring_ids.is_empty() {
                recurring_position_entity::Entity::delete_many()
                    .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_position_entity::Column::RecurringId.is_in(recurring_ids.clone()))
                    .exec(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE recurring_positions for customer {id}: {e}",
                    )))?;

                recurring_invoice_entity::Entity::delete_many()
                    .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_invoice_entity::Column::Id.is_in(recurring_ids))
                    .exec(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE recurring_invoices for customer {id}: {e}",
                    )))?;
            }

            if !invoice_ids.is_empty() {
                payment_entity::Entity::delete_many()
                    .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(payment_entity::Column::InvoiceId.is_in(invoice_ids.clone()))
                    .exec(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE payments for customer {id}: {e}",
                    )))?;

                position_entity::Entity::delete_many()
                    .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(position_entity::Column::InvoiceId.is_in(invoice_ids.clone()))
                    .exec(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE positions for customer {id}: {e}",
                    )))?;

                invoice_entity::Entity::delete_many()
                    .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(invoice_entity::Column::Id.is_in(invoice_ids))
                    .exec(tx)
                    .await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE invoices for customer {id}: {e}",
                    )))?;
            }

            sepa_mandate_entity::Entity::delete_many()
                .filter(sepa_mandate_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(sepa_mandate_entity::Column::CustomerId.eq(id_bytes.clone()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandates for customer {id}: {e}",
                )))?;

            contact_entity::Entity::delete_many()
                .filter(contact_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(contact_entity::Column::CustomerId.eq(id_bytes.clone()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE contacts for customer {id}: {e}",
                )))?;

            customer_entity::Entity::delete_many()
                .filter(customer_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(customer_entity::Column::Id.eq(id_bytes))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE customer {id}: {e}",
                )))?;

            let detail = detail_for(&self.name);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "customer",
                &self.id,
                "delete",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
