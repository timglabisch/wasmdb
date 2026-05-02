use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

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
        let detail = detail_for(&self.name);

        let recurring_ids = sql!(
            "SELECT id FROM recurring_invoices WHERE customer_id = {self.id}"
        )
        .read_uuid_col(db)?;
        let invoice_ids = sql!(
            "SELECT id FROM invoices WHERE customer_id = {self.id}"
        )
        .read_uuid_col(db)?;

        let mut acc = ZSet::new();

        if !recurring_ids.is_empty() {
            let rids: Vec<[u8; 16]> = recurring_ids.iter().map(|u| u.0).collect();
            acc.extend(
                sql!("DELETE FROM recurring_positions WHERE recurring_id IN ({rids})", rids = rids)
                    .execute(db)?,
            );
            acc.extend(
                sql!("DELETE FROM recurring_invoices WHERE id IN ({rids})", rids = rids)
                    .execute(db)?,
            );
        }

        if !invoice_ids.is_empty() {
            let iids: Vec<[u8; 16]> = invoice_ids.iter().map(|u| u.0).collect();
            acc.extend(
                sql!("DELETE FROM payments WHERE invoice_id IN ({iids})", iids = iids)
                    .execute(db)?,
            );
            acc.extend(
                sql!("DELETE FROM positions WHERE invoice_id IN ({iids})", iids = iids)
                    .execute(db)?,
            );
            acc.extend(
                sql!("DELETE FROM invoices WHERE id IN ({iids})", iids = iids)
                    .execute(db)?,
            );
        }

        acc.extend(
            sql!("DELETE FROM sepa_mandates WHERE customer_id = {self.id}").execute(db)?,
        );
        acc.extend(
            sql!("DELETE FROM contacts WHERE customer_id = {self.id}").execute(db)?,
        );
        acc.extend(
            sql!("DELETE FROM customers WHERE id = {self.id}").execute(db)?,
        );

        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'customer', {self.id}, 'delete', 'demo', {detail})"
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter, QuerySelect};
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
                let recurring_positions: Vec<recurring_position_entity::Model> = recurring_position_entity::Entity::find()
                    .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_position_entity::Column::RecurringId.is_in(recurring_ids.clone()))
                    .all(tx).await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "load recurring_positions for customer {id}: {e}",
                    )))?;
                for rp in recurring_positions {
                    rp.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE recurring_position for customer {id}: {e}",
                    )))?;
                }

                let recurring_invoices: Vec<recurring_invoice_entity::Model> = recurring_invoice_entity::Entity::find()
                    .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(recurring_invoice_entity::Column::Id.is_in(recurring_ids))
                    .all(tx).await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "load recurring_invoices for customer {id}: {e}",
                    )))?;
                for ri in recurring_invoices {
                    ri.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE recurring_invoice for customer {id}: {e}",
                    )))?;
                }
            }

            if !invoice_ids.is_empty() {
                let payments: Vec<payment_entity::Model> = payment_entity::Entity::find()
                    .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(payment_entity::Column::InvoiceId.is_in(invoice_ids.clone()))
                    .all(tx).await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "load payments for customer {id}: {e}",
                    )))?;
                for payment in payments {
                    payment.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE payment for customer {id}: {e}",
                    )))?;
                }

                let positions: Vec<position_entity::Model> = position_entity::Entity::find()
                    .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(position_entity::Column::InvoiceId.is_in(invoice_ids.clone()))
                    .all(tx).await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "load positions for customer {id}: {e}",
                    )))?;
                for pos in positions {
                    pos.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE position for customer {id}: {e}",
                    )))?;
                }

                let invoices: Vec<invoice_entity::Model> = invoice_entity::Entity::find()
                    .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                    .filter(invoice_entity::Column::Id.is_in(invoice_ids))
                    .all(tx).await
                    .map_err(|e| CommandError::ExecutionFailed(format!(
                        "load invoices for customer {id}: {e}",
                    )))?;
                for invoice in invoices {
                    invoice.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                        "DELETE invoice for customer {id}: {e}",
                    )))?;
                }
            }

            let sepa_mandates: Vec<sepa_mandate_entity::Model> = sepa_mandate_entity::Entity::find()
                .filter(sepa_mandate_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(sepa_mandate_entity::Column::CustomerId.eq(id_bytes.clone()))
                .all(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load sepa_mandates for customer {id}: {e}",
                )))?;
            for sm in sepa_mandates {
                sm.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandate for customer {id}: {e}",
                )))?;
            }

            let contacts: Vec<contact_entity::Model> = contact_entity::Entity::find()
                .filter(contact_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(contact_entity::Column::CustomerId.eq(id_bytes.clone()))
                .all(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load contacts for customer {id}: {e}",
                )))?;
            for contact in contacts {
                contact.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE contact for customer {id}: {e}",
                )))?;
            }

            let customer = customer_entity::Entity::find()
                .filter(customer_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(customer_entity::Column::Id.eq(id_bytes))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load customer {id}: {e}",
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "customer {id} not found",
                )))?;
            customer.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
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
