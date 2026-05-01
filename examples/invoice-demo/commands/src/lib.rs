use borsh::{BorshSerialize, BorshDeserialize};
use database::Database;
use rpc_command::rpc_command_enum;
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

mod helpers;
pub mod customer;
pub mod contact;
pub mod invoice;
pub mod position;
pub mod payment;
pub mod product;
pub mod sepa;
pub mod recurring;
pub mod activity;

use customer::create::CreateCustomer;
use customer::update::UpdateCustomer;
use customer::delete::DeleteCustomer;
use customer::delete_cascade::DeleteCustomerCascade;
use contact::create::CreateContact;
use contact::update::UpdateContact;
use contact::delete::DeleteContact;
use invoice::create::CreateInvoice;
use invoice::update_header::UpdateInvoiceHeader;
use invoice::delete::DeleteInvoice;
use invoice::mark_paid::MarkPaid;
use invoice::mark_sent::MarkSent;
use invoice::storno::Storno;
use invoice::convert_offer_to_invoice::ConvertOfferToInvoice;
use invoice::assign_customer::AssignCustomer;
use invoice::duplicate_invoice::DuplicateInvoice;
use invoice::create_credit_note::CreateCreditNote;
use position::add::AddPosition;
use position::update::UpdatePosition;
use position::delete::DeletePosition;
use position::move_nr::MovePosition;
use payment::create::CreatePayment;
use payment::update::UpdatePayment;
use payment::delete::DeletePayment;
use product::create::CreateProduct;
use product::update::UpdateProduct;
use product::delete::DeleteProduct;
use product::set_active::SetProductActive;
use sepa::create::CreateSepaMandate;
use sepa::update::UpdateSepaMandate;
use sepa::delete::DeleteSepaMandate;
use recurring::create::CreateRecurring;
use recurring::update::UpdateRecurring;
use recurring::delete::DeleteRecurring;
use recurring::add_position::AddRecurringPosition;
use recurring::update_position::UpdateRecurringPosition;
use recurring::delete_position::DeleteRecurringPosition;
use recurring::run_once::RunRecurringOnce;
use activity::log::LogActivity;

/// Wire-format enum. Each variant wraps the per-command struct defined in its
/// executor module; `#[serde(tag = "type")]` produces a flat JSON object
/// (`{ "type": "X", ...fields }`), Borsh encodes as `variant_idx + struct_bytes`,
/// ts-rs emits an intersection (`{ "type": "X" } & X`) that is structurally
/// equivalent to the old flat form. Variant order is API-stable (Borsh index).
#[rpc_command_enum]
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum InvoiceCommand {
CreateCustomer(CreateCustomer),
UpdateCustomer(UpdateCustomer),
DeleteCustomer(DeleteCustomer),
DeleteCustomerCascade(DeleteCustomerCascade),

CreateContact(CreateContact),
UpdateContact(UpdateContact),
DeleteContact(DeleteContact),

CreateInvoice(CreateInvoice),
UpdateInvoiceHeader(UpdateInvoiceHeader),
DeleteInvoice(DeleteInvoice),
    MarkPaid(MarkPaid),
MarkSent(MarkSent),
Storno(Storno),
ConvertOfferToInvoice(ConvertOfferToInvoice),
AssignCustomer(AssignCustomer),
DuplicateInvoice(DuplicateInvoice),
CreateCreditNote(CreateCreditNote),

AddPosition(AddPosition),
UpdatePosition(UpdatePosition),
DeletePosition(DeletePosition),
MovePosition(MovePosition),

CreatePayment(CreatePayment),
UpdatePayment(UpdatePayment),
DeletePayment(DeletePayment),

CreateProduct(CreateProduct),
UpdateProduct(UpdateProduct),
DeleteProduct(DeleteProduct),
SetProductActive(SetProductActive),

CreateSepaMandate(CreateSepaMandate),
UpdateSepaMandate(UpdateSepaMandate),
DeleteSepaMandate(DeleteSepaMandate),

CreateRecurring(CreateRecurring),
UpdateRecurring(UpdateRecurring),
DeleteRecurring(DeleteRecurring),
AddRecurringPosition(AddRecurringPosition),
UpdateRecurringPosition(UpdateRecurringPosition),
DeleteRecurringPosition(DeleteRecurringPosition),
RunRecurringOnce(RunRecurringOnce),

LogActivity(LogActivity),
}

impl Command for InvoiceCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        use InvoiceCommand::*;
        match self {
            CreateCustomer(c) => c.execute_optimistic(db),
            UpdateCustomer(c) => c.execute_optimistic(db),
            DeleteCustomer(c) => c.execute_optimistic(db),
            DeleteCustomerCascade(c) => c.execute_optimistic(db),
            CreateContact(c) => c.execute_optimistic(db),
            UpdateContact(c) => c.execute_optimistic(db),
            DeleteContact(c) => c.execute_optimistic(db),
            CreateInvoice(c) => c.execute_optimistic(db),
            UpdateInvoiceHeader(c) => c.execute_optimistic(db),
            DeleteInvoice(c) => c.execute_optimistic(db),
            MarkPaid(c) => c.execute_optimistic(db),
            MarkSent(c) => c.execute_optimistic(db),
            Storno(c) => c.execute_optimistic(db),
            ConvertOfferToInvoice(c) => c.execute_optimistic(db),
            AssignCustomer(c) => c.execute_optimistic(db),
            DuplicateInvoice(c) => c.execute_optimistic(db),
            CreateCreditNote(c) => c.execute_optimistic(db),
            AddPosition(c) => c.execute_optimistic(db),
            UpdatePosition(c) => c.execute_optimistic(db),
            DeletePosition(c) => c.execute_optimistic(db),
            MovePosition(c) => c.execute_optimistic(db),
            CreatePayment(c) => c.execute_optimistic(db),
            UpdatePayment(c) => c.execute_optimistic(db),
            DeletePayment(c) => c.execute_optimistic(db),
            CreateProduct(c) => c.execute_optimistic(db),
            UpdateProduct(c) => c.execute_optimistic(db),
            DeleteProduct(c) => c.execute_optimistic(db),
            SetProductActive(c) => c.execute_optimistic(db),
            CreateSepaMandate(c) => c.execute_optimistic(db),
            UpdateSepaMandate(c) => c.execute_optimistic(db),
            DeleteSepaMandate(c) => c.execute_optimistic(db),
            CreateRecurring(c) => c.execute_optimistic(db),
            UpdateRecurring(c) => c.execute_optimistic(db),
            DeleteRecurring(c) => c.execute_optimistic(db),
            AddRecurringPosition(c) => c.execute_optimistic(db),
            UpdateRecurringPosition(c) => c.execute_optimistic(db),
            DeleteRecurringPosition(c) => c.execute_optimistic(db),
            RunRecurringOnce(c) => c.execute_optimistic(db),
            LogActivity(c) => c.execute_optimistic(db),
        }
    }
}

/// Wire-enum-level `ServerCommand` dispatcher. Exhaustive on purpose: every
/// variant has its own `impl ServerCommand` that runs its SQL directly
/// against TiDB; `CreatePayment` additionally enforces an authoritative
/// balance check. Dropping the catch-all arm means adding a new variant
/// forces a compile-time decision about its server-side policy.
#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for InvoiceCommand {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            use InvoiceCommand::*;
            match self {
                CreateCustomer(c) => c.execute_server(tx, client_zset).await,
                UpdateCustomer(c) => c.execute_server(tx, client_zset).await,
                DeleteCustomer(c) => c.execute_server(tx, client_zset).await,
                DeleteCustomerCascade(c) => c.execute_server(tx, client_zset).await,
                CreateContact(c) => c.execute_server(tx, client_zset).await,
                UpdateContact(c) => c.execute_server(tx, client_zset).await,
                DeleteContact(c) => c.execute_server(tx, client_zset).await,
                CreateInvoice(c) => c.execute_server(tx, client_zset).await,
                UpdateInvoiceHeader(c) => c.execute_server(tx, client_zset).await,
                DeleteInvoice(c) => c.execute_server(tx, client_zset).await,
                MarkPaid(c) => c.execute_server(tx, client_zset).await,
                MarkSent(c) => c.execute_server(tx, client_zset).await,
                Storno(c) => c.execute_server(tx, client_zset).await,
                ConvertOfferToInvoice(c) => c.execute_server(tx, client_zset).await,
                AssignCustomer(c) => c.execute_server(tx, client_zset).await,
                DuplicateInvoice(c) => c.execute_server(tx, client_zset).await,
                CreateCreditNote(c) => c.execute_server(tx, client_zset).await,
                AddPosition(c) => c.execute_server(tx, client_zset).await,
                UpdatePosition(c) => c.execute_server(tx, client_zset).await,
                DeletePosition(c) => c.execute_server(tx, client_zset).await,
                MovePosition(c) => c.execute_server(tx, client_zset).await,
                CreatePayment(c) => c.execute_server(tx, client_zset).await,
                UpdatePayment(c) => c.execute_server(tx, client_zset).await,
                DeletePayment(c) => c.execute_server(tx, client_zset).await,
                CreateProduct(c) => c.execute_server(tx, client_zset).await,
                UpdateProduct(c) => c.execute_server(tx, client_zset).await,
                DeleteProduct(c) => c.execute_server(tx, client_zset).await,
                SetProductActive(c) => c.execute_server(tx, client_zset).await,
                CreateSepaMandate(c) => c.execute_server(tx, client_zset).await,
                UpdateSepaMandate(c) => c.execute_server(tx, client_zset).await,
                DeleteSepaMandate(c) => c.execute_server(tx, client_zset).await,
                CreateRecurring(c) => c.execute_server(tx, client_zset).await,
                UpdateRecurring(c) => c.execute_server(tx, client_zset).await,
                DeleteRecurring(c) => c.execute_server(tx, client_zset).await,
                AddRecurringPosition(c) => c.execute_server(tx, client_zset).await,
                UpdateRecurringPosition(c) => c.execute_server(tx, client_zset).await,
                DeleteRecurringPosition(c) => c.execute_server(tx, client_zset).await,
                RunRecurringOnce(c) => c.execute_server(tx, client_zset).await,
                LogActivity(c) => c.execute_server(tx, client_zset).await,
            }
        }
    }
}
