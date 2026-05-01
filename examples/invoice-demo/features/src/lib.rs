//! Invoice-demo umbrella crate. Each business entity lives in its own
//! folder. The naming inside a folder follows a convention:
//!   - lone Rust files get a `<entity>_` prefix (e.g. `product_client.rs`)
//!     so global file lookups are never ambiguous.
//!   - folders use the unprefixed role name (`client/`, `server/`,
//!     `command/`) because the parent folder already disambiguates.
//! Commands live in their own `command/` folder, one file per command.

pub mod shared;
pub mod command_helpers;
#[cfg(feature = "server")]
pub mod server_helpers;

// ============================================================
// Features (one folder per business entity)
// ============================================================

pub mod activity_log;
pub mod contacts;
pub mod customers;
pub mod invoices;
pub mod payments;
pub mod positions;
pub mod products;
pub mod recurring;
pub mod sepa_mandates;

// ============================================================
// AppCtx — server-side execution context
// ============================================================

/// App-level storage context. Server boot constructs this once with a
/// connected pool. `db` is a SeaORM handle that wraps the same pool —
/// `from_sqlx_mysql_pool` does not open a second connection pool.
#[cfg(feature = "server")]
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
    pub db: sea_orm::DatabaseConnection,
}

// ============================================================
// Server-side codegen output (Fetcher impls + register_all)
// ============================================================

#[cfg(feature = "server")]
pub mod __generated {
    include!(concat!(env!("OUT_DIR"), "/generated.rs"));
}

#[cfg(feature = "server")]
pub use __generated::register_all;

// ============================================================
// Command wire enum
// ============================================================

use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command_enum;
use serde::{Deserialize, Serialize};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use activity_log::command::log_activity::LogActivity;
use contacts::command::{
    create_contact::CreateContact, delete_contact::DeleteContact,
    update_contact::UpdateContact,
};
use customers::command::{
    create_customer::CreateCustomer, delete_customer::DeleteCustomer,
    delete_customer_cascade::DeleteCustomerCascade,
    update_customer::UpdateCustomer,
};
use invoices::command::{
    assign_customer::AssignCustomer,
    convert_offer_to_invoice::ConvertOfferToInvoice,
    create_credit_note::CreateCreditNote, create_invoice::CreateInvoice,
    delete_invoice::DeleteInvoice, duplicate_invoice::DuplicateInvoice,
    mark_paid::MarkPaid, mark_sent::MarkSent, storno::Storno,
    update_invoice_header::UpdateInvoiceHeader,
};
use payments::command::{
    create_payment::CreatePayment, delete_payment::DeletePayment,
    update_payment::UpdatePayment,
};
use positions::command::{
    add_position::AddPosition, delete_position::DeletePosition,
    move_position::MovePosition, update_position::UpdatePosition,
};
use products::command::{
    create_product::CreateProduct, delete_product::DeleteProduct,
    set_product_active::SetProductActive, update_product::UpdateProduct,
};
use recurring::command::{
    add_recurring_position::AddRecurringPosition,
    create_recurring::CreateRecurring,
    delete_recurring::DeleteRecurring,
    delete_recurring_position::DeleteRecurringPosition,
    run_recurring_once::RunRecurringOnce, update_recurring::UpdateRecurring,
    update_recurring_position::UpdateRecurringPosition,
};
use sepa_mandates::command::{
    create_sepa_mandate::CreateSepaMandate,
    delete_sepa_mandate::DeleteSepaMandate,
    update_sepa_mandate::UpdateSepaMandate,
};

/// Wire-format enum. Variant order is API-stable (Borsh index).
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
