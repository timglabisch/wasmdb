use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
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
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        use InvoiceCommand::*;
        match self {
            CreateCustomer(c) => c.execute(db),
            UpdateCustomer(c) => c.execute(db),
            DeleteCustomer(c) => c.execute(db),
            DeleteCustomerCascade(c) => c.execute(db),
            CreateContact(c) => c.execute(db),
            UpdateContact(c) => c.execute(db),
            DeleteContact(c) => c.execute(db),
            CreateInvoice(c) => c.execute(db),
            UpdateInvoiceHeader(c) => c.execute(db),
            DeleteInvoice(c) => c.execute(db),
            AddPosition(c) => c.execute(db),
            UpdatePosition(c) => c.execute(db),
            DeletePosition(c) => c.execute(db),
            MovePosition(c) => c.execute(db),
            CreatePayment(c) => c.execute(db),
            UpdatePayment(c) => c.execute(db),
            DeletePayment(c) => c.execute(db),
            CreateProduct(c) => c.execute(db),
            UpdateProduct(c) => c.execute(db),
            DeleteProduct(c) => c.execute(db),
            CreateSepaMandate(c) => c.execute(db),
            UpdateSepaMandate(c) => c.execute(db),
            DeleteSepaMandate(c) => c.execute(db),
            CreateRecurring(c) => c.execute(db),
            UpdateRecurring(c) => c.execute(db),
            DeleteRecurring(c) => c.execute(db),
            AddRecurringPosition(c) => c.execute(db),
            UpdateRecurringPosition(c) => c.execute(db),
            DeleteRecurringPosition(c) => c.execute(db),
            RunRecurringOnce(c) => c.execute(db),
            LogActivity(c) => c.execute(db),
        }
    }
}
