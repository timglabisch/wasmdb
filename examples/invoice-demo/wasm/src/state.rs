use std::cell::RefCell;

use database::Database;
use invoice_demo_commands::InvoiceCommand;
use invoice_demo_tables_client_generated::{
    activity_log::ActivityLogEntry,
    contacts::Contact,
    customers::Customer,
    invoices::Invoice,
    payments::Payment,
    positions::Position,
    products::Product,
    recurring_invoices::RecurringInvoice,
    recurring_positions::RecurringPosition,
    sepa_mandates::SepaMandate,
};
use sync_client::client::SyncClient;

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<InvoiceCommand>>> = RefCell::new(None);
    pub(crate) static DEFAULT_STREAM_ID: RefCell<Option<u64>> = RefCell::new(None);
}

pub(crate) fn install_client(client: SyncClient<InvoiceCommand>) {
    CLIENT.with(|c| *c.borrow_mut() = Some(client));
}

pub(crate) fn with_client<T>(f: impl FnOnce(&mut SyncClient<InvoiceCommand>) -> T) -> T {
    CLIENT.with(|c| {
        let mut borrow = c.borrow_mut();
        let client = borrow.as_mut().expect("client not initialized — call init() first");
        f(client)
    })
}

pub(crate) fn make_db() -> Database {
    let mut db = Database::new();

    // Row-Types live next to their `#[query]` fetchers in
    // `examples/invoice-demo/tables-storage/src/*.rs`; `DbTable` is emitted
    // by codegen so client registration and the server fetchers share one
    // column layout.
    db.register_table::<Customer>().unwrap();
    db.register_table::<Contact>().unwrap();
    db.register_table::<Invoice>().unwrap();
    db.register_table::<Position>().unwrap();
    db.register_table::<Payment>().unwrap();
    db.register_table::<Product>().unwrap();
    db.register_table::<RecurringInvoice>().unwrap();
    db.register_table::<RecurringPosition>().unwrap();
    db.register_table::<SepaMandate>().unwrap();
    db.register_table::<ActivityLogEntry>().unwrap();

    db
}
