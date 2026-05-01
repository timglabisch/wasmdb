use std::cell::RefCell;

use database::Database;
use invoice_demo_features::InvoiceCommand;
use invoice_demo_tables_client_generated::{
    activity_log::activity_log_client::ActivityLogEntry,
    contacts::contact_client::Contact,
    customers::customer_client::Customer,
    invoices::invoice_client::Invoice,
    payments::payment_client::Payment,
    positions::position_client::Position,
    products::product_client::Product,
    recurring::recurring_invoice_client::RecurringInvoice,
    recurring::recurring_position_client::RecurringPosition,
    sepa_mandates::sepa_mandate_client::SepaMandate,
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
