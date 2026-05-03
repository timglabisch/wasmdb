//! cdylib-only crate — the entire surface is wasm32. The
//! `sync_client::define_wasm_api!` macro and the codegen-emitted
//! `register_all_requirements` both live behind
//! `cfg(target_arch = "wasm32")`, so on host targets this lib is empty.

#[cfg(target_arch = "wasm32")]
mod app {
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
        register_all_requirements,
        sepa_mandates::sepa_mandate_client::SepaMandate,
    };

    fn setup_db(db: &mut Database) {
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
    }

    sync_client::define_wasm_api!(
        command = InvoiceCommand,
        setup_db = setup_db,
        register_requirements = register_all_requirements,
    );
}
