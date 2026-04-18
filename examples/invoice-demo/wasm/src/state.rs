use std::cell::RefCell;

use database::Database;
use invoice_demo_commands::InvoiceCommand;
use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};
use sync_client::client::SyncClient;

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<InvoiceCommand>>> = RefCell::new(None);
    pub(crate) static DEFAULT_STREAM_ID: RefCell<Option<u64>> = RefCell::new(None);
    pub(crate) static ID_COUNTER: RefCell<i64> = RefCell::new(0);
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

fn col(name: &str, ty: DataType) -> ColumnSchema {
    ColumnSchema { name: name.into(), data_type: ty, nullable: false }
}
fn str_col(name: &str) -> ColumnSchema { col(name, DataType::String) }
fn i64_col(name: &str) -> ColumnSchema { col(name, DataType::I64) }

pub(crate) fn make_db() -> Database {
    let mut db = Database::new();

    db.create_table(TableSchema {
        name: "customers".into(),
        columns: vec![
            i64_col("id"),
            str_col("name"), str_col("email"), str_col("created_at"),
            str_col("company_type"), str_col("tax_id"), str_col("vat_id"),
            i64_col("payment_terms_days"), i64_col("default_discount_pct"),
            str_col("billing_street"), str_col("billing_zip"), str_col("billing_city"), str_col("billing_country"),
            str_col("shipping_street"), str_col("shipping_zip"), str_col("shipping_city"), str_col("shipping_country"),
            str_col("default_iban"), str_col("default_bic"), str_col("notes"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();

    db.create_table(TableSchema {
        name: "contacts".into(),
        columns: vec![
            i64_col("id"), i64_col("customer_id"),
            str_col("name"), str_col("email"), str_col("phone"), str_col("role"),
            i64_col("is_primary"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "invoices".into(),
        columns: vec![
            i64_col("id"), i64_col("customer_id"),
            str_col("number"), str_col("status"),
            str_col("date_issued"), str_col("date_due"), str_col("notes"),
            str_col("doc_type"),
            i64_col("parent_id"),
            str_col("service_date"),
            i64_col("cash_allowance_pct"), i64_col("cash_allowance_days"), i64_col("discount_pct"),
            str_col("payment_method"),
            i64_col("sepa_mandate_id"),
            str_col("currency"), str_col("language"),
            str_col("project_ref"), str_col("external_id"),
            str_col("billing_street"), str_col("billing_zip"), str_col("billing_city"), str_col("billing_country"),
            str_col("shipping_street"), str_col("shipping_zip"), str_col("shipping_city"), str_col("shipping_country"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "positions".into(),
        columns: vec![
            i64_col("id"), i64_col("invoice_id"), i64_col("position_nr"),
            str_col("description"),
            i64_col("quantity"), i64_col("unit_price"), i64_col("tax_rate"),
            i64_col("product_id"),
            str_col("item_number"), str_col("unit"),
            i64_col("discount_pct"), i64_col("cost_price"),
            str_col("position_type"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "payments".into(),
        columns: vec![
            i64_col("id"), i64_col("invoice_id"),
            i64_col("amount"), str_col("paid_at"),
            str_col("method"), str_col("reference"), str_col("note"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "products".into(),
        columns: vec![
            i64_col("id"),
            str_col("sku"), str_col("name"), str_col("description"),
            str_col("unit"),
            i64_col("unit_price"), i64_col("tax_rate"), i64_col("cost_price"),
            i64_col("active"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();

    db.create_table(TableSchema {
        name: "recurring_invoices".into(),
        columns: vec![
            i64_col("id"), i64_col("customer_id"),
            str_col("template_name"),
            str_col("interval_unit"), i64_col("interval_value"),
            str_col("next_run"), str_col("last_run"),
            i64_col("enabled"),
            str_col("status_template"), str_col("notes_template"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "recurring_positions".into(),
        columns: vec![
            i64_col("id"), i64_col("recurring_id"), i64_col("position_nr"),
            str_col("description"),
            i64_col("quantity"), i64_col("unit_price"), i64_col("tax_rate"),
            str_col("unit"), str_col("item_number"),
            i64_col("discount_pct"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "sepa_mandates".into(),
        columns: vec![
            i64_col("id"), i64_col("customer_id"),
            str_col("mandate_ref"),
            str_col("iban"), str_col("bic"),
            str_col("holder_name"),
            str_col("signed_at"),
            str_col("status"),
        ],
        primary_key: vec![0],
        indexes: vec![
            IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
        ],
    }).unwrap();

    db.create_table(TableSchema {
        name: "activity_log".into(),
        columns: vec![
            i64_col("id"),
            str_col("timestamp"),
            str_col("entity_type"), i64_col("entity_id"),
            str_col("action"), str_col("actor"), str_col("detail"),
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();

    db
}
