//! E2E test fixture for the `tables` ecosystem.
//!
//! Provides `AppCtx` with in-memory row fixtures plus generated trait impls
//! produced by `tables-codegen` in `build.rs`. Tests in `tests/` register
//! the generated `DbTable`s and `DbCaller`s with a `Database` and drive SQL
//! through `execute_async` — see `tests/common/mod.rs` for the harness.

mod contacts;
mod customers;
mod invoices;
mod products;

pub use contacts::Contact;
pub use customers::Customer;
pub use invoices::Invoice;
pub use products::Product;

/// In-memory fixture context for the generated `#[query]` functions.
/// Each `Vec` is read-only after construction; query bodies filter + clone.
#[derive(Clone, Default)]
pub struct AppCtx {
    pub customers: Vec<Customer>,
    pub products: Vec<Product>,
    pub invoices: Vec<Invoice>,
    pub contacts: Vec<Contact>,
}

impl AppCtx {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Populated fixture used by most tests.
    ///
    /// * customers: Alice(1)/Carol(3) own=1, Bob(2) own=2
    /// * products: gadget $100, widget $50, freebie (no price)
    /// * invoices: Alice 2×, Bob 1×, Carol 1×; mixed `note` values
    pub fn with_default_fixtures() -> Self {
        Self {
            customers: vec![
                Customer { id: 1, name: "Alice".into(), owner_id: 1 },
                Customer { id: 2, name: "Bob".into(), owner_id: 2 },
                Customer { id: 3, name: "Carol".into(), owner_id: 1 },
            ],
            products: vec![
                Product { sku: "gadget".into(), name: "Gadget".into(), price: Some(100) },
                Product { sku: "widget".into(), name: "Widget".into(), price: Some(50) },
                Product { sku: "freebie".into(), name: "Freebie".into(), price: None },
            ],
            invoices: vec![
                Invoice { id: 10, customer_id: 1, amount: 100, note: Some("rush".into()) },
                Invoice { id: 11, customer_id: 1, amount: 200, note: None },
                Invoice { id: 12, customer_id: 2, amount: 50, note: Some("urgent rush".into()) },
                Invoice { id: 13, customer_id: 3, amount: 300, note: None },
            ],
            contacts: contact_fixtures(),
        }
    }
}

/// Stable UUID fixtures so tests can match by bytes without parsing.
pub fn contact_uuid(n: u8) -> sql_engine::storage::Uuid {
    let mut b = [0u8; 16];
    b[15] = n;
    sql_engine::storage::Uuid(b)
}

fn contact_fixtures() -> Vec<Contact> {
    vec![
        Contact { id: contact_uuid(1), name: "Alice".into(), external_id: Some(contact_uuid(101)) },
        Contact { id: contact_uuid(2), name: "Bob".into(),   external_id: None },
        Contact { id: contact_uuid(3), name: "Carol".into(), external_id: Some(contact_uuid(103)) },
    ]
}

pub mod __generated {
    include!(concat!(env!("OUT_DIR"), "/generated.rs"));
}
