pub mod cols;
pub mod customers;
pub mod invoices;
pub mod products;
pub mod recurring;
pub mod sepa_mandates;
pub mod activity_log;

use database::Database;

pub fn make_db() -> Database {
    let mut db = Database::new();
    customers::create(&mut db);
    invoices::create(&mut db);
    products::create(&mut db);
    recurring::create(&mut db);
    sepa_mandates::create(&mut db);
    activity_log::create(&mut db);
    db
}
