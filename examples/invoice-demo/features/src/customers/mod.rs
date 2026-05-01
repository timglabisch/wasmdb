pub mod customer_client;
#[cfg(feature = "server")]
pub mod customer_server;
pub mod command;

pub use customer_client::Customer;
