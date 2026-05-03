pub mod product_client;
#[cfg(feature = "server")]
pub mod product_server;
pub mod command;

pub use product_client::Product;
