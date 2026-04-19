pub mod customers;
pub use customers::Customers;

/// App-level storage context carried inside `StorageCtx<AppCtx>`.
/// Server boot constructs this once with a connected pool and hands it
/// to every `StorageTable::fetch`.
#[cfg(feature = "storage")]
pub struct AppCtx {
    pub pool: sqlx::MySqlPool,
}
