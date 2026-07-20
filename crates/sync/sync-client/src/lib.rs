pub mod stream;
pub mod client;

pub use client::SyncClient;

/// Re-export so app crates can build a `ProjectionEngine` for
/// `define_wasm_api!(… projections = …)` without a direct dependency.
pub use database_projection;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
