pub mod stream;
pub mod client;

pub use client::SyncClient;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
