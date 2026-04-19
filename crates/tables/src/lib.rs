//! Parameterized tables — the primary data-access primitive.
//!
//! A *table* is a named, parameterized dataset. Each parameter combination
//! `(table_id, args)` is a distinct logical table instance. Base entities
//! (`Customers`, `Invoices`, ...) and reports (`CustomerRevenue`) are both
//! expressed as tables — only the backing differs.
//!
//! Clients cannot write arbitrary SQL against tables — the parameters are
//! the only knobs, fixed by the Rust definition.
//!
//! # Layering
//!
//! - `tables` (this crate, shared wasm + native): `Table` trait.
//! - `tables-client` (wasm-capable): client Registry, `subscribe()`, `Live`.
//! - `tables-server` (native only): `ServerTable` trait, sqlx bridge.

use borsh::{BorshSerialize, BorshDeserialize};

/// Stable identifier for a table kind (one per `#[table]` definition).
pub type TableId = &'static str;

/// A parameter tuple for one table instance.
pub trait Params: BorshSerialize + BorshDeserialize + Clone + 'static {}

/// A row in the result set of a table. The PK projection lets the
/// reactive system match deletes/updates without consulting the server.
pub trait Row: BorshSerialize + BorshDeserialize + Clone + 'static {
    type Pk: Clone + Eq + std::hash::Hash;
    fn pk(&self) -> Self::Pk;
}

/// Ties a table definition together. Shared between client and server.
pub trait Table: 'static {
    const ID: TableId;
    type Params: Params;
    type Row: Row;
}

/// Wire-level fetch request. Body of `POST /table-fetch` (or whatever
/// path the app picks). `params` is Borsh-encoded `T::Params` for the
/// named table.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct FetchRequest {
    pub table_id: String,
    pub params: Vec<u8>,
}
