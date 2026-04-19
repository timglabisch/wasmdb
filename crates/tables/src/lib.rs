//! Parameterized data access — rows and fetchers.
//!
//! Two concepts, kept separate:
//!
//! - A **Row** is a data shape. `Customer { id, name }`. Reusable; carries
//!   no RPC identity of its own.
//! - A **Fetcher** is a named query that returns rows of a given type.
//!   Its params are the input to that query, and its `ID` is the wire-level
//!   identity for RPC dispatch. Many fetchers can share a row type
//!   (`customers::by_owner`, `customers::by_id`, …).
//!
//! Clients cannot write arbitrary SQL — the params of a fetcher are the
//! only knobs, fixed by the Rust definition.
//!
//! # Layering
//!
//! - `tables` (this crate, shared wasm + native): `Row`, `Fetcher`,
//!   `FetchRequest`.
//! - `tables-client` (wasm-capable): generic `fetch::<F>()`.
//! - `tables-storage` (native only): `Registry`, `#[row]`, `#[query]`.
//! - `tables-codegen` (build-time, used in `build.rs`): parses the
//!   storage crate's source and emits Row/Fetcher/wasm-binding glue
//!   for both sides.

use borsh::{BorshSerialize, BorshDeserialize};

/// Stable identifier for a fetcher (one per `#[query]` definition).
pub type FetcherId = &'static str;

/// A row in the result set of a fetcher. The PK projection lets the
/// reactive system match deletes/updates without consulting the server.
pub trait Row: BorshSerialize + BorshDeserialize + Clone + 'static {
    type Pk: Clone + Eq + std::hash::Hash;
    fn pk(&self) -> Self::Pk;
}

/// Ties a named query together. Shared between client and server.
/// Params are Borsh-encoded on the wire; no separate `Params` trait —
/// the bounds live here directly.
pub trait Fetcher: 'static {
    const ID: FetcherId;
    type Params: BorshSerialize + BorshDeserialize + Clone + 'static;
    type Row: Row;
}

/// Wire-level fetch request. Body of `POST /table-fetch` (or whatever
/// path the app picks). `params` is Borsh-encoded `F::Params` for the
/// named fetcher.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct FetchRequest {
    pub fetcher_id: String,
    pub params: Vec<u8>,
}
