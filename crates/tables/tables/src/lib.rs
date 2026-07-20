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

/// The generated event-log row shape (`#[projection_row]` on a struct —
/// design doc §9.4/§9.6). One table holds many independent logs, one per
/// partition value; `PARTITION_COLUMN` names the column that carries it.
///
/// Implemented by the macro expansion, never by hand. The provided
/// methods replace the identical preamble of every log-consuming
/// projection; hole semantics, domain mapping and skipping unfoldable
/// events stay product code.
pub trait ProjectionLog {
    /// Column name of the partition — the log's stream identity.
    const PARTITION_COLUMN: &'static str;

    /// Authoritative once committed; provisional (client-assigned,
    /// per-partition `max + 1`) while off-chain.
    fn seq(&self) -> i64;
    /// `0` = off-chain/optimistic; anything else = committed.
    fn committed(&self) -> i64;
    /// The command's RPC wire form (JSON).
    fn payload(&self) -> &str;

    fn is_committed(&self) -> bool {
        self.committed() != 0
    }

    /// Decode the payload back into its RPC command type. `Err` carries
    /// the type name — returning it from `project` pins the partition's
    /// failure slot. Products wanting forward compatibility (unknown
    /// command from a newer client, arrived via tail) match and skip
    /// instead of `?`.
    fn decode<C: serde::de::DeserializeOwned>(&self) -> Result<C, String> {
        serde_json::from_str(self.payload()).map_err(|e| {
            format!(
                "payload does not decode as {}: {e}",
                std::any::type_name::<C>()
            )
        })
    }

    /// Fold order (design §9.3): committed events by authoritative `seq`,
    /// then pendings by provisional `seq`. Stable within ties.
    fn in_fold_order(rows: &[Self]) -> Vec<&Self>
    where
        Self: Sized,
    {
        let mut ordered: Vec<&Self> = rows.iter().collect();
        ordered.sort_by_key(|r| (!r.is_committed(), r.seq()));
        ordered
    }
}
