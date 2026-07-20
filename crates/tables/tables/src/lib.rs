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
use sql_engine::storage::Uuid;

/// The chain root sentinel (design §11): a row whose parent link is
/// `ROOT_PARENT` is the first of its partition. The nil UUID. Both parent
/// links (`client_parent_id` and, once accepted, `server_parent_id`) point
/// here for a partition's opening row.
pub const ROOT_PARENT: Uuid = Uuid([0u8; 16]);

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
/// design doc §9.4/§9.6/§11). One table holds many independent logs, one
/// per partition value; `PARTITION_COLUMN` names the column that carries
/// it.
///
/// Commit order is a **two-parent-link chain** (§11): no `committed` flag,
/// no client `seq`. `client_parent_id` is the predecessor the client
/// optimistically assumes; `server_parent_id` is the one the server
/// assigns authoritatively. A row is committed exactly when the server has
/// linked it (`server_parent_id.is_some()`); drift is a per-row
/// `client_parent_id != server_parent_id`.
///
/// Implemented by the macro expansion, never by hand. The provided
/// methods replace the identical preamble of every log-consuming
/// projection; hole semantics, domain mapping and skipping unfoldable
/// events stay product code.
pub trait ProjectionLog {
    /// Column name of the partition — the log's stream identity.
    const PARTITION_COLUMN: &'static str;

    /// This row's identity — the PK the parent links point at.
    fn command_id(&self) -> Uuid;
    /// The client's optimistically-assumed predecessor: its local chain
    /// head when the row was posted. Always set — `ROOT_PARENT` for the
    /// partition's opening row.
    fn client_parent_id(&self) -> Uuid;
    /// The server's authoritative predecessor. `None` = pending (off-chain,
    /// not yet accepted into the chain); `Some(ROOT_PARENT)` = the first
    /// committed row; `Some(x)` = committed directly after `x`. Assigned
    /// EXCLUSIVELY by the server (§11.1) — the client never sets it, so
    /// `is_some()` stays a faithful "committed" witness.
    fn server_parent_id(&self) -> Option<Uuid>;
    /// The command's RPC wire form (JSON).
    fn payload(&self) -> &str;

    /// Committed once the server has linked the row into the chain.
    fn is_committed(&self) -> bool {
        self.server_parent_id().is_some()
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

    /// Fold order (design §11.3): the committed rows first — the
    /// `server_parent_id` chain walked forward from `ROOT_PARENT` — then the
    /// pending tail — the `client_parent_id` chain walked forward from the
    /// committed head. Rows the chains cannot reach (a gap awaiting
    /// backward-refetch repair, §11.4) are appended last in a deterministic
    /// order — committed before pending, then by `command_id` — rather than
    /// dropped or panicked on.
    fn in_fold_order(rows: &[Self]) -> Vec<&Self>
    where
        Self: Sized,
    {
        use std::collections::{HashMap, HashSet};

        // parent → child, split by which chain the child belongs to: a
        // committed row is indexed by its server parent, a pending one by
        // its client parent.
        let mut committed_child: HashMap<Uuid, &Self> = HashMap::with_capacity(rows.len());
        let mut pending_child: HashMap<Uuid, &Self> = HashMap::new();
        for r in rows {
            match r.server_parent_id() {
                Some(parent) => {
                    committed_child.insert(parent, r);
                }
                None => {
                    pending_child.insert(r.client_parent_id(), r);
                }
            }
        }

        let mut ordered: Vec<&Self> = Vec::with_capacity(rows.len());
        let mut seen: HashSet<Uuid> = HashSet::with_capacity(rows.len());

        // Committed prefix: forward along the server chain from ROOT.
        let mut cursor = ROOT_PARENT;
        while let Some(&r) = committed_child.get(&cursor) {
            if !seen.insert(r.command_id()) {
                break; // cycle guard — corrupt chain, stop rather than loop
            }
            ordered.push(r);
            cursor = r.command_id();
        }
        // Pending tail: forward along the client chain from the committed
        // head (or ROOT when nothing is committed yet).
        while let Some(&r) = pending_child.get(&cursor) {
            if !seen.insert(r.command_id()) {
                break;
            }
            ordered.push(r);
            cursor = r.command_id();
        }
        // Unreachable rows (gap/drift, §11.4): keep them — never drop, never
        // panic — in a deterministic order until repair fills the chain.
        if ordered.len() != rows.len() {
            let mut rest: Vec<&Self> = rows
                .iter()
                .filter(|r| !seen.contains(&r.command_id()))
                .collect();
            rest.sort_by_key(|r| (!r.is_committed(), r.command_id()));
            ordered.extend(rest);
        }
        ordered
    }
}
