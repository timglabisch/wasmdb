use borsh::{BorshSerialize, BorshDeserialize};
use sql_engine::storage::Uuid;
use crate::zset::ZSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub struct SeqNo(pub u64);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CommandRequest<C> {
    pub stream_id: StreamId,
    pub seq_no: SeqNo,
    pub command: C,
    pub client_zset: ZSet,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CommandResponse {
    pub stream_id: StreamId,
    pub seq_no: SeqNo,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum Verdict {
    Confirmed { server_zset: ZSet },
    Rejected { reason: String },
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BatchCommandRequest<C> {
    pub requests: Vec<CommandRequest<C>>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BatchCommandResponse {
    pub responses: Vec<CommandResponse>,
}

/// Fetch canonical committed rows by primary key — the transport for
/// commit-chain-v2 gap-repair (design §11.4). When a client receives a
/// committed `server_parent_id` it doesn't hold locally, an ancestor of
/// the chain is missing: it walks the chain backward by requesting the
/// unknown parents here, then their parents, until the chain is contiguous
/// from `ROOT`. Orthogonal to the confirm path — the server answers from
/// its authoritative store.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct FetchRowsRequest {
    /// The log table whose rows are being fetched (e.g. `ledger_log`).
    pub table: String,
    /// The primary keys (`command_id`s) of the missing rows.
    pub ids: Vec<Uuid>,
}

/// The rows a [`FetchRowsRequest`] resolved to, as a `+1` ZSet ready to
/// apply. Rows the server does not hold are simply absent — the client
/// treats a short/empty answer as "chain end reached / nothing more to
/// repair" and stops, so a lying or lossy server can never spin the loop.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct FetchRowsResponse {
    pub rows: ZSet,
}

/// Ask the server for the current committed-chain heads (the tip
/// `command_id` of every partition) of a log table. This is the seed a
/// fresh client needs to bootstrap: it holds nothing, so nothing
/// *references* a parent to repair yet. Handed the heads, it fetches them
/// by PK (`FetchRowsRequest`) and then walks each chain backward to `ROOT`
/// via gap-repair (design §11.4) — reconstructing the whole committed
/// history from the server. A page reload is exactly this: the client's
/// wasm memory is empty, so bootstrap restores its state from the server.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct HeadsRequest {
    /// The log table whose partition heads are being requested.
    pub table: String,
}

/// The current head `command_id`s across all partitions of the requested
/// table. Order is not significant; the client fetches and repairs each.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct HeadsResponse {
    pub ids: Vec<Uuid>,
}
