//! Confirm-server for projection-demo.
//!
//! No SQL database — only a tiny in-memory [`ServerLog`] (design §11.5):
//! per-account chain heads plus a by-PK store of the committed rows. Two
//! endpoints:
//!
//! - `POST /command` — every command approves its own delta through
//!   `ServerCommand::execute_server`. `PostEntry` approves by echoing the
//!   client's delta with the `ledger_log` row's `server_parent_id` stamped
//!   to the account's current head (`ROOT_PARENT` for the first commit) and
//!   recording the committed row. The client's invert+apply reconcile then
//!   finalizes the row from optimistic (`server_parent_id: None`) to
//!   committed (`Some(..)`), which lets `BalanceFold`'s committed-frontier
//!   memo advance and makes the pending → committed transition (and any
//!   `client_parent_id != server_parent_id` drift) visible in the UI.
//!
//! - `POST /fetch` — fetch-by-PK (design §11.4). Answers a
//!   [`FetchRowsRequest`] from the row store so a client can backfill
//!   committed ancestors it never fetched. This is what makes gap-repair
//!   possible: the server links a client's row onto a head the client
//!   doesn't hold (see the `carol` seed below), and the client walks the
//!   chain backward through this endpoint until it is contiguous from ROOT.
//!
//! - `POST /heads` — the current chain heads (`HeadsRequest`/`HeadsResponse`).
//!   A fresh client holds nothing, so nothing references a parent to repair
//!   yet; handed the heads it fetches them and walks each chain to ROOT.
//!   This is the bootstrap: it makes the server the source of truth and a
//!   page reload non-destructive (the client rebuilds its state from here).
//!
//! - `POST /foreign-write` — simulate another writer: append a burst of
//!   committed entries to `carol` out-of-band. The next client sync hits an
//!   unknown `server_parent_id` and gap-repairs — a live, mid-session
//!   demonstration of §11.4 rather than only the boot-time one.
//!
//! The server owns the order: it links each confirmation onto the head in
//! the order `execute_server` is called. A real multi-client server would
//! persist this frontier and the row store rather than hold them in memory.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use borsh::BorshDeserialize;
use projection_demo_domain::{ProjectionDemoCommand, ServerCommand, ServerLog};
use sql_engine::storage::Uuid;
use sync::protocol::{
    BatchCommandRequest, BatchCommandResponse, CommandResponse, FetchRowsRequest,
    FetchRowsResponse, HeadsRequest, HeadsResponse, Verdict,
};
use tower_http::services::ServeDir;

/// Shared, mutable confirm-server state — chain heads + committed rows.
type Store = Arc<Mutex<ServerLog>>;

async fn handle_command(State(store): State<Store>, body: Bytes) -> impl IntoResponse {
    let batch = match BatchCommandRequest::<ProjectionDemoCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let responses: Vec<CommandResponse> = {
        let mut log = store.lock().unwrap();
        batch
            .requests
            .into_iter()
            .map(|r| {
                let verdict = match r.command.execute_server(&r.client_zset, &mut log) {
                    Ok(server_zset) => Verdict::Confirmed { server_zset },
                    Err(e) => Verdict::Rejected { reason: e.to_string() },
                };
                CommandResponse {
                    stream_id: r.stream_id,
                    seq_no: r.seq_no,
                    verdict,
                }
            })
            .collect()
    };

    let bytes = borsh::to_vec(&BatchCommandResponse { responses })
        .expect("serialize batch response");
    (StatusCode::OK, bytes)
}

/// Fetch-by-PK: hand back the committed rows the client is missing so its
/// backward-refetch loop can close a chain gap (design §11.4).
async fn handle_fetch(State(store): State<Store>, body: Bytes) -> impl IntoResponse {
    let request = match FetchRowsRequest::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let rows = store.lock().unwrap().fetch(&request.ids);
    let bytes = borsh::to_vec(&FetchRowsResponse { rows }).expect("serialize fetch response");
    (StatusCode::OK, bytes)
}

/// Current chain heads: the tip `command_id`s a fresh client bootstraps
/// from. The client fetches these by PK, then walks each chain back to
/// ROOT via gap-repair — reconstructing its whole state from the server
/// (which is why a page reload no longer loses anything).
async fn handle_heads(State(store): State<Store>, body: Bytes) -> impl IntoResponse {
    // The demo has a single log table; the field is accepted (and future-
    // proofs the wire) but every partition head is returned regardless.
    let _request = match HeadsRequest::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let ids = store.lock().unwrap().heads();
    let bytes = borsh::to_vec(&HeadsResponse { ids }).expect("serialize heads response");
    (StatusCode::OK, bytes)
}

/// Simulate another writer: append a short burst of committed entries to
/// `carol` out-of-band. No client held these, so the next client sync (or
/// carol post) hits an unknown `server_parent_id` and gap-repairs. Returns
/// the number of entries injected as plain text.
async fn handle_foreign_write(State(store): State<Store>) -> impl IntoResponse {
    const BURST: u32 = 3;
    let ids = store.lock().unwrap().foreign_write("carol", BURST);
    (StatusCode::OK, ids.len().to_string())
}

/// The server owns *all* opening state now — no client-side seed. On boot
/// the client holds nothing and bootstraps it back from here (fetch heads →
/// walk each chain to ROOT), so a page reload restores exactly this.
///
/// - `alice`/`bob`: ordinary opening balances (fixed `…00aN`/`…00bN` ids).
/// - `carol`: a pre-existing chain from *another writer* who advanced this
///   partition before the client ever loaded (`0xca…` ids). The client
///   never posted these, so bootstrapping carol is itself a gap-repair, and
///   the `/foreign-write` button keeps extending this chain out-of-band.
fn seed(log: &mut ServerLog) {
    // Single-byte ids mirroring the demo's original opening balances.
    let id = |n: u8| {
        let mut b = [0u8; 16];
        b[15] = n;
        Uuid(b)
    };
    log.seed_chain("alice", &[(id(0xa1), 5000), (id(0xa2), -1250)]);
    log.seed_chain("bob", &[(id(0xb1), 10000), (id(0xb2), -2000), (id(0xb3), 750)]);

    // Carol's `0xca…` chain — distinct from the client's random v4 ids.
    let carol = |n: u8| {
        let mut b = [0u8; 16];
        b[0] = 0xca;
        b[15] = n;
        Uuid(b)
    };
    log.seed_chain("carol", &[(carol(1), 3000), (carol(2), -500)]);
}

pub async fn run() {
    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../frontend/apps/ui/dist");

    let mut initial = ServerLog::new();
    seed(&mut initial);
    let store: Store = Arc::new(Mutex::new(initial));

    let app = axum::Router::new()
        .route("/command", post(handle_command))
        .route("/fetch", post(handle_fetch))
        .route("/heads", post(handle_heads))
        .route("/foreign-write", post(handle_foreign_write))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(store);

    let addr = "0.0.0.0:3126";
    eprintln!("[projection-demo-server] listening on http://localhost:3126");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
