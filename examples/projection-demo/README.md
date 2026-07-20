# projection-demo

The first end-to-end showcase of wasmdb's **projection engine**: an
event-sourced account ledger where derived state is maintained
automatically from an append-only event log.

```text
  PostEntry command  ──append──▶  ledger_log  ──fold──▶  balance
  (deposit/withdraw)              (event log)  (BalanceFold) (derived read model)
```

## What it demonstrates

- **`#[projection_row]`** (`shared/domain/src/ledger/ledger_log.rs`) — the
  append-only event log `ledger_log`. You declare only the identity
  (`command_id` PK + the `account` partition); the macro generates the
  two-parent-link bookkeeping (`client_parent_id` / `server_parent_id` /
  `payload`, design §11). The payload holds the domain event `EntryPosted`.
- **`#[rpc_command]`** (`.../command/post_entry.rs`) — `PostEntry` is a
  *request*, not a log row. Its `execute_optimistic` builds the log row
  directly (`sync::append::{client_head, append_row}` + `payload_json`),
  linking the new row onto the account's current chain head
  (`server_parent_id = None`, off-chain). Appending is an effect the command
  performs, not the command's identity — so any other entry point (an HTTP
  API, an MCP tool) can perform the same append.
- **`#[projection]`** (`.../balance_fold.rs`) — `BalanceFold` is
  implemented *on its own state type* (the cqrs-es Aggregate idiom):
  `apply` replays one log row, `render` projects the accumulated state
  into the derived `balance` table. The engine folds each account's
  committed prefix once and memoizes it (design §9.3), keyed by the
  server-chain id list.
- **`ServerCommand` + `ServerLog`** (`shared/domain/src/lib.rs`) — the
  server-side, DB-less counterpart of `Command`. `PostEntry::execute_server`
  *approves* the client's delta by stamping the authoritative
  `server_parent_id` from a per-account chain head, and records the committed
  row (design §11.5); the confirm-server (`server/src/lib.rs`) holds that
  `ServerLog` and dispatches. The UI shows entries as `pending` until the
  server links them (`committed`), and flags any `client_parent_id !=
  server_parent_id` drift.
- **Bootstrap = the server owns state** — the client DB lives entirely in
  wasm memory, so a page reload (or a fresh tab) starts empty. Rather than
  re-seed hardcoded rows, the client rebuilds its state *from the server*:
  on load it calls `bootstrap('ledger_log')` (`@wasmdb/client`), which asks
  the new **`/heads`** route for the current chain heads, fetches the ones
  it doesn't already hold via `/fetch`, then walks each chain back to ROOT.
  All opening balances (alice/bob/carol) are seeded server-side in
  `server/src/lib.rs::seed`. Consequence: **a reload no longer loses
  anything** (including ad-hoc posts, which the server recorded), and the
  bootstrap *is itself* a gap-repair — the empty client holds only heads and
  fills every ancestor.
- **Gap-repair, two ways** (design §11.4):
  - *At boot* — carol's chain is a pre-existing history from another writer
    (`0xca…` ids) the client never posted; bootstrapping carol walks that
    whole chain in.
  - *Live, mid-session* — the **“simulate another writer → carol”** button
    (`foreignWriteCarol` in `commands.ts`) hits `POST /foreign-write`, which
    appends a committed burst to carol out-of-band. The client then
    `bootstrap`s: carol's head is now a `command_id` it never fetched (a
    chain *gap*), so the backward walk pulls the new ancestors in and the
    balance jumps — no reload. Posting to carol *after* that also hits the
    gap on `.confirmed` via `repairChain`.

  `sync_client::repair::missing_parents` computes the gap frontier and
  `unknown_ids` keeps a re-bootstrap idempotent (never re-applies a held
  head); `sync::protocol::{FetchRows,Heads}{Request,Response}` are the wire.

The derived `balance` table is a normal reactive table: the React UI just
`useQuery`s it and re-renders when the projection writes to it.

## Run it

```bash
# dev (vite + wasm, hot reload):
make projection-demo-dev          # UI on http://localhost:5173
make projection-demo-dev-server   # confirm-server on :3126 (separate terminal)

# production build + serve from the server:
make projection-demo              # builds wasm + UI, serves on :3126
```

`make projection-demo-types` regenerates the TypeScript command factories
(via ts-rs) — run it after changing command shapes.

## Test

```bash
cargo test -p projection-demo-domain
```

`tests/ledger_projection.rs` drives `BalanceFold` through the real
`ProjectionEngine` and asserts the signed running balance and per-account
partition isolation. `tests/gap_repair.rs` is the host stand-in for the
wasm loops: it proves gap-repair converges, and that a bootstrap from an
*empty* client reconstructs every balance from the server's heads —
including a re-bootstrap after a `/foreign-write` that pulls new rows in
without double-counting the already-held partitions.
