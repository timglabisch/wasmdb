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
  `seq` / `committed` / `payload` bookkeeping. The payload holds the
  domain event `EntryPosted`.
- **`#[rpc_command]`** (`.../command/post_entry.rs`) — `PostEntry` is a
  *request*, not a log row. Its `execute_optimistic` builds the log row
  directly (`sync::append::{next_seq, append_row}` + `payload_json`) and
  appends an `EntryPosted` event. Appending is an effect the command
  performs, not the command's identity — so any other entry point (an HTTP
  API, an MCP tool) can perform the same append.
- **`#[projection]`** (`.../balance_fold.rs`) — `BalanceFold` is
  implemented *on its own state type* (the cqrs-es Aggregate idiom):
  `apply` replays one log row, `render` projects the accumulated state
  into the derived `balance` table. The engine folds each account's
  committed prefix once and memoizes it (design §9.3).
- **Optimistic → committed** — the confirm-server (`server/src/lib.rs`)
  flips the appended row's `committed` flag; the UI shows entries as
  `pending` until the server confirms them.

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
partition isolation.
