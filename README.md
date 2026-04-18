# wasmdb

A local-first SQL database that runs in the browser (Rust → WASM) and syncs
to an authoritative server over a Borsh-encoded command protocol.

Business logic — the mutations themselves — is written **once in Rust** and
executes in **both places**: optimistically on the client (compiled to WASM)
and authoritatively on the server (compiled to native). The same `execute()`
function runs on both sides, against the same SQL engine, producing
Z-set deltas that the protocol carries between them. No duplicated logic,
no two languages, no dialect drift.

Mutations flow as Z-sets — deltas as signed multisets — so optimistic
writes can be rolled back on reject, and server corrections apply as pure
diffs without rebuilding local state.

Live SQL subscriptions are maintained by an incremental view engine on the
client. React components subscribe to `SELECT` queries and re-render only
when a mutation actually affects their result set — condition-level
granularity, not table-level invalidation.

## Idea

Three design choices reinforce each other:

1. **Commands are shared Rust code — one implementation, two execution
   sites.** A `Command` is a Borsh-serializable Rust type implementing
   `execute(&mut Database) -> ZSet`. The *same* impl block runs
   optimistically on the client (compiled to WASM) and authoritatively on
   the server (compiled to native). Business logic — validation, price
   calculation, multi-table updates, whatever — is written once, in one
   language, against one `Database` API. The protocol only ever sees the
   net Z-set that `execute()` produces. A command can be simple (a single
   `INSERT`) or complex (read, compute, mutate across many tables); the
   wire doesn't care. Sketch:

   ```rust
   // defined once; compiled into both wasm and server binaries
   impl Command for UserCommand {
       fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
           match self {
               UserCommand::InsertOrder { id, user_id, amount, .. } => {
                   // same code path runs optimistically on the client
                   // and authoritatively on the server
                   execute_sql(db, "INSERT INTO orders ...", params)
               }
               // ...
           }
       }
   }
   ```

2. **One SQL engine on both sides.** Because commands run in both places,
   the SQL engine has to as well — same planner, same executor, same
   storage semantics. No dialect drift, no "works on client but not on
   server" class of bug.

3. **Z-sets as the wire format.** Every mutation — INSERT, UPDATE, DELETE,
   or an arbitrary user command — produces a Z-set of row deltas
   (`weight = +1` insert, `-1` delete). Optimistic apply is `+Z`, rollback
   is `-Z`, and a server correction is just another Z-set applied on top.
   The server may produce a *different* Z-set than the client's optimistic
   one (validated ID, corrected total, enforced uniqueness); the client
   rebases cleanly via Z-set algebra. No special cases, no manual merge
   logic, no bespoke rebase code.

On top of this, reactive queries use `REACTIVE(...)` markers inside SELECT
statements. The reactive engine indexes each subscription by the equality
constraints it carries, so a mutation surfaces as *"subscription S had
conditions `[2, 4]` triggered"* — precise enough that the UI can both
re-query and visually mark the affected rows, without diffing the result
set manually.

## Status

Single-author research project. The client-side engine, planner (logical +
physical), incremental view maintenance runtime, sync protocol, and React
integration are in place and covered by tests. The server side is
intentionally thin today: a generic Axum handler over a `Mutex<Database>`.

**Not ready for production use.** Persistence, partial replication, and
server-initiated push are on the roadmap; see below.

## Layout

```
crates/              Rust workspace (DB engine + sync)
frontend-packages/   reusable TS packages (@wasmdb/*)
examples/sync-demo/  end-to-end demo (commands, wasm, server, frontend)
```

## Rust crates (`crates/`)

| Crate                | Zweck                                                                                   |
|----------------------|------------------------------------------------------------------------------------------|
| `sql-parser`         | SQL-Parser → AST (`ast.rs`, `parser/`, `schema.rs`).                                    |
| `sql-engine`         | Planner (logical + physical), Executor, Storage, Z-Set, Bitmap-Indexe, reactive Runtime + Query-Registry. Kern. |
| `database`           | Dünner `Database`-Wrapper um `sql-engine` (Tabellen anlegen, `execute`/`execute_mut`, `apply_zset`). |
| `database-reactive`  | `ReactiveDatabase` + Subscription-Dedup + Pull-API (`next_dirty`) mit Edge-triggered Wake. |
| `dirty-set`          | Inline-List + Overflow-Bitmap Dirty-Set als eigenständiges Primitiv.                    |
| `sync`               | Protokoll-Typen: `Command`-Trait, `CommandRequest/Response`, `Verdict`, Z-Set re-export.|
| `sync-client`        | `SyncClient<C>`: optimistic DB + confirmed DB, Stream-Batching, Rollback bei `Rejected`.|
| `sync-server`        | Axum-Router (`POST /command`) + `ServerState<C>` mit autoritativer DB.                  |
| `wasmdb-debug`       | Pure-Rust Instrumentation: Event-Log, Query-Traces, Notification-Counter.               |

Feature-Flags: `borsh` (Wire-Format), `serde` (Debug/JSON), `wasm-timing`
(`web-time` statt `std::time` im Browser).

## Frontend-Packages (`frontend-packages/`)

| Paket                    | Zweck                                                                            |
|--------------------------|----------------------------------------------------------------------------------|
| `@wasmdb/client`         | React-Hooks (`useWasm`, `useQuery`, `useQueryConfirmed`) + Command-Wrapper. Interner Drain-Pump über `queueMicrotask`. |
| `@wasmdb/debug-toolbar`  | Dev-Overlay: Sync-Status, Subscriptions, Event-Log, DB-Inspector, Query-Traces, Performance-Panel. |

Beide werden über npm-Workspaces (`package.json` im Root) eingebunden.

## Demo (`examples/sync-demo/`)

| Ordner      | Inhalt                                                                                      |
|-------------|----------------------------------------------------------------------------------------------|
| `commands/` | `UserCommand`-Enum (Insert/Update/Delete auf `users`/`orders`), `Command`-Impl, `ts-rs`-Export.|
| `wasm/`     | `cdylib` mit `wasm-bindgen`-API (`execute`, `query`, `subscribe`, Streams, Debug).          |
| `server/`   | Axum-Binary auf `:3123`, nutzt `sync-server` + serviert das gebaute Frontend.               |
| `frontend/` | React 19 + Vite, Live-Queries (LEFT JOIN + GROUP BY + COUNT) + `DebugToolbar`.              |

## Build / Run

```bash
make install     # npm workspaces
make sync-dev    # ts-rs bindings → wasm-pack → vite dev
make sync        # bindings → wasm-pack → vite build → cargo run -p sync-demo-server
```

`sync-types` generiert `UserCommand.ts` aus dem Rust-Enum via `ts-rs`
(`cargo test -p sync-demo-commands`) und kopiert es nach
`frontend/src/generated/`.

## Roadmap

The client-side engine is the mature part of the project. The next
investments are on the server and persistence sides, and on making
subscriptions scale to real-world datasets:

- **Persistence.** OPFS-backed snapshot + command replay log, so clients
  warm-start without re-fetching everything.
- **Partial replication.** Shape-based subscriptions so a client holds
  only the slice of data it actually needs — a prerequisite for
  multi-tenant and large-dataset use.
- **Server-initiated push.** WebSocket stream of confirmed Z-sets from
  server to subscribed clients, for multi-client coherence without
  polling.
- **Command authorization.** Session context threaded through
  `Command::execute` so the server can enforce row-level policy inside
  the same Rust function that produces the Z-set.
- **Type system.** Decimal, date/time, and binary alongside the current
  `I64`/`String`/`Null`.
- **Observability.** Extend the existing span-based tracing in
  `sql-engine` and the reactive runtime with exportable formats
  (OpenTelemetry-style) for production debugging.

Contributions, issues, and discussion are welcome — but keep in mind this
is early-stage research, not a stable framework.
