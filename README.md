# wasmdb

In-browser SQL database (Rust → WASM) with optimistic client / authoritative
server sync over a Borsh-encoded command protocol. Updates are expressed as
Z-sets (delta multisets) so optimistic mutations can be replayed and rejections
can be rolled back without rebuilding the whole DB. The React frontend
subscribes to live SQL queries driven by a reactive query engine in the WASM
crate.

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
| `sql-engine`         | Planner, Executor, Storage, Z-Set, Bitmap-Indexe, reactive Query-Registry. Kern.        |
| `database`           | Dünner `Database`-Wrapper um `sql-engine` (Tabellen anlegen, `execute`/`execute_mut`).  |
| `database-reactive`  | `ReactiveDatabase` + Subscriptions (`SubId`, `Callback`) für Live-Queries.              |
| `sync`               | Protokoll-Typen: `Command`-Trait, `CommandRequest/Response`, `Verdict`, Z-Set re-export.|
| `sync-client`        | `SyncClient<C>`: optimistic DB + confirmed DB, Stream-Batching, Rollback bei `Rejected`.|
| `sync-server`        | Axum-Router (`POST /command`) + `ServerState<C>` mit autoritativer DB.                  |
| `wasmdb-debug`       | serde-freundliche Debug-Snapshots für die Debug-Toolbar.                                |

Feature-Flags: `borsh` (Wire-Format), `serde` (Debug/JSON), `wasm-timing`
(`web-time` statt `std::time` im Browser).

## Frontend-Packages (`frontend-packages/`)

| Paket                    | Zweck                                                                            |
|--------------------------|----------------------------------------------------------------------------------|
| `@wasmdb/client`         | React-Hooks (`useWasm`, `useQuery`, `useQueryConfirmed`) + Command-Wrapper.      |
| `@wasmdb/debug-toolbar`  | Dev-Overlay: Sync-Status, Subscriptions, Query-Traces, DB-Inspector.             |

Beide werden über npm-Workspaces (`package.json` im Root) eingebunden.

## Demo (`examples/sync-demo/`)

| Ordner      | Inhalt                                                                                      |
|-------------|----------------------------------------------------------------------------------------------|
| `commands/` | `UserCommand`-Enum (Insert/Update/Delete auf `users`/`orders`), `Command`-Impl, `ts-rs`-Export.|
| `wasm/`     | `cdylib` mit `wasm-bindgen`-API (`execute`, `query`, `subscribe`, Streams, Debug).          |
| `server/`   | Axum-Binary auf `:3123`, nutzt `sync-server` + serviert das gebaute Frontend.               |
| `frontend/` | React 19 + Vite, Panels + `DebugToolbar`, konsumiert `wasm-pkg/` via `sync.ts`.             |

## Build / Run

```bash
make install     # npm workspaces
make sync-dev    # ts-rs bindings → wasm-pack → vite dev
make sync        # bindings → wasm-pack → vite build → cargo run -p sync-demo-server
```

`sync-types` generiert `UserCommand.ts` aus dem Rust-Enum via `ts-rs`
(`cargo test -p sync-demo-commands`) und kopiert es nach
`frontend/src/generated/`.
