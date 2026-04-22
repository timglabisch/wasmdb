# wasmdb

**A reactive SQL database with realtime business logic, authoritative sync, and no dedicated server infrastructure — one Rust engine, from browser to backend.**

Mutations are Rust `Command` types. They execute optimistically against a SQL database in the browser (compiled to WASM) and authoritatively against the same SQL database on the server (compiled to native) — literally the same `execute()` function, against the same engine, producing the same Z-set deltas.

```rust
// defined once — compiled into the client (WASM) and the server (native)
impl Command for InvoiceCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            InvoiceCommand::AddPosition { invoice_id, product_id, qty } => {
                // read product price, insert position, update invoice total —
                // multi-table business logic running in both places
                let price = lookup_price(db, *product_id)?;
                insert_position(db, *invoice_id, *product_id, *qty, price)?;
                recompute_invoice_total(db, *invoice_id)
            }
            // ...
        }
    }
}
```

The protocol only ever sees the net Z-set. Rollback is negation. Server corrections are another Z-set applied on top. No duplicated logic, no two languages, no dedicated sync engine.

---

## Live SQL, on both sides of the wire

The same SQL engine powers the browser — so live queries are just SQL. Tables come from two sources: the local database (mutated by commands) and *callers*, server-hosted fetchers that the engine invokes transparently. `reactive(col)` markers tell the engine which columns carry the subscription's identity, so invalidation is condition-level, not table-level.

```tsx
// live query — plans and executes against the browser's SQL engine
const invoices = useQuery(
  `SELECT reactive(invoice.id),
          invoice.id, invoice.total, invoice.status, customer.name
     FROM invoices.by_customer(:customer_id) AS invoice
     INNER JOIN customers.list() AS customer
             ON customer.id = invoice.customer_id
    WHERE invoice.status = :status
    ORDER BY invoice.id DESC
    LIMIT 50`,
  { customer_id: 42, status: 'open' },
  row => ({ id: row[1], total: row[2], status: row[3], customer: row[4] }),
);

// fire a command — same Rust code that runs authoritatively on the server
await execute({ type: 'AddPosition', invoice_id: 17, product_id: 3, qty: 2 });
```

What the engine does:

1. **First render.** `invoices.by_customer(42)` and `customers.list()` are callers. The engine resolves them with one HTTP fetch, lands the rows in the local DB, then plans and runs the query locally.
2. **Command fires.** `AddPosition` runs against the local DB, producing a Z-set. The reactive index sees "invoice 17 changed"; the query re-runs; the UI updates in the next microtask.
3. **Server confirms.** The same Rust code runs against the server's database. Its Z-set — possibly with a corrected total — comes back and applies cleanly. One more reactive hit, one more render, authoritative state.
4. **On rejection.** The optimistic Z-set is negated; the subscription re-fires once with the pre-command state.

UI code never calls a fetch, never invalidates a cache, never handles a conflict. It subscribes to SQL; SQL updates.

---

## Four pillars

**Reactive SQL.** Subscribe to `SELECT` queries; re-render only when a mutation actually affects the result. Condition-level invalidation, not table-level. The incremental view-maintenance runtime runs in the browser.

**Realtime Business Logic.** Commands are Rust types with an `execute(&mut Database) -> ZSet` method. Validation, multi-table updates, derived totals — written once, run everywhere. Complex business rules live *in* the database layer, not above it.

**Authoritative Corrections.** The server may produce a different Z-set than the client's optimistic guess — server-assigned IDs, recomputed totals under concurrent edits, enforced uniqueness. The client rebases cleanly via Z-set algebra. Rollback is negation; rebase is arithmetic. No bespoke merge logic.

**Sync Without Infrastructure.** No CDC pipeline, no logical replication, no dedicated sync service. The server is an Axum handler that runs your commands and returns Z-sets. Storage-agnostic: pair it with MySQL, Postgres, an in-memory store, or whatever your existing backend happens to be.

---

## Why this combination matters

Each pillar exists somewhere. Reactive SQL exists in Materialize. Shared mutation logic exists in Replicache, in TypeScript. Sync exists in ElectricSQL, against Postgres. The *combination* — reactive SQL with shared Rust logic and zero backend rewrite — is what we haven't found elsewhere.

|                       | Reactive SQL      | Shared Logic   | Storage-agnostic  | No dedicated sync infra |
|-----------------------|-------------------|----------------|-------------------|-------------------------|
| Materialize           | ✓                 | –              | –                 | – (*is* the infra)      |
| Replicache / Zero     | partial (KV-ish)  | ✓ (TypeScript) | ✓                 | –                       |
| ElectricSQL           | –                 | –              | Postgres only     | –                       |
| PowerSync             | –                 | –              | partial           | –                       |
| **wasmdb**            | ✓                 | ✓ (Rust)       | ✓                 | ✓                       |

The commercial consequence is adoption friction. *"Switch your Postgres to logical replication and run our service"* is a Friday-afternoon blocker at most companies. *"Mount a handler on your existing backend"* is a Friday-afternoon demo.

---

## A full roundtrip, in one paragraph

A user clicks "Add Position" on an invoice. The `AddPosition` command runs locally against the browser's SQL database, producing a Z-set delta. The relevant live query fires immediately; the invoice total updates in the UI. In parallel, the same command goes to the server, runs against the authoritative database, and comes back confirmed — possibly with corrections (a server-assigned primary key, a recomputed total under concurrent edits, an enforced uniqueness constraint). The client rebases against the server's Z-set. The UI settles on the final truth. Every step runs Rust; nothing crosses a language boundary; the wire carries only Z-sets.

---

## What you'd build with it

- **B2B SaaS** — invoicing, CRM, ERP, practice-management tools — that feel like Linear rather than like SAP.
- **Multi-user dashboards** where updates land in the UI with sub-frame latency.
- **Offline-capable field tools** — construction sites, trains, patchy networks — where optimistic writes reconcile later.
- **Internal platforms** layered on top of an existing database, without an infrastructure migration.
- **Products where UX itself is the moat**, because competitors with REST/GraphQL backends can't close the latency gap without rebuilding.

---

## How it works

Three design choices reinforce each other:

### 1. Commands are shared Rust code — one implementation, two execution sites

A `Command` is a Borsh-serializable Rust type implementing `execute(&mut Database) -> ZSet`. The *same* impl block runs optimistically on the client (compiled to WASM) and authoritatively on the server (compiled to native). Business logic — validation, price calculation, multi-table updates — is written once, in one language, against one `Database` API. The protocol only ever sees the net Z-set that `execute()` produces. A command can be simple (a single `INSERT`) or arbitrarily complex (read, compute, mutate across many tables); the wire doesn't care.

### 2. One SQL engine on both sides

Because commands run in both places, the SQL engine has to as well — same parser, same planner, same executor, same storage semantics. No dialect drift, no "works on client but not on server" class of bug. The engine is a column store with bitmap null encoding, a three-phase planner (requirement resolution, physical plan, reactive plan), and an executor pipeline that handles scans, joins (nested-loop and index-lookup), filters, group-by/aggregates, ordering, and limits.

### 3. Z-sets as the wire format

Every mutation — INSERT, UPDATE, DELETE, or an arbitrary user command — produces a Z-set of row deltas (`weight = +1` insert, `weight = -1` delete). Optimistic apply is `+Z`, rollback is `-Z`, and a server correction is just another Z-set applied on top. The server may produce a *different* Z-set than the client's optimistic one; the client rebases via Z-set algebra. No special cases, no manual merge logic, no bespoke rebase code.

### Reactive subscriptions

Live queries use `REACTIVE(...)` markers inside SELECT statements. The reactive engine indexes each subscription by the equality constraints it carries, so a mutation surfaces as *"subscription S had conditions `[2, 4]` triggered"* — precise enough that the UI can both re-query and visually mark the affected rows, without diffing the result set manually.

---

## Architecture

### Rust crates (`crates/`)

| Crate                | Purpose                                                                                              |
|----------------------|------------------------------------------------------------------------------------------------------|
| `sql-parser`         | SQL parser → AST (`ast.rs`, `parser/`, `schema.rs`).                                                 |
| `sql-engine`         | Planner (logical + physical), executor, column store, Z-sets, bitmap indexes, reactive runtime, query registry. The core. |
| `database`           | Thin `Database` wrapper over `sql-engine` — register tables, `execute`/`execute_mut`, `apply_zset`. |
| `database-reactive`  | `ReactiveDatabase` — subscription deduplication, pull-API (`next_dirty`), edge-triggered wake.       |
| `dirty-set`          | Inline-list + overflow-bitmap dirty-set as a standalone primitive.                                   |
| `sync`               | Protocol types: `Command` trait, `CommandRequest` / `Response`, `Verdict`, Z-set re-export.          |
| `sync-client`        | `SyncClient<C>` — optimistic DB + confirmed DB, stream batching, rollback on `Rejected`.             |
| `sync-server`        | Axum router (`POST /command`) + `ServerState<C>` over the authoritative DB.                          |
| `wasmdb-debug`       | Pure-Rust instrumentation — event log, query traces, notification counters.                          |

Feature flags: `borsh` (wire format), `serde` (debug/JSON), `wasm-timing` (`web-time` in the browser).

### Frontend packages (`frontend-packages/`)

| Package                  | Purpose                                                                                                     |
|--------------------------|-------------------------------------------------------------------------------------------------------------|
| `@wasmdb/client`         | React hooks (`useWasm`, `useQuery`, `useQueryConfirmed`) + command wrappers. Internal drain pump via `queueMicrotask`. |
| `@wasmdb/debug-toolbar`  | Dev overlay — sync status, subscription list, event log, DB inspector, query traces, performance panel.     |

Both are wired in via npm workspaces (`package.json` at the repo root).

---

## Demos

### `examples/sync-demo/` — minimal end-to-end

| Folder      | Content                                                                                                   |
|-------------|-----------------------------------------------------------------------------------------------------------|
| `commands/` | `UserCommand` enum (insert/update/delete on `users` / `orders`), `Command` impl, `ts-rs` export.           |
| `wasm/`     | `cdylib` with a `wasm-bindgen` API (`execute`, `query`, `subscribe`, streams, debug).                      |
| `server/`   | Axum binary on `:3123`, uses `sync-server` and serves the built frontend.                                  |
| `frontend/` | React 19 + Vite, live queries (LEFT JOIN + GROUP BY + COUNT) and the `DebugToolbar`.                       |

### `examples/invoice-demo/` — realistic business app

Six tables (customers, invoices, products, recurring, SEPA mandates, activity log), 38 command variants (`CreateCustomer`, `AddPosition`, `MovePosition`, …), MySQL on the server side — demonstrates the storage-agnostic story in practice and the kind of business-logic density the `Command` model was designed for.

---

## Build / Run

```bash
make install     # npm workspaces
make sync-dev    # ts-rs bindings → wasm-pack → vite dev
make sync        # bindings → wasm-pack → vite build → cargo run -p sync-demo-server
```

`sync-types` generates `UserCommand.ts` from the Rust enum via `ts-rs` (`cargo test -p sync-demo-commands`) and copies it into `frontend/src/generated/`.

---

## Status

Single-author research project. The client-side engine, planner (logical + physical), incremental view-maintenance runtime, sync protocol, and React integration are in place and covered by an end-to-end test suite with plan snapshots. The server side is intentionally thin today — a generic Axum handler over a `Mutex<Database>`.

**Not ready for production.** The four pillars are real and working; the polish, the persistence story, and several production concerns are not.

---

## Roadmap

The client-side engine is the mature part of the project. The next investments are on the server, on persistence, and on making subscriptions scale to real-world datasets:

- **Persistence.** OPFS-backed snapshot + command-replay log, so clients warm-start without re-fetching everything.
- **Partial replication.** Shape-based subscriptions so a client holds only the slice of data it actually needs — a prerequisite for multi-tenant and large-dataset use.
- **Server-initiated push.** WebSocket stream of confirmed Z-sets from server to subscribed clients, for multi-client coherence without polling.
- **Command authorization.** Session context threaded through `Command::execute` so the server can enforce row-level policy inside the same Rust function that produces the Z-set.
- **Type system.** Decimal, date/time, and binary alongside the current `I64` / `String` / `Null`.
- **Observability.** Extend the existing span-based tracing in `sql-engine` and the reactive runtime with exportable formats (OpenTelemetry-style) for production debugging.

Contributions, issues, and discussion are welcome — keep in mind this is early-stage research, not a stable framework.
