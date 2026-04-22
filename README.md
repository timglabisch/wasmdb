# wasmdb

**A small, fast SQL database written in Rust. Runs in the browser (WASM), runs on the server (optional), keeps them in sync.**

- **Reactive** — subscribe to `SELECT` queries from React; re-render only on the deltas that matter.
- **Compact** — a lean WASM bundle. Real SQL engine, not a wrapper over `IndexedDB`.
- **Commands everywhere** — mutations are Rust code that runs optimistically in the client and authoritatively on the server. Same function, both sides.

---

## Just SQL

```tsx
const users = useQuery("SELECT id, name FROM users ORDER BY name");
```

It's a real SQL engine — joins, group by, aggregates, index lookups, ordering, limits. Written in Rust, compiled to WASM.

## Reactive

```tsx
const orders = useQuery(
  "SELECT reactive(orders.id), id, amount FROM orders WHERE status = 'open'"
);
```

`reactive(col)` marks the subscription's identity. When a mutation touches matching rows, *this* query re-runs — nothing else. Condition-level invalidation, not table-level. No manual cache busting, no `queryClient.invalidateQueries`.

## Server-hosted data, transparent

```tsx
const invoices = useQuery(
  `SELECT invoice.id, invoice.total, customer.name
     FROM invoices.by_customer(:id) AS invoice
     INNER JOIN customers.list()    AS customer
             ON customer.id = invoice.customer_id`,
  { id: 42 },
);
```

`invoices.by_customer(...)` and `customers.list()` are server-hosted fetchers. The engine calls them once over HTTP, lands the rows in the local DB, then plans the query locally. JOINs across local and server tables — just SQL.

## Mutations are Rust commands — running on both sides

```rust
// defined once; compiled into the client (WASM) and the server (native)
impl Command for InvoiceCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            InvoiceCommand::AddPosition { invoice_id, product_id, qty } => {
                let price = lookup_price(db, *product_id)?;
                insert_position(db, *invoice_id, *product_id, *qty, price)?;
                recompute_invoice_total(db, *invoice_id)
            }
        }
    }
}
```

From the UI:

```tsx
await execute({ type: 'AddPosition', invoice_id: 17, product_id: 3, qty: 2 });
// → runs locally against the browser DB; every reactive query updates instantly
// → the same Rust runs on the server; corrections come back as Z-sets and apply cleanly
```

One function, two execution sites. No REST layer, no DTOs, no duplicated validation, no dialect drift. Complex multi-table business logic — derived totals, enforced invariants, cross-entity updates — lives inside the database, not above it.

---

## Why it's different

The pieces exist elsewhere. Reactive SQL in Materialize. Shared mutations in Replicache — in TypeScript, over a KV store. Sync in ElectricSQL — against Postgres, via logical replication and a dedicated service.

wasmdb is reactive SQL **with** shared Rust logic **with** Z-set sync **with** no dedicated sync infrastructure. The server is a plain Axum handler that runs commands and returns Z-sets. Storage is whatever you already have — MySQL, Postgres, in-memory.

---

## Demos

- **`examples/sync-demo/`** — minimal end-to-end: users + orders, React frontend, live queries with JOIN + GROUP BY + COUNT.
- **`examples/invoice-demo/`** — realistic B2B app: six tables, 38 command variants, MySQL-backed server.

```bash
make install     # npm workspaces
make sync-dev    # ts-rs bindings → wasm-pack → vite dev
make sync        # full build: bindings → wasm-pack → vite build → cargo run -p sync-demo-server
```

---

## Architecture

<details>
<summary>Rust crates (<code>crates/</code>)</summary>

| Crate                | Purpose                                                                                              |
|----------------------|------------------------------------------------------------------------------------------------------|
| `sql-parser`         | SQL parser → AST.                                                                                    |
| `sql-engine`         | Planner (logical + physical), executor, column store, Z-sets, bitmap indexes, reactive runtime, query registry. The core. |
| `database`           | Thin `Database` wrapper — register tables, `execute` / `execute_mut`, `apply_zset`.                  |
| `database-reactive`  | `ReactiveDatabase` — subscription dedup, pull-API (`next_dirty`), edge-triggered wake.               |
| `dirty-set`          | Inline-list + overflow-bitmap dirty-set primitive.                                                   |
| `sync`               | Protocol types: `Command` trait, `CommandRequest` / `Response`, `Verdict`.                           |
| `sync-client`        | `SyncClient<C>` — optimistic DB + confirmed DB, stream batching, rollback on `Rejected`.             |
| `sync-server`        | Axum router (`POST /command`) + `ServerState<C>` over the authoritative DB.                          |
| `wasmdb-debug`       | Pure-Rust instrumentation: event log, query traces, notification counters.                           |

Feature flags: `borsh` (wire format), `serde` (debug / JSON), `wasm-timing` (`web-time` in the browser).

</details>

<details>
<summary>Frontend packages (<code>frontend-packages/</code>)</summary>

| Package                  | Purpose                                                                                                     |
|--------------------------|-------------------------------------------------------------------------------------------------------------|
| `@wasmdb/client`         | React hooks (`useWasm`, `useQuery`, `useQueryConfirmed`) + command wrappers. Internal drain pump via `queueMicrotask`. |
| `@wasmdb/debug-toolbar`  | Dev overlay — sync status, subscription list, event log, DB inspector, query traces, performance panel.    |

</details>

---

## Status

Single-author research project. Client engine, planner, incremental view maintenance, sync protocol, and React integration are in place and covered by an end-to-end test suite with plan snapshots. The server side is intentionally thin — a generic Axum handler over a `Mutex<Database>`.

**Not production-ready.** The core works; persistence, server-initiated push, and partial replication are on the roadmap.

## Roadmap

- **Persistence** — OPFS-backed snapshot + command-replay log, for warm-start.
- **Partial replication** — shape-based subscriptions for multi-tenant and large datasets.
- **Server-initiated push** — WebSocket stream of confirmed Z-sets to subscribed clients.
- **Command authorization** — session context threaded through `execute` for row-level policy.
- **Type system** — decimal, date/time, binary alongside the current `I64` / `String` / `Null`.
- **Observability** — OpenTelemetry-style export for the existing span-based tracing.

Contributions and discussion welcome — keep in mind this is early-stage research, not a stable framework.
