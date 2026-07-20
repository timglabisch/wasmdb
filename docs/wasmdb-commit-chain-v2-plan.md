# Commit-Chain v2 — Umsetzungsplan (Handoff)

**Status (2026-07-20): Schritt 1 (Framework) ✅, Schritt 2 (Demo-Prototyp) ✅
und Schritt 3 / Stufe 2 (Backward-Refetch/Rebuild) ✅ umgesetzt und
verifiziert. Nichts mehr offen.** Verifikation grün: `cargo test -p
tables-e2e` (27), `cargo test -p projection-demo-domain` (6: +2 `gap_repair`),
`cargo test -p sync-client` (5 `repair`), `cargo build --workspace`,
`wasm-pack build …`, UI `tsc --noEmit` + `vite build`, Live-Smoke-Test der
`/fetch`-Route (borsh-Roundtrip). Nichts committet.

**Zweck:** Selbsttragender Plan, damit die Umsetzung nach einem Context-Clear
ohne Rückfragen fortgesetzt werden kann. Das **Modell** steht autoritativ in
`docs/wasmdb-projections-design.md` **§11** — dieser Plan ist nur die
Umsetzungs-Reihenfolge + Code-Anker. Bei Widerspruch gilt §11.

Sprache mit dem User: **Deutsch**. Er ist Maintainer von wasmdb.

---

## 0. Ziel in einem Satz

`committed`-Flag (0/1) und client-`seq` als Order-Mechanismus **ersetzen**
durch zwei Parent-Links pro Log-Row: `client_parent_id` (Client, optimistische
Kette) + `server_parent_id` (Server, autoritative Kette). `committed` =
`server_parent_id.is_some()`. Drift = `client_parent_id != server_parent_id`.

## 1. Modell (Kurzfassung — Details §11)

Row-Schema (v2):
```
command_id       : Uuid          (PK)
<partition>      : ...           (z.B. account / doc_id)
client_parent_id : Uuid          (Client: ROOT | vorherige lokale Row der Partition)
server_parent_id : Option<Uuid>  (Server: None=pending · ROOT=erste · Some(x)=nach x)
payload          : String        (serialisiertes Event, unverändert)
```
- **Bestätigt** = `server_parent_id.is_some()`. `committed: i64` entfällt.
- **Fold-Order** = `server_parent_id`-Kette ab ROOT (committed) ++
  `client_parent_id`-Kette ab Client-Head (pending Tail).
- **`seq: i64` entfällt** als Mechanismus (kein Framework-Anzeigeindex).
- **Invariante:** `server_parent_id` vergibt AUSSCHLIESSLICH der Server.

## 2. Ausgangsstand (bei Planerstellung)

- §11 + Brücke in §4.6 sind im Design-Doc (uncommitted, working tree).
- Frühere `committed`-Flip-Demo-Änderung ist **nicht** mehr im Working Tree
  (reverted) — Demo wird in Schritt 2 gegen v2 neu gebaut.
- **De-risk erledigt:** `#[row]` unterstützt `Option<Uuid>` nativ
  (`crates/tables/tables-macros/src/lib.rs:168` — `OptUuid` → `Some`=Uuid,
  `None`=`CellValue::Null`). Kein Blocker.
- `CellValue` hat `Null`-Variante (`crates/core/sql-engine/src/storage.rs:69`)
  und `Uuid([u8;16])` (`:65`).

## 3. Gelockte Entscheidungen (Defaults, mit User bestätigt bzw. vorgeschlagen)

- **ROOT-Sentinel** = nil-Uuid (`0000…`).
- **`seq` fällt ganz weg** (Debug-Feld: NICHT behalten, sofern User nicht
  widerspricht — offener Punkt beim letzten Checkpoint).
- **Accessor-Namen:** `client_parent_id()` / `server_parent_id()`.
- **`server_parent_id` nur vom Server** gesetzt.

## 4. Reihenfolge

1. **Framework** (`tables` / `tables-macros` / `database-projection` / `sync`)
2. **Demo-Prototyp** (`examples/projection-demo`)
3. **Stufe 2:** Backward-Refetch / Rebuild (fetch-by-PK-Endpoint + Repair-Loop)

---

## Schritt 1 — Framework (file-by-file)

| # | Datei / Ort | Änderung |
|---|---|---|
| 1 | `crates/tables/tables-macros/src/lib.rs` — `#[projection_row]` | generiert `client_parent_id: Uuid` + `server_parent_id: Option<Uuid>` statt `seq: i64` + `committed: i64` |
| 2 | `crates/tables/tables/src/lib.rs` — `trait ProjectionLog` | Accessoren `client_parent_id() -> Uuid` / `server_parent_id() -> Option<Uuid>` statt `seq()`/`committed()`; `is_committed()` = `server_parent_id().is_some()`; `in_fold_order()` = Ketten-Traversal (PK→Row-Map, ab ROOT server-Links folgen; dann pending [server_parent_id=None] über client-Links ab Head). Anker: `is_committed` ~L75, `in_fold_order` ~L95. |
| 3 | Fold-Shim `tables-macros/src/lib.rs` (~L737–775) + `FoldSnapshot` `crates/core/database-projection/src/typed.rs:44` | Memo-Key = server-chain **PK-Liste** (ab ROOT) statt committed-seq-Liste; `__committed_len` = Länge der server-Kette; `starts_with`-Resume analog (immutable per Kettenposition). |
| 4 | `crates/sync/sync/src/append.rs` | `next_seq` (max-seq-Scan) → `client_head(partition) -> Uuid` (findet den Ketten-Tail: die Partition-Row, die von keinem `client_parent_id` referenziert wird; leer ⇒ ROOT). `append_row` unverändert. |
| 5 | `crates/tables/tables-e2e/tests/{projection_log.rs, projection_fold.rs, projection_fold_incremental.rs}` | Fixtures + Append-Helfer auf neue Spalten; `SetLinePrice`-Payload bleibt, aber `append()`-Helfer setzt client/server-Parent. |

**Verifikation Schritt 1:**
```
cargo test -p tables-e2e
cargo build -p tables -p tables-macros -p database-projection -p sync
```

## Schritt 2 — Demo-Prototyp (`examples/projection-demo`)

| Ort | Änderung |
|---|---|
| `shared/domain/src/ledger/ledger_log.rs` | `LedgerLog` via `#[projection_row]` → neue Spalten automatisch; `EntryPosted` bleibt |
| `shared/domain/src/ledger/command/post_entry.rs` | `execute_optimistic`: `client_parent_id = client_head(partition)`, `server_parent_id = None`, append. `ServerCommand::execute_server`: setzt `server_parent_id` (siehe Server-Head-Map) — demo-lokales Trait bleibt (kein Crate-Touch) |
| `shared/domain/src/lib.rs` | `ServerCommand`-Trait + Enum-Dispatch (wie zuletzt), Doku |
| `shared/domain/src/ledger/balance_fold.rs` | `apply` unverändert (liest Event aus payload); Order kommt aus dem Shim |
| `server/src/lib.rs` | **Server-Head-Map** `HashMap<account, head_PK>` (minimal State, kein DB); `execute_server` setzt `server_parent_id = aktueller Kopf`, Kopf = neue Row. **Out-of-order** = Bestätigungen in anderer Reihenfolge verketten (z.B. per Trigger/verzögert) |
| `frontend/apps/ui/src/App.tsx` | Log-Zeile: pending/committed via `server_parent_id`; **Drift** anzeigen (`client_parent_id != server_parent_id`) |
| `frontend/apps/ui/src/{commands.ts, seed.ts}` | ggf. anpassen |

**Verifikation Schritt 2:**
```
cargo test -p projection-demo-domain
make projection-demo-types    # falls Command-Shape sich ändert
wasm-pack build examples/projection-demo/frontend/apps/wasm --target web --out-dir pkg
```

## Schritt 3 — Stufe 2 (Backward-Refetch/Rebuild) ✅

Umgesetzt (§11.4). Dateien:

| Ort | Änderung |
|---|---|
| `crates/sync/sync/src/protocol.rs` | `FetchRowsRequest { table, ids: Vec<Uuid> }` + `FetchRowsResponse { rows: ZSet }` (borsh) — generische Fetch-by-PK-Wire |
| `crates/sync/sync-client/src/repair.rs` | `missing_parents(db, table)` — Gap-Frontier (committete `server_parent_id`s, die der Client nicht als PK hält; ohne ROOT, dedup, sortiert). Host-getestet (5), entkoppelt wie `sync::append` |
| `crates/sync/sync-client/src/wasm/api.rs` | `#[wasm_bindgen] async fn repair_chain(table, fetch_path)` — Loop: Frontier → POST fetch → `apply_zset` (re-fold) → wiederholen; selbstterminierend |
| `crates/sync/sync-client/src/wasm/stream.rs` | `do_fetch` → generisches `post_bytes(path, body)` (geteilter Transport) |
| `examples/projection-demo/shared/domain/src/lib.rs` | `ServerHeads` → **`ServerLog`** (heads + `rows: HashMap<Uuid, Vec<CellValue>>`): `record`, `fetch(ids) -> ZSet`, `seed_chain(account, &[(id, cents)])` |
| `.../shared/domain/src/ledger/command/post_entry.rs` | `execute_server` nimmt `&mut ServerLog`, stempelt `server_parent_id` **und** `record`et die committete Row |
| `examples/projection-demo/server/src/lib.rs` | `POST /fetch` (fetch-by-PK) auf `Arc<Mutex<ServerLog>>`; `seed_carol` legt eine Vorgeschichte (`0xca…`, +€30 / −€5) an, die kein Client hält |
| `frontend-packages/client/src/index.ts` | `repairChain(table, fetchPath='/fetch')`-Wrapper + `repair_chain?`-Surface |
| `.../frontend/apps/ui/src/commands.ts` | `post()` ruft nach `.confirmed` → `repairChain('ledger_log')` |
| `.../shared/domain/tests/gap_repair.rs` | End-to-end Host-Test: Gap → 2 Runden Backward-Refetch → Fold recovered |

**Demonstration:** Poste eine Entry auf `carol` → der Server verlinkt sie
hinter carols geseedeter (client-unbekannter) Kette → der committete Row hat
ein unbekanntes `server_parent_id` (Gap + Drift) → `repairChain` fetcht die
zwei Ancestor-Rows rückwärts nach → Balance re-foldet auf die volle
Historie. Rebuild ist gratis: `apply_zset` re-foldet die Partition.

### Nachtrag — Bootstrap-from-Server + Live-Cross-Writer (Demo-Umbau)

Ausgelöst durch zwei Beobachtungen: (a) `repairChain` feuerte im normalen
Flow nie (ein Single-Client erzeugt keine Lücke), (b) F5 / zweiter Tab
löschte den State — nicht der Server (die `ServerLog` überlebt prozesslang),
sondern der **Client** bootstrappte nie; er re-seedete nur hardcoded
alice/bob. Fix: der Client rekonstruiert seinen State aus dem Server, und das
*ist* ein Gap-Repair (leerer Client = maximale Lücke).

| Ort | Änderung |
|---|---|
| `crates/sync/sync/src/protocol.rs` | `HeadsRequest { table }` + `HeadsResponse { ids: Vec<Uuid> }` |
| `crates/sync/sync-client/src/repair.rs` | `unknown_ids(db, table, ids)` — filtert bereits gehaltene PKs (idempotenter Re-Bootstrap, kein Doppel-Fold). +2 Tests |
| `crates/sync/sync-client/src/wasm/api.rs` | `bootstrap(table, heads_path, fetch_path)`: Heads holen → unbekannte fetchen+applyen → `walk_gap_to_root` (aus `repair_chain` extrahiert, geteilt) |
| `.../shared/domain/src/lib.rs` | `ServerLog::heads() -> Vec<Uuid>`, `foreign_write(account, count)` (Out-of-band-Burst, `0xcf…`-ids, Muster `[1500,-400,900]`), Feld `foreign_seq` |
| `examples/projection-demo/server/src/lib.rs` | `POST /heads`, `POST /foreign-write`; `seed_carol` → `seed` (alice/bob/carol **alle** serverseitig) |
| `frontend-packages/client/src/index.ts` | `bootstrap(table, headsPath='/heads', fetchPath='/fetch')`-Wrapper + `bootstrap?`-Surface |
| `.../frontend/apps/ui/src/commands.ts` | `foreignWriteCarol()` = `POST /foreign-write` → `bootstrap('ledger_log')` |
| `.../frontend/apps/ui/src/App.tsx` | Client-`seed()` entfällt → `bootstrap('ledger_log')` beim Load; „simulate another writer → carol"-Button + Repair-Status |
| `.../shared/domain/tests/gap_repair.rs` | +2 Tests: Bootstrap-from-empty rekonstruiert alle Balances; Re-Bootstrap nach `foreign_write` zieht neue Rows ohne Doppelzählung |

- **Boot:** `/heads` → `[alice, bob, carol-Head]` → fetch+apply → jede Kette
  bis ROOT gewalkt → alle Balances stehen bei erstem Paint. **F5 ist
  verlustfrei** (auch Ad-hoc-Posts, da der Server sie `record`et hat).
- **Live-Gap:** der Button schiebt carols Head out-of-band vor; `bootstrap`
  sieht einen unbekannten Head → Backward-Walk zieht den Burst nach →
  Balance springt, ohne Reload. Verifiziert über echtes HTTP (`/heads`
  liefert den `0xcf…`-Head, `/fetch` die Row mit `account: carol`).

---

## Umsetzungs-Entscheidungen (in Schritt 1/2 getroffen)

- **`seq` ganz weg** — kein Debug-/Anzeigefeld (Default umgesetzt).
- **`command_id` MUSS `Uuid` sein** — `#[projection_row]` erzwingt es mit
  gezielter Fehlermeldung (die Parent-Links referenzieren `command_id`).
- **`client_head`** findet den Tail = Partition-Row ohne eingehenden
  `client_parent_id`-Verweis; leere Partition ⇒ `ROOT_PARENT`; bei Fork das
  max-PK (deterministisch). Nil-Sentinel lokal in `append.rs` definiert
  (`sync` bleibt von `tables` entkoppelt, sucht Spalten per Name).
- **`in_fold_order`** = Ketten-Traversal mit Cycle-Guard; unerreichbare Rows
  (Gap/Drift) werden deterministisch ans Ende gehängt statt zu paniken
  (Repair = Stufe 2).
- **Memo-Test** `reorder_of_the_committed_chain_folds_from_zero` modelliert
  Reorder via `apply_zset` (delete alte + insert re-verlinkte Row) — ersetzt
  den v1 `backfill_behind_the_frontier`-Test.
- **`ServerHeads`** = demo-lokaler `HashMap<account, head_PK>`, im
  Confirm-Server als `Arc<Mutex<_>>` über `axum::State`; `execute_server`
  bekommt `&mut ServerHeads`. (Stufe 2 hat das zu **`ServerLog`** erweitert:
  heads + Row-Store; `execute_server` nimmt jetzt `&mut ServerLog`.)
- **UI** rekonstruiert die Ketten-Order clientseitig (`chainOrder`, spiegelt
  §11.3), da es kein sortierbares `seq` mehr gibt; Drift-Badge bei
  `client_parent_id != server_parent_id`.

## Stufe-2-Entscheidungen (getroffen)

- **Gap erzeugen ohne Multi-Client-Streaming:** der Confirm-Server bekommt
  einen autoritativen Row-Store (für Stufe 2 ohnehin nötig, um fetch-by-PK
  zu bedienen) und wird mit einer `carol`-Vorgeschichte geseedet, die kein
  Client hält. Das erste Posten auf `carol` löst den Gap deterministisch aus
  — ehrliches Modell von „ein anderer Writer war zuerst da". Kein SSE/Broadcast.
- **Repair frontend-getrieben** (nach `.confirmed`), nicht im Core
  `receive_response` (sync/non-wasm, mit Tests geteilt). Doku hält fest, dass
  produktiv der Confirm-Chokepoint der Ort wäre.
- **`in_fold_order` Gap-Fallback bleibt** (unerreichbare Rows ans Ende) — er
  ist die korrekte Übergangsanzeige, *bis* der Repair-Loop die Ancestors
  nachgefetcht hat; danach ist die Kette lückenlos und der Fallback greift
  nicht mehr.
- **`missing_parents` ignoriert pending Rows** (`server_parent_id = None`):
  nur committete Rows tragen einen autoritativen Vorgänger zum Backfill.

## Nachtrag — Codegen-Parität (Runtime-Bug, behoben)

`#[projection_row]` wird an **zwei** Stellen expandiert und beide müssen die
gleichen Spalten erzeugen:

1. `tables-macros` (Proc-Macro) — für die kompilierte Domain (Server/Host,
   `LedgerLog::schema()` / `into_cells`).
2. `tables-codegen` (`parse.rs::parse_projection_row`) — für die
   **Client-Registrierung** (`register_all_tables` im wasm-`build.rs`).

Schritt 1 hatte nur (1) auf v2 gezogen; (2) hängte weiter `seq`/`committed`
an → der Client registrierte `ledger_log` mit den alten Spalten, während der
optimistische Insert die neuen Zellen lieferte. Runtime-Symptome: `subscribe:
unknown column: ledger_log.client_parent_id` und beim Insert `type mismatch`.
Fix: `parse_projection_row` erzeugt jetzt `client_parent_id: Uuid` /
`server_parent_id: Option<Uuid>` / `payload: String` (+ Codegen-Tests +
Fixture `command_id: Uuid`). **Merke:** projection_row-Änderungen immer in
beiden Expansions spiegeln.

## Tooling / Gotchas

- **rust-analyzer** meldet für makro-generierte Felder wiederholt falsche
  „no such field" / „proc-macro map is missing" — **ignorieren**, `cargo` ist
  maßgeblich (grüner Build zählt).
- Vorbestehend & unabhängig: `invoice-demo-server` `composite_pk`-Test scheitert
  (fehlendes `schema`-Modul, MySQL-Integration) — nicht durch diese Arbeit.

## Constraints (User-Memory, gelten durchgehend)

- **Niemals selbstständig committen** — keine Commit-Vorschläge, keine Nachfrage.
- **Keine `as`-Aliase in `use`** — direkte Modul-Imports.
- **Exhaustive `match` statt `if let`** bei evolving Enums.
