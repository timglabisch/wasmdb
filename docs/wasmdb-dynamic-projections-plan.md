# Dynamische Projektions-Instanzen (activate/disable) — Umsetzungsplan (Handoff)

**Status (2026-07-20): VOLLSTÄNDIG UMGESETZT (Schritt 0-4).** Grün:
`cargo test -p database-projection` (9 dynamic-Kernel-Tests),
`-p database-reactive` (4 Integrationstests), `-p projection-demo-domain`
(`dynamic_projection.rs`, 4 Host-Tests), `cargo build --workspace`,
wasm-pack, UI `tsc` + `vite build`. Aus Stufe 2 ist die Macro-Syntax
nachgezogen: `#[dynamic_projection]` (siehe Stufe-2-Abschnitt unten) —
`ActivityFold` ist damit im selben `apply`/`render`-Idiom wie `BalanceFold`
geschrieben; Codegen-Registrierung, Uuid-Namen und Stufe 3 bleiben offen.
Nichts committet. Abweichungen vom Plan: keine nennenswerten — zusätzlich
kam nur eine `DbError::Projection(String)`-Variante in `database` dazu (die
neuen reactive-APIs brauchten einen Fehlertyp) und der Client-Durchstich in
Schritt 3.2 entfiel (der `DynClient`-Trait exponiert `db_mut()` bereits).

**Zweck:** Selbsttragender Plan, damit die Umsetzung nach einem Context-Clear
ohne Rückfragen fortgesetzt werden kann. Alle Code-Anker sind gegen den
aktuellen Stand verifiziert (Datei:Zeile). Sprache mit dem User: **Deutsch**.
Er ist Maintainer von wasmdb. Nichts selbstständig committen.

**Leitplanke (vom User gesetzt):** Eingriffe ins Subscription-System
**minimal** halten — die Projektionen leben primär in ihrem eigenen Crate
(`database-projection`). Verifiziertes Ergebnis: **`sql-engine` braucht null
Änderungen** (alles Nötige ist bereits public), `database-reactive` nur eine
kleine additive API + einen Notify-Split.

---

## 0. Ziel in einem Satz

Projektionen behalten ihre heutige Form (Fold bekommt jede Source-Row,
schreibt in eine owned Tabelle), aber **Instanzen** davon werden zur Laufzeit
per eindeutigem Verbund-Namen aktiviert und deaktiviert —
`projectionActivate(id, ['document', 1234])` / `projectionDisable(id, ...)` —
mit Refcount, Demand-Lifecycle (materialisiert solange beobachtet, evicted bei
0) und Routing über die vorhandene Subscription-Maschinerie
(candidates→verify), ohne diese zu verändern.

## 1. Modell (Kurzfassung)

- **Instanz** = (Template-Id, Verbund-Name). Der Name ist EIN eindeutiger
  zusammengesetzter Bezeichner, `Vec<CellValue>` (z.B.
  `[Str("account"), Str("carol")]`) — kein Array mehrerer Slices.
- **Footprint** (deklarativ, v1): pro Source-Tabelle eine Liste von Bindings
  `(spalten_idx, namens_komponenten_idx)` — eine Row gehört zur Instanz, wenn
  `row[col] == name[comp]` für alle Bindings. Kompiliert zu genau der
  Condition-Struktur, die Query-Subscriptions nutzen (Composite-Key +
  Verify-Filter).
- **Identifikation** (wer ist betroffen): `on_zset(&instance_registry, zset)`
  — dieselbe freie Funktion, dieselbe zweiphasige Pipeline
  (Reverse-Index-Probe → Verify auf der Row) wie bei Query-Subscriptions.
  Die Engine hält dafür eine **eigene** `SubscriptionRegistry`-Instanz.
- **Auflösung** (was ist das neue Ergebnis): KEIN SQL-Re-Run — direkter
  Row-Gather über den Host (`rows_matching`), memoisierter Fold
  (`FoldCache`), `multiset_diff` gegen `last_render`, Delta anwenden.
  (Subscriptions lösen lazy per vollem SELECT-Re-Run auf; Projektionen
  eager im Derive-Pass per Gather+Fold+Diff — der Identifikations-Split
  ist identisch, nur die Auflösung unterscheidet sich.)
- **Lifecycle:** `activate` = Conditions registrieren + initial
  materialisieren; nochmal `activate` = Refcount++; `deactivate` bei
  Refcount 0 = deregistrieren + Output-Rows zurückziehen + Memo verwerfen.
  Source-Rows bleiben liegen (Cache-Policy-Knopf, bewusst v1: nicht räumen).
- **Statische Projektionen (data-presence) bleiben unverändert.** Dynamik
  ist ein additiver, paralleler Pfad in der Engine.
- **Kein Server-Fetch in der Engine.** Die Engine materialisiert über lokale
  Daten. Fehlende Rows vom Server holen ist Sache der sync-Schicht
  (per-Footprint-Fetch = Stufe 3; die Demo braucht es nicht, weil
  `bootstrap` alles zieht).

## 2. Ausgangsstand — verifizierte Code-Anker

| Fakt | Anker |
|---|---|
| `notify()` läuft Derive-Pass ZUERST, Subscriber sehen EIN kombiniertes Delta („same-batch atomicity") — die Zwei-Stufen-Pipeline existiert schon | `crates/core/database-reactive/src/reactive_database.rs:431-450`, Felddoku `:82-88` |
| `SubscriptionRegistry` ist pure state, consumer-agnostisch; `new`/`subscribe`/`unsubscribe` public | `crates/core/sql-engine/src/reactive/registry.rs:1-11, 69-210` |
| `on_zset(registry, zset) -> SubscriptionId → triggered-Condition-Indizes` ist eine freie pub-Funktion | `crates/core/sql-engine/src/reactive/execute.rs:211` |
| `OptimizedReactiveCondition` / `ReactiveLookupStrategy::IndexLookup` / `ReactiveLookupKey`: **alle Felder pub** → von außen konstruierbar | `crates/core/sql-engine/src/planner/reactive/mod.rs:52-91` |
| `ast::Value` hat `Uuid([u8;16])` → Namens-Komponenten direkt als Werte, keine Placeholder nötig | `crates/core/sql-parser/src/ast.rs:171-181` |
| Verify wertet das Prädikat direkt auf der Row aus (`row.get(col.col)`); `sources` nur fürs Pretty-Print → `subscribe(..., sources: &[], ...)` ist okay (Registry-Tests machen genau das) | `crates/core/sql-engine/src/reactive/execute/verify.rs:44-46`, `registry.rs:499ff` |
| Registry-Hotpath braucht keine Allokation (nested `table → cols → subs`) | `registry.rs:53-64` |
| Engine-Bookkeeping heute: per Partition `last_render` / `live_partitions` / `fold_caches`; Routing primitiv via `sources_by_table` + Ein-Spalten-Extraktion | `crates/core/database-projection/src/engine.rs:87-95, 311-331` |
| Kaskade: derived Deltas werden im Derive-Loop re-geroutet | `engine.rs:219-224` |
| `DatabaseHost::rows_for_partition` nutzt Single-Column-Index wenn vorhanden | `crates/core/database-projection/src/db_host.rs:37-46` |
| `FoldCache` ist opak, Lifecycle folgt der Partition | `crates/core/database-projection/src/spec.rs:59-75` |
| `database-projection` hat sql-engine als Dep, aber **kein** sql-parser (bewusst, `format_uuid`-Kommentar) → Dep ergänzen | `engine.rs:400`, `crates/core/database-projection/Cargo.toml` |
| wasm-Exports (non-generic) liegen in sync-client | `crates/sync/sync-client/src/wasm/api.rs` (subscribe `:25`, bootstrap `:201`, `js_to_param_value` `:323`) |
| Codegen registriert Projektionen via `register_all_projections`; Demo wired über `define_wasm_api!(projections = ...)` | `crates/tables/tables-codegen/src/emit.rs:126-166`, `examples/projection-demo/frontend/apps/wasm/src/lib.rs:19-24` |
| TS-Client-Erweiterungspunkt (Muster: optionales `bootstrap` auf `WasmSyncApi`) | `frontend-packages/client/src/index.ts` |

## 3. Gelockte Entscheidungen

1. **Engine hält eine EIGENE `SubscriptionRegistry`.** Kein Touch an der
   Registry in `ReactiveDatabase`, null Änderungen in `sql-engine`. Der
   einheitliche Debug-Graph (Instanzen + Query-Subs in einer Registry) ist
   bewusst NICHT v1 (Stufe 3, optional).
2. **Additiver Pfad:** neuer Trait `DynamicProjection` + eigene
   Engine-Strukturen. Statischer Pfad (Trait `Projection`,
   `PartitionedSource`, data-presence) bleibt byte-identisch.
3. **Name = `Vec<CellValue>`**, HashMap-Key (CellValue ist Hash+Eq).
   Anzeige-Form für Fehler/Events: Komponenten mit `/` gejoint.
4. **Footprint v1 = Gleichheits-Bindings** (kompiliert zu einem
   IndexLookup-Key-Set pro Source + Verify-AND-Kette). Reichere Prädikate
   (Ranges etc.) später — die Verify-Maschinerie kann es schon, nur die
   Spec-Form fehlt dann.
5. **Dynamische Projektionen sind DAG-Blätter (v1):** ihre Outputs dürfen
   nicht Source/Read irgendeiner Projektion sein (Registrierung lehnt ab).
   Ihre Sources DÜRFEN statische Outputs sein — sie laufen nach dem
   statischen Pass über das akkumulierte Delta (extern + derived).
6. **`reads` bei dynamischen Templates:** grob wie statisch v1 — Änderung an
   einer Read-Tabelle re-rendert alle aktiven Instanzen des Templates.
7. **Refcount pro Instanz** (Muster: `reactive_database.rs:326-330` /
   `:369-383`). Evict bei 0: Output-Rows-Retraction-Delta + Memo weg +
   unsubscribe.
8. **v1 ohne Codegen:** Templates werden hand-registriert
   (`engine.register_dynamic(...)`); die Demo wrappt das generierte
   `register_all_projections`. Die Macro-Syntax ist inzwischen da
   (`#[dynamic_projection]`, s. Stufe 2); die Codegen-Registrierung nicht.
9. **Namens-Komponenten über die JS-Grenze v1:** `number → I64`,
   `string → Str`. Uuid-Komponenten Stufe 2 (explizite Form, kein
   String-Sniffing).
10. **Aktivierung wirkt auf die reaktive (optimistische) DB** des Clients;
    `replace_data`-Rebuild muss aktive Instanzen überleben (Engine-State
    liegt in der Engine, nicht in den Tabellen — `reset_and_rederive`
    re-materialisiert sie).

## 4. Reihenfolge

0. **Design-Doc:** `docs/wasmdb-projections-design.md` — neuer §12
   „Demand-getriebene Projektions-Instanzen": Modell aus Abschnitt 1,
   Identifikation-vs-Auflösung-Tabelle (Subscription lazy SQL-Re-Run vs
   Projektion eager Gather+Fold+Diff), Verweis auf diesen Plan.
1. **Kern** (`database-projection`) — alles Weitere hängt davon ab.
2. **Wiring** (`database-reactive`) — activate/deactivate public + Notify-Split.
3. **wasm + TS-Client** (`sync-client`, `frontend-packages/client`).
4. **Demo** (`examples/projection-demo`).
- Stufe 2 (optional): Macro-Syntax ✅ / Codegen-Registrierung + Uuid-Namen offen.
- Stufe 3 (optional): per-Footprint-Server-Fetch, TS-Folds, geteilte Registry.

---

## Schritt 1 — Kern in `database-projection` (file-by-file)

| # | Datei / Ort | Änderung |
|---|---|---|
| 1 | `Cargo.toml` | Dep `sql-parser` ergänzen (transitiv via sql-engine eh vorhanden; gebraucht für `ast::Value` beim Condition-Bau) |
| 2 | **NEU** `src/dynamic.rs` | `pub type InstanceName = Vec<CellValue>;` — `pub struct FootprintSource { pub table: String, pub bind: Vec<(usize, usize)> }` (Spaltenindex ↔ Namens-Komponente) — `pub struct DynamicSpec { pub id: String, pub sources: Vec<FootprintSource>, pub reads: Vec<String>, pub outputs: Vec<String> }` — `pub trait DynamicProjection { fn spec(&self) -> DynamicSpec; fn project(&self, name: &[CellValue], inputs: &Inputs, ctx: &ReadCtx<'_>, cache: &mut FoldCache) -> Result<Vec<OutputRow>, String>; }` — Helfer `fn cell_to_value(&CellValue) -> ast::Value` (I64→Int, Str→Text, Uuid→Uuid, Null→Null) — `fn compile_footprint(spec, name) -> Vec<OptimizedReactiveCondition>`: pro `FootprintSource` eine Condition, `source_idx` = Footprint-Index, `IndexLookup` mit EINEM Key-Set aus allen Bindings, `verify_filter` = AND-Kette aus `Equals { col: ColumnRef { source: fp_idx, col }, value }` (Verify liest nur `col.col`, s. Anker) — Anzeige-Helfer `display_name(&[CellValue]) -> String` (Komponenten `/`-gejoint, Uuid via vorhandenem `format_uuid`) |
| 3 | `src/spec.rs` — `trait RowReader` | **Additiv, non-breaking:** `fn rows_matching(&self, table: &str, keys: &[(usize, CellValue)]) -> Vec<Vec<CellValue>> { … }` mit Default-Impl über `all_rows` + Filter (Kernel-Test-Hosts kompilieren unverändert weiter) |
| 4 | `src/db_host.rs` | `rows_matching` überschreiben: Single-Column-Index auf dem ersten Key (`index_for_column`/`lookup_eq`, Muster `db_host.rs:37-46`), Rest-Keys als Filter; ohne Index Scan |
| 5 | `src/engine.rs` — neue Felder | `dyn_nodes: Vec<DynNode>` (`{ spec: DynamicSpec, imp: Box<dyn DynamicProjection> }`) — `dyn_owner_by_table: HashMap<String, usize>` — `instance_registry: SubscriptionRegistry` — `sub_to_instance: HashMap<SubscriptionId, (usize, InstanceName)>` — `instances: Vec<HashMap<InstanceName, InstanceState>>` mit `InstanceState { refcount: u32, sub_id: SubscriptionId, cache: FoldCache, last_render: Vec<OutputRow> }` |
| 6 | `src/engine.rs` — `register_dynamic(imp) -> Result<(), RegisterError>` | Validierung VOR Mutation (Muster `register`, `engine.rs:118-169`): Id unique über statisch+dynamisch; Outputs weder in `owner_by_table` noch `dyn_owner_by_table`; Output ≠ eigene Source/Read; **Blatt-Regel v1**: Outputs dürfen in keiner (statischen wie dynamischen) `sources`/`reads` vorkommen und umgekehrt darf keine dynamische Source ein dynamischer Output sein → neue `RegisterError`-Variante `DynamicOutputConsumed` |
| 7 | `src/engine.rs` — `activate(id, name, host) -> Result<DeriveOutcome, String>` | existiert die Instanz → `refcount += 1`, leeres Outcome. Sonst: `compile_footprint` → `instance_registry.subscribe(&conds, &[], &Params::new())` → `InstanceState` einfügen → `recompute_instance` (Gather via `rows_matching` pro FootprintSource → `total==0` ⇒ leerer Render (Instanz bleibt AKTIV — demand, nicht data-presence!) → sonst `project(name, inputs, ctx, cache)` → Output-Tabellen-Check → Diff gegen `last_render` → `host.apply_delta`) → Outcome mit Delta + `succeeded`/`failures` (partition = `display_name`) |
| 8 | `src/engine.rs` — `deactivate(id, name, host) -> Result<DeriveOutcome, String>` | `refcount -= 1`; bei 0: `instance_registry.unsubscribe(sub_id)`, `sub_to_instance`-Eintrag weg, Retraction-Delta = `multiset_diff(&[], &last_render)` via `host.apply_delta`, State droppen. Unbekannte Instanz = Fehler (Programmierfehler des Embedders) |
| 9 | `src/engine.rs` — `derive()` erweitern | Nach dem statischen Topo-Loop (`engine.rs:203-233`): akkumuliertes ZSet (externer Batch + `outcome.delta`) durch `on_zset(&self.instance_registry, …)` schicken → betroffene Instanzen sammeln. **Guard:** `on_zset` nur rufen, wenn `sub_to_instance` nicht leer ist — `on_zset` baut sonst pro Mutation Tracing-Spans inkl. Row-Clone (`execute.rs:245`) für nichts; mit Guard kostet der dynamische Pfad ohne aktive Instanzen einen Branch; zusätzlich `read_dirty`-Analog für dynamische `reads` (Tabellen-Treffer ⇒ alle Instanzen des Templates); deterministisch sortiert (Template-Idx, dann Name) recomputen; Deltas an `outcome.delta` anhängen. KEINE Rück-Kaskade nötig (Blatt-Regel) |
| 10 | `src/engine.rs` — `guard_external` + `reset_and_rederive` | `guard_external`: auch `dyn_owner_by_table` prüfen (`engine.rs:174-184`). `reset_and_rederive`: Teardown räumt zusätzlich dynamische Outputs; danach alle AKTIVEN Instanzen re-materialisieren (Caches + `last_render` vorher leeren — Registrierungen und Refcounts überleben; das ist der `replace_data`-Pfad, `reactive_database.rs:526-537`) |
| 11 | `src/lib.rs` | `pub mod dynamic;` + Re-Exports (`DynamicProjection`, `DynamicSpec`, `FootprintSource`, `InstanceName`); Layering-Doku ergänzen (Engine nutzt jetzt `sql_engine::reactive` als Routing-Bibliothek — Identifikation geteilt, Auflösung eigen) |
| 12 | **NEU** `tests/dynamic.rs` (Kernel-Tests, In-Memory-Host wie `tests/kernel.rs`) | (a) activate materialisiert genau die benannte Instanz, andere Keys unberührt; (b) Verbund-Name mit 2 Komponenten + 2 Bindings: Row muss BEIDE matchen (Composite-Key + Verify); (c) Routing: Insert einer matchenden Row → Instanz updated, nicht-matchende → kein Recompute (Registry-Miss); (d) Refcount: 2× activate, 1× deactivate ⇒ lebt, 2× ⇒ Output-Rows zurückgezogen, Registry leer; (e) leerer Footprint bei activate ⇒ Instanz aktiv mit leerem Render, späterer Insert materialisiert; (f) statischer Output als dynamische Source (Kaskade in einem Pass, EIN konsistentes Delta); (g) `reset_and_rederive` erhält Instanzen; (h) Fehler im Fold ⇒ `DeriveFailure` mit `display_name`-Partition, alter Output bleibt |

**Verifikation Schritt 1:**
```
cargo test -p database-projection
cargo build -p database-projection
```

## Schritt 2 — Wiring in `database-reactive`

| # | Ort | Änderung |
|---|---|---|
| 1 | `reactive_database.rs` — **Notify-Split** | Aus `notify()` (`:431-450`) den Dispatch-Teil als private `fn dispatch_to_subscribers(&mut self, zset: &ZSet)` extrahieren (Registry-Routing + DirtySet + Wake). `notify()` = `derive_pass` + Dispatch, unverändert im Verhalten. **Wichtig:** activate/deactivate dürfen NICHT `notify()` auf ihr eigenes Delta rufen — das würde `derive_pass` auf einem Delta laufen lassen, das owned Tables berührt (`debug_assert` in `derive_pass`, `:202-206`, würde feuern). Sie rufen nur `dispatch_to_subscribers` |
| 2 | `reactive_database.rs` — neue API | `pub fn activate_projection(&mut self, id: &str, name: Vec<CellValue>) -> Result<(), DbError>` und `pub fn deactivate_projection(&mut self, id: &str, name: &[CellValue]) -> Result<(), DbError>`: Engine-Op mit `DatabaseHost` (Muster `install_projections`, `:142-151`), Outcome durch `absorb_outcome_bookkeeping`, Delta an `dispatch_to_subscribers`. Ohne installierte Engine → Fehler |
| 3 | `tests/projections.rs` | Integration: (a) REACTIVE-Subscription auf der dynamischen Output-Tabelle, dann activate ⇒ Sub wird dirty, Query liefert die Instanz-Row; (b) Source-Mutation via `execute_mut` ⇒ EINE Notification, Source + Output konsistent; (c) deactivate ⇒ Sub dirty, Row weg; (d) `replace_data` + `notify_all` ⇒ Instanz-Row aus neuen Source-Daten wieder da |

**Verifikation Schritt 2:**
```
cargo test -p database-reactive
```

## Schritt 3 — wasm-Exports + TS-Client

| # | Ort | Änderung |
|---|---|---|
| 1 | `crates/sync/sync-client/src/wasm/api.rs` | Non-generic Exports (Muster `bootstrap`, `:201`): `#[wasm_bindgen] pub fn projection_activate(id: String, name: JsValue) -> Result<(), JsError>` + `projection_deactivate(...)`. Name-Konvertierung: JS-Array, `number → CellValue::I64` (Ganzzahl-Check), `string → CellValue::Str` (eigener kleiner Helfer neben `js_to_param_value`, `:323`). Nach der Engine-Op `drain_projection_events()` (Fold-Fehler sollen sichtbar werden, Muster `repair_chain`/`bootstrap`) |
| 2 | `crates/sync/sync-client` — Client-Durchstich | `with_client_dyn`-Pfad: Methode auf dem Client, die an `ReactiveDatabase::activate_projection`/`deactivate_projection` der reaktiven DB delegiert (Entscheidung 10) |
| 3 | `frontend-packages/client/src/index.ts` | `WasmSyncApi` um optionale `projection_activate`/`projection_deactivate` erweitern (Muster `bootstrap?`); exportierte Wrapper `activateProjection(id: string, name: (string \| number)[])` / `deactivateProjection(...)` — fehlender Export ⇒ no-op wie beim `bootstrap`-Fallback |

**Verifikation Schritt 3:**
```
cargo build -p sync-client --target wasm32-unknown-unknown
wasm-pack build …   (wie im Repo üblich)
cd frontend-packages/client && tsc --noEmit
```

## Schritt 4 — Demo (`examples/projection-demo`)

Ziel im UI: „Account-Detail (on demand)" — man aktiviert eine Detail-Projektion
für genau einen Account (10k-Szenario im Kleinen: nicht alles materialisieren,
sondern das Angeklickte).

| # | Ort | Änderung |
|---|---|---|
| 1 | `shared/domain/src/ledger/account_activity.rs` (NEU) | `#[row] AccountActivity { account: String (pk), deposits: i64, withdrawals: i64, largest_cents: i64 }` — die owned Output-Tabelle des Templates |
| 2 | `shared/domain/src/ledger/activity_fold.rs` (NEU) | `ActivityFold` als **hand-geschriebener** `DynamicProjection`-Impl (v1 ohne Macro): `spec()` = id `"activity"`, Source `ledger_log` mit `bind: [(account_col, 1)]` (Name = `[Str("account"), Str(<account>)]` — Komponente 0 ist Diskriminator/Namespace, Komponente 1 bindet), Output `account_activity`; `project()` dekodiert Rows via `decode_rows::<LedgerLog>` (`typed.rs:16`), zählt Deposits/Withdrawals, trackt `largest_cents`, emittiert via `Out` |
| 3 | `frontend/apps/wasm/src/lib.rs` | Wrapper statt Direkt-Durchreichung: `fn projections() -> ProjectionEngine { let mut e = generated::register_all_projections(); e.register_dynamic(Box::new(ActivityFold::default())).expect(...); e }` — `define_wasm_api!(projections = projections, ...)` (`lib.rs:19-24`) |
| 4 | `frontend/apps/ui/src/App.tsx` + `index.css` | Neues Panel „Account detail — demand projection": pro Account ein Activate/Disable-Toggle; bei aktiv `useQuery` auf `account_activity WHERE account = :a`; Hinweistext, dass nur aktivierte Accounts materialisiert sind. Nach F5 sind Instanzen weg (Client-Speicher) — UI re-aktiviert die getoggelten beim Mount nach `bootstrap` |
| 5 | `frontend/apps/ui/src/commands.ts` o.ä. | dünne Wrapper um `activateProjection('activity', ['account', name])` / `deactivateProjection(...)` |
| 6 | `shared/domain/tests/dynamic_projection.rs` (NEU, Host-Test) | Gegen `ReactiveDatabase` + Engine wie `gap_repair.rs`: (a) Seed alice/bob/carol, activate carol ⇒ genau eine `account_activity`-Row (carol), alice/bob NICHT materialisiert; (b) `foreign_write`-Rows anwenden ⇒ Instanz zählt hoch; (c) deactivate ⇒ Tabelle leer; (d) activate → `replace_data`-Rebuild ⇒ Row wieder da |
| 7 | `examples/projection-demo/README.md` | Abschnitt „Demand-Projektion" (was der Toggle demonstriert, Bezug §12) |

**Verifikation Schritt 4:**
```
cargo test -p projection-demo-domain
cargo build --workspace
wasm-pack build …
cd examples/projection-demo/frontend/apps/ui && tsc --noEmit && vite build
make projection-demo   (Klick-Test: activate carol → Panel füllt sich;
                        Post auf carol → Detail updated live; disable → leer;
                        alice bleibt unmaterialisiert)
```

---

## Stufe 2 (optional, NACH v1)

- **Macro-Syntax:** ✅ UMGESETZT als eigenes Attribut
  `#[dynamic_projection(id = "...", outputs(...), bind(spalte = komponente),
  reads(...))]` (`tables-macros`): derselbe `apply`/`render`-Vertrag wie
  `#[projection]`, geteilte Validierung (`fold_impl_parts`) und geteilter
  Fold-Shim (`fold_shim_body`, inkl. Committed-Prefix-Memo — dynamische
  Instanzen memoisieren damit wie statische). Codegen scannt exakt auf
  `projection` (`is_ident`), ignoriert das neue Attribut also —
  Registrierung bleibt Handarbeit (`register_dynamic` im wasm-Wrapper).
  OFFEN: Codegen-Registrierung via `register_all_projections`
  (`tables-codegen/src/emit.rs` `emit_register_all_projections`).
  ⚠ Falls dabei Row-Spalten entstehen/ändern: `#[projection_row]` wird
  ZWEIMAL expandiert (proc-macro `tables-macros` + codegen
  `tables-codegen`) — Änderungen in BEIDEN spiegeln, sonst
  Client-Schema-Drift erst im Browser.
- **Uuid-Namens-Komponenten** über die JS-Grenze: explizite Form
  (z.B. `{ uuid: '…' }`) im Konverter, kein String-Sniffing.
- **Typed-TS-Wrapper** pro Template aus dem Codegen
  (`projectionActivateActivity(account: string)`).

## Stufe 3 (optional, Skizzen)

- **Per-Footprint-Server-Fetch:** `sync::protocol::FetchMatchingRequest
  { table, keys: Vec<(col_name, value)> }` (Muster `HeadsRequest`/
  `FetchRowsRequest`); wasm `projection_activate_fetching(id, name,
  fetch_path)` = fetch → `apply_zset` → activate. Erst nötig, wenn der
  Client NICHT mehr alles bootstrapped (das echte 10k-Szenario).
- **TS-Folds** (`projectionDefine` in JS): Footprint bleibt deklarativ
  (Routing schnell), Fold als JS-Callback über die wasm-Grenze — pro
  Änderung volle Instanz-Partition neu rechnen (kein Memo, opak/nicht
  garantiert pur). Escape-Hatch für Laufzeit-definierte Projektionen.
- **Geteilte Registry / Debug-Graph:** Instanz-Footprints und
  Query-Subscriptions in der Debug-Toolbar als ein Abhängigkeitsgraph
  (Footprint → Instanz → owned Table → Subscriber). Erst dann lohnt sich
  die Diskussion shared-vs-own Registry erneut.

## Risiken / offene Punkte

- **`rows_matching`-Performance ohne Index:** Default = Scan. Für die Demo
  egal; fürs 10k-Szenario braucht die Source-Tabelle einen Index auf der
  ersten Bind-Spalte (`index_for_column` existiert — ggf. Index-Anlage
  prüfen, sonst ist der Gather O(n) pro Recompute).
- **Doppel-Materialisierung:** Ist ein Account gleichzeitig von der
  STATISCHEN BalanceFold (data-presence) und einer dynamischen Instanz
  erfasst, ist das korrekt (verschiedene Output-Tabellen), aber doppelte
  Arbeit — bewusst, die Demo zeigt beide Modelle nebeneinander.
- **`notify_all` nach activate?** Nicht nötig: activate dispatcht sein
  Delta präzise. Aber Subscriptions, die VOR der Tabellen-Existenz…
  entfällt — Tabellen existieren ab Schema-Registrierung, nur leer.
- **Fehler-Surface:** Fold-Fehler einer Instanz landen als
  `ProjectionEvent::Failed` mit `display_name`-Partition im bestehenden
  Event-Strom (`reactive_database.rs:98-110`) — Requirements/Toolbar
  zeigen sie ohne Zusatzarbeit.
