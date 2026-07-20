# Projections — Design & Umsetzungsplan

Status: M1, M1b, wasm-Registrierung, M2 (Requirements-Slot) und M3
(`#[projection]`, `append_to`, `register_all_projections`) sind
UMGESETZT (siehe Status-Abschnitt am Ende); M4 (Demo-Vertical) und M5
(README) offen; M6a (generiertes Event-Schema), die
Partition-Konsolidierung (§9.6) und M6b KOMPLETT UMGESETZT — der
Produkt-Contract (der Fold `apply`/`render` auf dem State-Typ, §9.4)
UND die inkrementelle Ausführung (EIN Committed-Frontier-Snapshot pro
live Partition statt des §9.3-Rings — bewusst einfachste Form, siehe §8).
Anlass: `heyrechnung/docs/wasmdb-derived-streams.md` (Handoff-Doc des Produkt-Repos).
Dieses Dokument ist selbsttragend — es enthält Problem, Entscheidung, Contracts
und die konkreten Arbeitspakete. Das Handoff-Doc ist nur noch Hintergrund.

## 0. Auftrag in einem Satz

wasmdb bekommt **materialisierte Sichten als Rust-Funktion**: eine Projektion
ist eine pure Funktion von Quell-Tabellen-Rows auf abgeleitete Tabellen-Rows,
die die Engine am `apply`-Chokepoint automatisch, atomar (same-batch) und
lebenszyklus-verwaltet (Requirements-DAG) nachführt.

## 1. Ausgangslage & Root Cause

Das Produkt (Rechnungs-App) hat ein Dokument-Vertical, dessen Wahrheit ein
append-only Command-Log pro Dokument ist (Event-Sourcing-artig, serverseitig
in FoundationDB). Die Browser-Tabellen sind IMMER abgeleitet:
`rows = render(fold(events))`. Mit dem heutigen wasmdb-Contract
(`Command::execute_optimistic` schreibt Hand-Projektionen, Rollback =
ZSet-Invert) entstehen daraus zwei strukturelle Lücken:

- **Ableitungs-Lücke:** wasmdb kann keine abgeleiteten Tabellen pflegen.
  Folge im Produkt: 37 handgeschriebene `execute_optimistic` (jede die
  partielle Ableitung „∂render/∂command"), Rücklesen aus der DB mit
  `unwrap_or(ZERO)`-Fehlerklasse, Paritätstest als Geständnis zweier
  Rechenwege.
- **Reconciliation-Lücke:** ZSet-Invert kann als Rollback nur den exakt
  gegangenen Pfad rückwärts. Der nötige Zielzustand nach einem Reject bei
  gleichzeitigen Fremd-Events (zweiter Tab, MCP-Agent) ist aber „Basis
  inkl. fremder Events, ohne meine" — ein Zustand, der nie existiert hat.

Kernbeobachtung der Lösung: werden die **Events selbst als Rows** einer
normalen Tabelle geführt, ist die Lokalität wiederhergestellt — jedes Command
ist seine eigene, disjunkte Row. Damit ist ZSet-Invert auf der Log-Tabelle
wieder korrekt (Reconciliation-Lücke verschwindet ohne Sync-Änderung), und
die Ableitungs-Lücke wird als echtes DB-Feature geschlossen (Projektion).

## 2. Zielbild

```
UI ──execute(cmd)──▶ Command: execute_optimistic = Append einer Log-Row
                                    │  (+1 Row, provisorische seq, committed=false)
                                    ▼
                    ┌──────── apply/notify-Chokepoint ────────┐
                    │  Derivations-Hook (same-batch, vor      │
                    │  Notify): betroffene Projektions-Keys   │
                    │  → project(rows) → diff(last_render)    │
                    │  → Output-Deltas in denselben Batch     │
                    └──────────────────────────────────────────┘
                                    │  ein Notify, ein konsistenter Stand
                                    ▼
UI ◀──subscribe/query── Log-Tabelle + abgeleitete Tabellen

Transport (unverändert): CommandRequest → Server → execute_server persistiert
im Backend-Log, Verdict-Echo finalisiert die Row (echte seq, committed=true).
Fremde Events kommen als Log-Rows über den bestehenden table-fetch-Tail
(`#[query]` mit from_seq) — und triggern denselben Hook.
```

Es gibt **kein neues JS-API**. Kein `create_derived_stream`, kein Seed, kein
close, kein advance. Öffnen eines Dokuments = Log-Requirement subscriben;
Lifecycle = Datenpräsenz + Refcount.

### Bewusst verworfene Alternativen (nicht re-litigieren)

| Alternative | Warum verworfen |
|---|---|
| `DerivedStream<P>`-Primitive in der Sync-Schicht (Vorschlag des Handoff-Docs) | Verschmilzt beide Lücken zu einem Spezial-Primitive; braucht neues JS-API, Seed-DTO, Stream-Enum, eigenen Verdict-Pfad. Der Rows-Schnitt löst beides ohne Sync-Änderung. |
| Command schreibt Ableitung selbst (Status quo) | Command ist nur einer von mehreren Schreibern; Fetch-Upserts und Reconcile-Inverts laufen an ihm vorbei → stale. Rollback invertiert dann abgeleitete Rows → Rebase-Problem. |
| Projektion als Produkt-Schicht „davor" | Wurde im Produkt als Gerüst (observer.rs + Treiber) gebaut und ist das, was wegsoll: Trigger-Lücken, keine Same-Batch-Atomarität, Lifecycle von Hand. Chokepoint, Atomarität und Lifecycle sind nur in der Engine implementierbar. |
| Trigger-API (Row-Change-Events mit insert/update/delete an Userland) | Deltas nützen einem Fold nichts (nicht invertierbar); imperative Effekte zerstören Re-Ableitbarkeit und Ownership; Trigger-Hölle (Ordnung, Rekursion). Change-Art bleibt engine-internes Scheduling-Signal. |
| Fold-State/Wasserlinie im Framework-Contract | Pure Funktion über Rows ist strikt einfacher: Snapshot-Handling, Lücken-Logik, Pending-Ordnung, Payload-Decode werden testbarer Produkt-Code. Framework behält nur, was nur die Engine kann. — Gilt weiter für den DEFAULT-Contract; als opt-in Fold-KNOTENSORTE mit Reducer-Contract und Daten-Guards inzwischen als korrekt machbar konkretisiert, siehe §9. |
| SQL-IVM (inkrementelle Views) jetzt | Viel größerer Scope. Der Rows-Contract definiert aber den Slot dafür: eine SQL-View ist dieselbe Knotensorte mit anderer Ausführungsstrategie (Delta-Propagation statt recompute+diff). Später. |

## 3. Contracts & Garantien

1. **Projektion = pure Funktion.** Engine-seitig: `(source_rows, ctx) ->
   Vec<(TableName, Vec<CellValue>)>`. Kein IO, keine Uhr, kein RNG, kein
   globaler Zustand. Die Rückgabe-Rows sind der einzige Effekt-Kanal.
   Determinismus: gleicher Input ⇒ gleiche Rows. (Rollback-Vollständigkeit
   ist äquivalent zu dieser Purity.) Produkt-seitig wird diese Funktion
   als Fold GESCHRIEBEN (`apply`/`render`, §4.3/§9.4) — der generierte
   Shim `render ∘ fold` IST die pure rows→rows-Funktion.
2. **Zustandsbasierte Invariante.** Abgeleitete Tabellen ≡
   `project(aktueller Quell-Inhalt)` — egal, wie die Quelle sich geändert hat
   (Append, UPDATE, DELETE, Compaction). Append-only auf der Log-Tabelle ist
   Produkt-Konvention, die Performance kauft, nicht Korrektheit.
   (Eine Ausnahme seit der M6b-Snapshot-Ausführung: der Fold-Shim
   memoisiert per committed-seq-Präfix — eine committete Log-Row, die
   bei UNVERÄNDERTER seq inhaltlich mutiert, wird nicht erkannt und
   servierte einen veralteten State. Inserts, Deletes und der
   Pending→Committed-Übergang ändern die seq-Liste und invalidieren
   korrekt; `replace_data` leert alle Memos. Für den Fold-Knoten ist
   die Unveränderlichkeit committeter Rows pro (Partition, seq) damit
   tragend, nicht mehr nur Konvention.)
3. **Same-Batch-Atomarität.** Ableitung läuft im selben apply-Batch wie das
   auslösende Delta, VOR dem Notify. Subscriber sehen nie Quelle-neu /
   Ableitung-alt (kein Mischbild; Konsistenz-Einheit = ein Zustand).
4. **Exklusives Ownership der Output-Tabellen.** Nur die Engine (im Namen
   genau einer Projektion) schreibt eine Output-Tabelle. Registrierung
   markiert sie als framework-owned; fremde Writes ⇒ Fehler. (Nötig für
   `diff(neu, last_render)`-Korrektheit und DAG-Ordnung.)
5. **Buchhaltung = `last_render` pro Key, kein Delta-Journal.** Teleskop:
   die Summe aller je geschriebenen Deltas ist `last_render`. Totales
   Zurückrollen = `−last_render`; jeder Übergang = `diff(neu, last_render)`.
   Speicher O(Output-Größe), erreichbar ist jeder Zielzustand.
6. **Multiset-Diff, deterministisch sortiert.** Duplikate zählen. Anwendung
   über bestehendes `apply_zset` (PK-Upsert für +, Full-Row-Delete für −).
7. **DAG.** Projektionen dürfen Outputs anderer Projektionen als Quelle
   deklarieren; Ausführung topologisch im selben Batch. Zyklen ⇒ Fehler bei
   Registrierung.
8. **Key-Scope-Kriterium (Doku-Pflicht):** Kosten pro Änderung =
   O(Rows des Keys), nicht O(Tabelle). Geeignet für dokumentgroße Keys;
   für große Key-lose Aggregationen ist später die SQL-IVM-Knotensorte da.
   (Terminologie: der „Key" ist die PARTITION des Logs — eine Tabelle
   enthält viele unabhängige Logs, einen pro Dokument; siehe §9.6.)
9. **Wire-Grenze (Produkt-Regel, hier nur festgehalten):** Log-payload ist
   die RPC-Form. Mapping RPC→Domain passiert IN der Projektions-Funktion.
   Server persistiert zusätzlich seine Domain-Form; die RPC-Form muss für
   den Tail wiedergewinnbar sein (mitpersistieren, kein Rück-Mapping).

## 4. Die Bausteine

### 4.1 Engine: Derivations-Hook (das Herzstück)

Ort: der Trichter, durch den alle wirksamen Änderungen laufen, bevor
Subscriber benachrichtigt werden — `ReactiveDatabase`
(`crates/core/database-reactive/src/reactive_database.rs`). Beide
Schreibpfade münden dort:

- `apply_zset(&zset)` — Requirements-Fetch-Upserts (siehe apply-Closure in
  `sync-client/src/wasm/mod.rs:73`), Reconcile-Invert/Apply
  (`sync-client/src/client.rs:110-130`).
- `notify(&zset)` nach `execute_optimistic` (`sync-client/src/client.rs:85`)
  — Commands schreiben via SQL auf die Raw-DB, das beschreibende ZSet läuft
  danach durch `notify`.

Hook-Ablauf (eine Funktion, vier Auslöser sind ihr egal):

```
fn derive(batch: ZSet) -> ZSet {
    dirty_keys = extrahiere (projektion, key) aus batch          // via Quell-Tabellen + Key-Spalte
    for (p, key) in topologisch_sortiert(dirty_keys):
        rows      = lies Quell-Rows des Keys aus der DB          // Zustand, nicht Delta
        neu       = p.project(rows, ctx)                          // pure Produkt-Funktion
        delta     = multiset_diff(neu, last_render[p, key])
        wende delta an; last_render[p, key] = neu
        batch.extend(delta)                                       // kaskadiert ggf. weiter im DAG
    return batch                                                  // EIN Notify für alles
}
```

Details:
- **Key-Extraktion:** aus den ZSet-Entries der Quell-Tabelle (volle Rows)
  wird die registrierte Key-Spalte gelesen. Änderungen an `#[reads]`-Tabellen
  triggern in v1 grob alle lebenden Keys der Projektion (feingranulares
  Dependency-Tracking ist explizit vertagt).
- **Re-Entranz/Zyklen:** Output-Deltas laufen erneut durch die Key-Extraktion
  (DAG-Kaskade); da Output-Tabellen exakt einer Projektion gehören und der
  Graph azyklisch registriert wurde, terminiert das.
- **Key stirbt** (keine Quell-Rows mehr): `project` über leere Rows ⇒ Diff
  räumt `−last_render` komplett; Cache-Eintrag wird entfernt.
- **Fehler in `project`** (Panic/Decode): Slot auf Error (siehe 4.2),
  Output des Keys unangetastet lassen, loggen. Kein Teil-Rendering.

Dies ist bewusst KEIN inkrementeller Fold: kein State-Cache, keine
Wasserlinie im Framework. Fastpath-Optimierungen (Memoisierung) sind
Produkt-Sache innerhalb der puren Funktion (Chain-Hash als Memo-Schlüssel).
(Der Produkt-Contract ist trotzdem ALS Fold formuliert — `apply`/`render`,
§4.3. Seine Ausführung war zunächst exakt dieser Recompute — Fold ab
`Default::default()` pro Derive; inzwischen memoisiert der Fold-Shim den
Committed-Präfix-State im engine-eigenen `FoldCache` und foldet nur noch
Neues, §9.3/§8 — dank Determinismus ergebnisgleich, §9.1.)

### 4.2 Requirements-Integration: `SlotKind::Projected`

`crates/requirements/src/store/slot.rs` kennt `Fetched` (Server-`#[query]`)
und `Derived` (lokales SQL). Neu: `Projected` — Identity
`projected:<id>:<key>`, Upstream = Log-Requirement des Keys (+ Read-Tabellen).
Wiederverwendet ohne Änderung: Refcount-Lifecycle (GC des per-Key-Caches bei
refcount 0), Status-Aggregation (`recompute_status_from_upstream` — Draft ist
`Loading`, bis der Log `Ready` ist; löst das 13-Fetch-Konsistenzloch UND den
Ladezustand), Generation/Invalidation (Chain-Hash-Bruch ⇒ `invalidate()`).

### 4.3 Macro `#[projection]` (Client-Gegenstück zu `#[query]`)

Umgesetzt in tables-macros (Re-Export via tables-storage). Ein Attribut
auf einem inherenten Impl-Block, EIN Produkt-Contract — der Fold (§9.4).
Das Impl-Target IST der Fold-State (Idiom der Rust-ES-Welt, vgl.
cqrs-es `Aggregate`): eine vom Produkt deklarierte Struct mit
`derive(Default, Clone)`, ihre Felder sind der Akkumulator, ein Wert pro
Partition. `apply` bekommt immer genau EINE typisierte Log-Row, in
Fold-Ordnung (bestätigte nach seq, dann eigene Pendings nach
provisorischer seq); `render` macht aus dem gefoldeten `self` die
Output-Rows. Reads NUR in `render`:

```rust
#[derive(Default, Clone)]
pub struct InvoiceDraft { total: i64, ... }

#[projection(outputs(DocumentRow, LineRow), reads(Customer))]
impl InvoiceDraft {
    fn apply(&mut self, row: &InvoiceDraftEvent) -> Result<(), String> {
        let cmd: DraftCommand = row.decode()?;   // Decode-Politik ist Produkt-Code
        self.total += ...;
        ...
    }
    fn render(&self, ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String>;
}
```

Der State muss `Default + Clone` sein (im Shim erzwungen); genau eine
Quelle — der Row-Typ aus `apply` —, ihre Partition kommt aus
`tables::ProjectionLog` (kein `key = ...`, kein Attribut-Rauschen).
Ausgeführt wird inkrementell (§9.3/§8): der Shim memoisiert den State
des committed Präfixes pro Partition (engine-eigener `FoldCache`) und
foldet pro Derive nur neue committete Rows plus die Pendings;
Read-Dirty re-rendert ohne jeden Fold. Ungültiger Präfix (Backfill,
Delete, `replace_data`) ⇒ Fold ab null — derselbe Fold, verschieden
weit ausgeführt, per Determinismus ergebnisgleich (§9.1).

Der Macro emittiert die dyn-Registrierung (Quelle, Partitionsspalte,
Outputs, Aufruf-Shim: Rows decodieren → `in_fold_order` → Fold →
`render`). In `apply` macht das Produkt, was es will: Domain-Mapping,
unfoldbare Events überspringen (server-äquivalentes No-op) oder als
Fehler melden, am Loch stoppen. Alles in normalen Rust-Tests prüfbar —
ohne wasm, DB, Server. (Exotische Nicht-Log-Sichten bleiben über eine
Hand-Implementierung des row-level `Projection`-Traits möglich — das
ist Engine-Schnittstelle, kein zweiter Produkt-Contract.)

Output-Tabellen sind aus dem Aufruf nicht ableitbar: sie werden im
Attribut deklariert (`outputs(...)`), damit die Registrierung
Ownership + DAG prüfen kann, bevor je gerendert wird.

### 4.4 Command-Seite: Append im hand-geschriebenen `execute_optimistic`

```rust
#[rpc_command]
pub struct SetLinePrice {
    pub id: Uuid,
    pub doc_id: Uuid,
    pub line_id: Uuid,
    pub price_cents: i64,
}

impl Command for SetLinePrice {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        // Das Event, das im Log landet — ein eigener, serialisierbarer Typ,
        // NICHT das Command selbst.
        let event = LinePriceSet { line_id: self.line_id, price_cents: self.price_cents };
        let partition = CellValue::from(self.doc_id);
        let seq = sync::append::next_seq::<InvoiceDraftEvent>(
            db, InvoiceDraftEvent::PARTITION_COLUMN, &partition)?;
        sync::append::append_row(db, InvoiceDraftEvent {
            command_id: self.id,
            doc_id: self.doc_id,
            seq,
            committed: 0,
            payload: rpc_command::payload_json(&event)?,
        })
    }
}
```

Ein Command ist eine *Anfrage*, keine Log-Row. Das Anhängen eines Events an
den Log ist eine Wirkung, die das Command in seinem `execute_optimistic`
ausführt — über `sync::append::{next_seq, append_row}` +
`rpc_command::payload_json`: Row bauen (`command_id = id`, Partitionswert,
provisorische seq = max(seq der Partition)+1, `committed = false`, payload =
serialisierte Event-Form), Insert, +1-Row-ZSet zurück.

Das frühere `#[rpc_command(append_to = LogRow)]` + `#[partition]`-Marker
verschmolz Command und Log-Row zu einem und wurde entfernt — der Weg ist nie
gut: derselbe Append kann aus einem HTTP-API oder MCP-Tool kommen, nicht nur
aus einem RPC-Command, und das geloggte *Event* ist nicht dasselbe wie das
*Command*. Kriterium unverändert: *Tabellen mit genau einem Schreiber darf
das Command direkt schreiben; abgeleitete Tabellen nie.*

Log-Row-Schema (Konvention, als `#[row]` im Produkt):
`(command_id PK, <partition>, seq, committed, payload[, parent, chain_hash])`.
Seit M6a wird diese Row aus der `#[projection_row]`-Deklaration GENERIERT
(§9.4; die Partition wird aus der strikten Form inferiert, §9.6) — die
Formvorschrift ist Code, keine Konvention mehr.
Die Chain-Spalten kommen vom Server (seq-Vergabe, parent-Kette, Hash über
kanonische RPC-Bytes) — siehe 4.6.

Optionaler Fastpath (nicht v1): Vorab-Check gegen gefoldeten Zustand für
sofortiges lokales Reject-Promise. Nicht korrektheitsrelevant — der Fold
überspringt unfoldbare Rows, das Server-Verdict ist die Autorität.

### 4.5 Composition Root

```rust
sync_client::define_wasm_api!(
    command = RpcCommand,
    setup_db = generated::register_all_tables,
    register_requirements = generated::register_all_requirements,
    register_projections = generated::register_all_projections,   // NEU
);
```

`tables-codegen` sammelt `#[projection]`-Impls ein und emittiert
`register_all_projections` analog zu `register_all_tables`
(`crates/tables/tables-codegen/src/emit.rs:114`).

### 4.6 Hydrierung, Listen & die Chain als Wahrheitsanker

Hintergrund: das Handoff-Doc ergänzte (§9 Punkt 9) einen realen Fehlerfall
seines DerivedStream-Schnitts — zwei Schreiber auf denselben abgeleiteten
Rows (Stream optimistisch, Listen-Refetch mit Serverstand) erzeugen
Überschreiber und Zombie-Rows (pending gelöschte Zeile kommt per Fetch
zurück; der Diff kennt sie nicht mehr). In diesem Design ist die Fehlerklasse
strukturell ausgeschlossen, durch zwei Regeln:

1. **Fetch schreibt ausschließlich Quell-Tabellen (Logs), nie abgeleitete
   Tabellen.** Auch für Listen-Ansichten: die Requirements laden die Logs
   ALLER offenen Drafts (der Server foldet heute bei jedem table-fetch
   ohnehin alle offenen Drafts — vergleichbare Kosten); sämtliche
   Draft-Tabellen entstehen client-seitig aus Projektionen. Es gibt keinen
   zweiten Schreiber, der verregelt werden müsste; Listen zeigen pendings
   automatisch. (Fallback, falls Logs für Listen zu teuer werden: separate,
   fetch-eigene Summary-Tabelle — dann Ownership weiterhin sauber pro
   Tabelle getrennt, Liste zeigt bestätigten Stand.)
2. **Bestätigt-Sein ist eine Daten-Eigenschaft, kein Buchhaltungszustand.**
   Log-Rows tragen die Server-Chain: autoritative `seq`, `parent`-Kette,
   optional `chain_hash` über die kanonischen RPC-Bytes. Bestätigt = Row hat
   Chain-Position; pending = Off-Chain (provisorische seq, `committed=false`).
   Daraus folgt:
   - **Rollback des Unbestätigten ist eine Datenoperation:** alle
     Off-Chain-Rows eines Keys löschen ⇒ Projektion leitet auf den
     bestätigten Stand zurück. Nutzbar für Reject, Recovery, Kettenbruch
     (Off-Chain-Rows löschen, Tail refetchen, neu ableiten). Die
     PendingEntry-Buchhaltung der Sync-Schicht bleibt der Latenz-Fastpath;
     die Daten allein genügen aber jederzeit zur Rekonstruktion.
   - **Echo/Tail-Races sind harmlos:** PK = `command_id` ⇒ Verdict-Echo und
     Tail-Fetch konvergieren idempotent auf dieselbe Row; Regel: Chain
     schlägt provisorisch, unabhängig von der Ankunftsreihenfolge.

### 4.7 Sync-Schicht: unverändert

`stream.rs`, `client.rs`, Echo-Contract, Reject-Semantik — kein Touch.
Confirm-Feinheit (Produkt-Server): `execute_server` gibt als `server_zset`
dieselbe Log-Row mit autoritativer seq und `committed = true` zurück; der
bestehende Invert+Apply-Reconcile finalisiert die Row damit von selbst.
Reject: Invert entfernt die disjunkten +Rows ⇒ Hook leitet neu ab ⇒
„Basis inkl. fremder Events, ohne meine" fällt gratis heraus.
Empfehlung an Produkte: Stream pro Dokument (ein Reject verwirft den
pending-Satz seines Streams; der globale Default-Stream koppelt sonst
fremde Dokumente).

## 5. Was bewusst NICHT gebaut wird

- Kein Event-/Aggregat-/Log-Begriff in wasmdb. Keine Append-only-Erzwingung.
- Kein Seed-/Snapshot-DTO im Framework (Snapshots = Produkt-Konvention:
  Snap-Row als Quelle, Session-Memos in der puren Funktion; Gültigkeit über
  Chain-Hash/Version, ungültig ⇒ Fold ab 0 — Snapshots sind Cache, nie Wahrheit).
- Kein Trigger-API. Falls später Effekt-Hooks: separates Observer-API,
  post-notify, hart read-only.
- Kein inkrementelles IVM / SQL-Views (späterer eigener SlotKind, gleicher
  DAG). Gleiches Muster: die Fold-Knotensorte (§9) — geplant, opt-in,
  ersetzt den Recompute-Default nicht.
- Kein JS-API für Projektionen. Kein `execute_optimistic`-Signaturbruch.
- Kein feingranulares Read-Dependency-Tracking (v1: grob).
- Undo/Time-Travel: kein Framework-Feature — lesend `fold(log[0..N])` als
  Query + pure Funktion; schreibend ausschließlich kompensierende Events am
  Tail (nie Log truncaten — geteilter Log).

## 6. Umsetzungsreihenfolge

**M1 — `crates/core/database-projection` (nativ testbar, ohne wasm):**
Neues core-Crate im Muster `database`/`database-reactive`. Schichtung:
`sql-engine ← database ← database-projection ← database-reactive`.
Landeplatz für die Blaupause `crates/wasm-projection` aus dem Produkt-Repo
(Handoff §8). Zwei Modul-Ebenen:
- `kernel` (pur, database-frei, gegen Traits `SourceReader`/`DeltaSink`
  programmiert, Unit-Tests mit In-Memory-Fakes): Projektions-Registry
  (dyn: Quellen, Key-Spalte, Outputs, project-Shim), Multiset-Diff,
  `last_render`-Cache, Recompute-Planner (Batch-ZSet → betroffene Keys),
  Ownership-Durchsetzung, DAG-Topologie + Zyklen-Check.
- `db` (Adapter): `SourceReader`/`DeltaSink` gegen die echte `Database`
  (Key-Reads, `apply_zset`); Integrationstests mit echter DB, ohne
  Reactive-Schicht.
Tests: Teleskop-Property (Σ Deltas ≡ `last_render`), Key-Tod räumt Output,
Ownership-Verletzung ⇒ Fehler, DAG-Kaskade, Determinismus-Doppellauf,
Fixture-Projektion im Stil von `tables-e2e`.

**M1b — Hook in `database-reactive`:**
Minimal: apply/notify-Trichter ruft `database-projection` auf und notifyt
das erweiterte Batch-ZSet einmal. Tests: ein Notify pro Batch (Atomarität),
Reject-Fluss (Invert der Quelle ⇒ Neuableitung).
Dateien: `crates/core/database-reactive/src/reactive_database.rs`,
`crates/core/sql-engine/src/storage.rs` (ZSet-Helfer).

**M2 — Requirements-Integration:**
`SlotKind::Projected`, Key-Slots, Status-Aggregation, GC-Kopplung an den
per-Key-Cache aus M1, Invalidation-Pfad.
Dateien: `crates/requirements/src/store/slot.rs`, `store/mod.rs`,
`sync-client/src/wasm/req_bindings.rs`.

**M3 — Macros & Codegen:**
`#[projection]` (+ `RenderCtx`), `append_to`-Arm in
`rpc-command`/`rpc-command-derive`, `register_all_projections` in
`tables-codegen`, `define_wasm_api!`-Parameter.
Dateien: `crates/tables/*`, `crates/rpc-command/*`,
`crates/sync/sync-client/src/wasm/mod.rs`.

**M4 — Beweis im invoice-demo:**
Ein kleines Draft-Vertical (Log-Row, 3–4 Event-Commands, eine Projektion
mit 2–3 Output-Tabellen, `#[query]`-Tail mit `from_seq`, BlurInput-UI).
E2E: eigener Edit optimistisch, fremder Edit via Tail, Reject mit
Fremd-Event dazwischen (der Rebase-Fall — DER Akzeptanztest), Confirm-
Finalisierung, Loading-Gate.

**M5 — Doku:**
README-Abschnitt: Konzept, Purity-/Ownership-Contract, Key-Scope-Kriterium,
Ein-Schreiber-Regel, Abgrenzung zu kommendem SQL-IVM.

**M6 — Fold-Knotensorte (geplant, opt-in, Design in §9):**
- M6a: generiertes Event-Schema — Event-Typ wird per
  `#[projection_row]` deklariert statt als handgeschriebene
  `#[row]`-Log-Row; Framework generiert die Log-Row samt
  Buchhaltungsspalten. UMGESETZT (siehe §8).
- M6a-Konsolidierung (§9.6): Partition-Terminologie,
  `#[partition]`-Marker bzw. Inferenz aus der strikten Log-Form,
  generierte Fold-Helfer. UMGESETZT (siehe §8).
- M6b: Fold-Knotensorte. KOMPLETT UMGESETZT: der PRODUKT-CONTRACT
  (`apply(&mut self, row)` / `render(&self, ...)` auf dem State-Typ)
  als DER Contract der `#[projection]`-Impl-Form, UND die
  inkrementelle AUSFÜHRUNG — in der einfachsten Form (EIN
  Committed-Frontier-Snapshot pro live Partition, Gültigkeit per
  seq-Präfix; siehe §8), nicht als §9.3-Ring. Der Ring bleibt
  ungebaut; sein Mehrwert (schnellere Erholung nach Backfill hinter
  der Frontier) griffe nur in einem seltenen Fall, der heute schlicht
  auf den immer korrekten Fold-ab-null zurückfällt.

Migration im Produkt-Repo (NICHT Teil dieses Auftrags, nur Erwartung):
pro Command Hand-`execute_optimistic` löschen → `append_to`; ein
`#[projection]` um den existierenden Fold/`open_rows`-Code; die 13
Server-Query-Handler sterben zugunsten des Log-Tails; Paritätstest lebt bis
Migrationsende, dann stirbt er mit dem Gerüst (`rpc/observer.rs`).

## 7. Offene Punkte (bei Umsetzung entscheiden, keine Blocker)

- Payload-Spalte: `String` (JSON) vs. neuer `CellValue::Bytes`. Start: JSON.
- Read-Handle (`RenderCtx`): Row-Lookup per PK reicht v1? (Vermutlich ja.)
- Fehler-Sichtbarkeit: Slot-Error genügt, oder zusätzlich wasmdb-debug-Event?
  (Empfehlung: beides, DebugEvent ist billig.)
- Grob-Recompute bei `#[reads]`-Dirty: alle Keys oder per Tabellen-Fanout
  begrenzen? v1: alle lebenden Keys, messen, dann entscheiden.
- Namenskonvention der Slot-Identity und DevTools-Darstellung im
  wasmdb-debug.

## 8. Umsetzungsstand

Implementiert und getestet (Workspace: 943 Tests grün, wasm32 baut):

- **M1 — `crates/core/database-projection`**: `Projection`-Trait +
  `ProjectionSpec` (`spec.rs`), Multiset-Diff (`diff.rs`),
  `ProjectionEngine` mit Registrierung (Ownership, Selbst-Zyklus,
  Kahn-Toposort mit Validierung VOR Commit), `derive`-Pass
  (Key-Extraktion, Read-Dirty → alle Live-Keys, Kaskade in Topo-Ordnung,
  Failure-Isolation pro Key) und `reset_and_rederive` (`engine.rs`);
  `DatabaseHost`-Adapter mit Index-Fastpath (`db_host.rs`).
  Tests: `tests/kernel.rs` (18, In-Memory-Fakes, inkl.
  Teleskop-Property), `tests/db_host.rs` (2, echte DB/PK-Upsert),
  5 Diff-Unit-Tests.
- **M1b — Hook in `database-reactive`**: `install_projections`
  (materialisiert Bestandsdaten), Derive-Pass in `notify` (läuft auch
  ohne Subscriber; Subscriber sehen den kombinierten Batch — ein
  Zyklus), Ownership-Guard in `apply_zset`
  (`DbError::OwnedByProjection`, VOR dem Apply), `replace_data` mit
  Reset+Rederive, Failure/Recovery-Buchhaltung als GEORDNETER
  Event-Strom (`ProjectionEvent::Failed`/`Recovered`,
  `take_projection_events`).
  Tests: `database-reactive/tests/projections.rs` (8).
  Hinweis: Raw-Writes über `db_mut_raw`/SQL auf owned Tables sind nicht
  hart abfangbar — `debug_assert` im Derive-Pass deckt sie im Debug-Build.
- **wasm-Registrierung**: `define_wasm_api!` hat einen optionalen
  Parameter `projections = <fn() -> ProjectionEngine>`; `sync_client`
  re-exportiert `database_projection`. Bestehende Aufrufformen
  unverändert (beide Demo-cdylibs bauen).
- **Akzeptanztests (M4-Kriterium, sync-client-Ebene)**:
  `sync-client/tests/projection_rebase.rs` — DER Rebase-Fall (Reject
  mit zwischengelandetem Fremd-Event ⇒ „Basis + fremd, ohne meine"),
  Confirm-Echo netto null auf abgeleitetem Zustand, Reject-Isolation
  pro Dokument.

- **M2 — Requirements-Integration**: `SlotKind::Projected` in
  `requirements` (Identity `projected:<id>:<key>`, `make_projected_key`,
  `upsert_projected`; Key-Repr = Display-Form des Engine-Keys). Nie
  gefetcht; Status aggregiert aus Upstream wie Derived (Log `Loading` ⇒
  Draft `Loading` — das Loading-Gate). Per-Key-`project()`-Fehler pinnen
  den Slot auf `Error` (`FetchError::Projection`,
  `report_projection_failure`) bis der Key nachweislich re-derived
  (`clear_projection_failure`) oder `invalidate` ihn löst — Upstream-
  Flattern unpinnt NICHT. Dafür liefert die Engine im `DeriveOutcome`
  jetzt `succeeded`-Paare; `ReactiveDatabase` verrechnet sie mit den
  Failures zu einem geordneten Event-Strom (`take_projection_events`)
  — geordnet, weil zwischen zwei Drains mehrere Derive-Pässe laufen
  können (Reconcile = Invert+Apply) und beim Konsumenten das letzte
  Event pro Key gewinnen muss. Recovered-Events nur für zuvor
  gescheiterte Keys; identische Wiederholungs-Failures erzeugen kein
  neues Event (bounded auch ohne Drain, keine redundanten
  Subscriber-Pings). wasm-Verdrahtung: `requires`-JSON kennt
  `{projection, partition, requires}`-Einträge (rekursiv, untagged),
  `define_wasm_api!` registriert die Drain-Quelle, gedraint wird nach
  Command-Execute, Batch-Response und Requirement-Apply; geänderte
  Slots pingen ihre JS-Callbacks. Tests: `requirements` (Slot- und
  Store-Ebene, F→P→D-Statuskette, Pinning, Idempotenz, GC/Backlinks),
  `database-reactive` (Failure→Recovery-Zyklus).
- **M3 — Macros & Codegen**:
  - `DbTable::from_cells` (sql-engine) als Inverse von `into_cells`,
    emittiert von `#[row]` UND dem tables-codegen-Row-Duplikat
    (Fehler nennen `tabelle.spalte`); Round-Trip-Tests in tables-e2e.
  - `database_projection::typed`: `decode_rows`, `partition_column_index`,
    typisierter `RenderCtx` (Reads als `#[row]`-Structs), `Out`-Collector
    (Output-Tabelle kommt vom Row-Typ). Auch ohne Macro nutzbar.
  - `#[projection]` (tables-macros, Re-Export via tables-storage):
    `#[projection(outputs(RowA, ...), reads(RowB, ...))]` auf einem
    inherenten Impl-Block des State-Typs; Macro emittiert den Shim auf
    das row-level `Projection`-Trait. (Der M3-Contract war eine
    typisierte `fn project(&[SourceRow]..., ...)` auf einer generierten
    Unit-Struct — INZWISCHEN ENTFERNT zugunsten des einen
    Fold-Contracts `apply`/`render` auf dem State-Typ, siehe den
    Fold-Contract-Eintrag unten; `key = ...` ist mit ihm gestorben.)
  - `append_to` in `#[rpc_command(append_to = LogRow)]` + `#[partition]`
    am Feld: emittiert `execute_optimistic` = genau eine Log-Row
    (`command_id = self.id`, Partitionswert aus dem markierten Feld,
    provisorische `seq = max+1` via `sync::append::next_seq` mit
    Index-Fastpath, `committed = 0`, `payload` = JSON-RPC-Form via
    `rpc_command::payload_json`). Row-Schema-Abweichungen sind
    Compile-Fehler am Struct-Literal — auch ein `#[partition]`-Marker
    auf einem Feld, das die Log-Row nicht hat.
    Tests: `rpc-command/tests/append.rs`.
  - `register_all_projections` in tables-codegen (Client-Mode): scannt
    `#[projection]`-Impls, emittiert `fn register_all_projections() ->
    ProjectionEngine` passend zum `projections =`-Parameter. Da
    Projektionen Funktionskörper tragen, werden sie REFERENZIERT statt
    re-emittiert: Builder-Option `.projections_path("::domain_crate")`
    (Default `"crate"`). Ohne Projektionen wird nichts emittiert (keine
    erzwungene Dependency). Unit-Tests gegen `tests/fixture/`.

- **M6a — generiertes Event-Schema (`#[projection_row]`)**:
  `#[projection_row]` auf einem Struct deklariert das Log. Deklariert
  werden NUR `command_id` (i64 oder Uuid — wird PK) und die
  Partitionsspalte; das Macro hängt `seq: i64`, `committed: i64`,
  `payload: String` an und expandiert zur vollen `#[row]`. Alles
  andere ist ein benannter Compile-Fehler (generierte Spalten
  deklariert, mehr als eine Partitionsspalte, eigenes `#[pk]`,
  Attribut-Argumente). (Zunächst als `#[projection]` auf dem Struct
  ausgeliefert — ein Attribut, Dispatch auf die Item-Form; umbenannt,
  weil die Struct-Form eine Row-SHAPE deklariert und keine Projektion —
  Spiegel zu `#[row]` für Tabellen. Ein Struct unter `#[projection]`
  ist heute ein benannter Compile-Fehler, der auf `#[projection_row]`
  zeigt.) tables-codegen scannt die Struct-Form als Row
  MIT den generierten Spalten (Client-Duplikat = expandierte Form;
  landet in `register_all_tables`, NICHT in
  `register_all_projections`). Im Zuge dessen heißt die
  Konventionsspalte jetzt `committed` (0 = off-chain/optimistisch)
  statt `pending` — emittiert von `#[rpc_command(append_to = ...)]`,
  konsistent in §2/§4.
  Tests: tables-e2e `projection_log.rs` (Schema-Form, Cell-Roundtrip,
  append_to Ende-zu-Ende), tables-codegen (Scan + Client-Emit).

- **M6a-Konsolidierung — Partition statt Key (§9.6)**: Terminologie
  konsequent im Code. `tables::ProjectionLog`-Trait (von der Log-Form
  implementiert): `PARTITION_COLUMN` + Accessoren + die Fold-Helfer
  `decode::<C>()` (payload → RPC-Command, Fehler nennt den Typ),
  `is_committed()`, `in_fold_order(&[Row])` (committed nach seq, dann
  Pendings nach provisorischer seq) als Default-Methoden. Log-Form ohne
  Attribut-Argument (Partition inferiert); Command markiert das
  Partitionsfeld mit `#[partition]` statt `key = ...`; Impl-Form
  inferiert über den Trait-Const (`key = <spalte>` existierte kurz als
  Override und ist mit der `project`-Form komplett entfernt).
  Engine-Begriffe:
  `PartitionedSource.partition_column` (vormals
  `KeyedSource.key_column`), `rows_for_partition`,
  `DeriveFailure::partition`, `ProjectionEvent::Recovered{partition}`,
  Requirements-`partition_repr`, `requires`-JSON-Feld `partition`. Das
  Slot-Key-FORMAT `projected:<id>:<wert>` blieb unverändert
  (wire-stabil). Tests: `projection_log.rs` (Inferenz, Helfer),
  `append.rs` (Marker), bestehende Suiten mechanisch nachgezogen.

- **Fold-Contract — der EINE Produkt-Contract (§9.4, vorgezogen)**: die
  `#[projection]`-Impl-Form hat genau einen Contract, und das
  Impl-Target IST der Fold-State (ES-Idiom, vgl. cqrs-es `Aggregate`;
  ursprünglich als statische Fns mit `state: &mut State`-Parameter
  ausgeliefert — der Bezug Projektion↔State hing dann nur an der
  `apply`-Signatur und wirkte magisch; die getrennte Unit-Struct
  entfiel bei der Umstellung).
  `apply(&mut self, row: &LogRow) -> Result<(), String>`
  bekommt immer genau EINE typisierte Log-Row; `render(&self, ctx, out)`
  emittiert die Outputs. Der Shim implementiert das row-level
  `Projection`-Trait (Engine-Schnittstelle) auf dem State-Typ selbst:
  Rows decodieren → `ProjectionLog::in_fold_order` → Fold ab
  `Default::default()` → einmal rendern — Contract und
  Ausführungsstrategie sind getrennt (§9.1: derselbe Fold, verschieden
  weit ausgeführt), der M6b-Snapshot-Ring ersetzt später nur den Shim.
  Registriert wird ein `Default`-Wert des Typs als Handle (versteckt in
  `register_all_projections`). Genau eine Quelle (der Row-Typ aus
  `apply`), Partition immer via `ProjectionLog`; `Default + Clone` wird
  im Shim erzwungen (der Ring wird klonen). Reads nur in
  `render` (§9.4 — sonst backt der State veraltete Read-Werte ein).
  Die frühere `project`-Slice-Form (M3) und ihr `key = ...`-Override
  sind ENTFERNT — beides gezielte Compile-Fehler; `typed::decode_rows`/
  `RenderCtx`/`Out` leben als Shim-Bausteine weiter, Hand-Impls des
  Trait-Levels bleiben möglich (so testen database-reactive/sync-client
  die Engine). Im selben Zug: `typed::RenderCtx` hat EINE Lifetime
  (`RenderCtx<'_>` statt `<'_, '_>`).
  Tests: tables-e2e `projection_fold.rs` (Spec, Fold-Ordnung übers
  Engine-E2E beobachtbar, Multi-Output, Decode-Fehler pinnt die
  Partition und lässt den letzten Render stehen, Read-Re-Render),
  `projection_log.rs` (Partition-Inferenz), tables-codegen
  (Scan/Registrierung).

- **M6b — inkrementelle Ausführung (Committed-Frontier-Snapshot,
  bewusst einfachste Form)**: KEIN Ring, KEIN X-Intervall — pro
  (Projektion, live Partition) genau EIN Memo: der State des kompletten
  committed Präfixes plus dessen seq-Liste
  (`typed::FoldSnapshot { seqs, state }`). Ablage im engine-eigenen
  `FoldCache` (opak, `dyn Any`), den `Projection::project` als neuen
  Parameter bekommt — die Engine besitzt ihn, weil NUR sie den
  Partition-Lifecycle kennt: Memo stirbt mit der Partition (letzte
  Quell-Row weg), `reset_and_rederive`/`replace_data` leert alle
  (seq-Listen könnten die neue Realität zufällig treffen). Der Shim:
  committed-seq-Liste bauen → `starts_with(memo.seqs)` ⇒ Resume per
  `clone()`, sonst Fold ab null → nur neue committete Rows folden →
  Memo fortschreiben (nur wenn neue committete Rows kamen; ein
  apply-Fehler im Pending-Tail lässt das gültige Memo stehen) →
  Pendings folden (nie memoisiert — sie reordnen und wandern beim
  Confirm) → einmal rendern. Konsequenz: Read-Dirty foldet NICHTS
  mehr, es rendert nur; der Fanout aus §7 kostet damit nur noch Render.
  `cache` ist per Contract Memo, nie Input — leerer Cache muss dasselbe
  Ergebnis liefern (im Trait dokumentiert). Neu tragend: committete
  Rows sind pro (Partition, seq) unveränderlich (§3(2)-Einschub).
  Hand-Impls ignorieren den Parameter. Tests:
  `projection_fold_incremental.rs` (apply-Zähler: nur neue committete
  Rows folden; Read-Dirty ohne Fold; Pendings ab Snapshot; Backfill
  hinter der Frontier ⇒ Fold ab null), kernel.rs (Memo stirbt mit der
  Partition, Reset leert alle Memos).

Entschieden (vormals §7 offen): Payload als JSON-String (`payload_json`);
Slot-Fehler-Sichtbarkeit via `FetchError::Projection` (DebugEvent
weiterhin offen). Bewusst NICHT umgesetzt: Slot-Drop-getriebene GC des
per-Key-Caches — solange gefetchte Log-Rows beim Slot-Drop in der DB
bleiben (DB-als-Cache-Semantik, wie bei Fetched), MUSS `last_render`
für vorhandene Daten erhalten bleiben (Invariante derived ≡
project(rows)); Cache-Speicher folgt der Datenpräsenz. Wenn Row-GC für
Fetched-Slots kommt, ist der Hook der Slot-Drop in
`RequirementStore::unsubscribe`.

Offen: M4 (Demo-Vertical im invoice-demo), M5 (README-Doku),
TS-Builder für `{projection, partition, requires}`-Einträge im
Frontend-Layer — M6 ist komplett (Contract + inkrementelle
Ausführung; nur der §9.3-Ring bleibt Ausbaustufe nach Messung). Produkte
können Projektionen heute vollständig nutzen: `#[projection_row]`-Log +
`#[rpc_command(append_to = ...)]`-Commands mit `#[partition]`-Feld +
`#[projection]`-Impl (`apply`/`render`) + `.projections_path(...)` im
build.rs + `projections = generated::register_all_projections` in
`define_wasm_api!`.

## 9. M6 — Fold-Knotensorte (UMGESETZT; Ausführung als Frontier-Snapshot, §9.3-Ring bleibt Ausbaustufe)

Ergebnis der Design-Diskussion (Juli 2026, nach M3): eine ZWEITE
Knotensorte im selben DAG für event-geförmte Quellen — inkrementeller
Fold statt recompute+diff. Sie ersetzt den Rows-Contract aus §3 NICHT;
sie ist die erste Einlösung des §2/§5-Versprechens „dieselbe Knotensorte,
andere Ausführungsstrategie" (wie das spätere SQL-IVM).

### 9.1 Warum das korrekt machbar ist (Präzisierung gegenüber §2)

Die §2-Ablehnung „Fold-State/Wasserlinie im Framework" bleibt für den
Default-Contract bestehen. Für eine OPT-IN-Knotensorte ist die Lage aus
drei Gründen anders:

1. **Die Server-Order ist final; nur die ANKUNFT ist unsortiert.**
   Committete Events werden vom Server nie umsortiert. Aus Client-Sicht
   kann sich ein committetes Event aber nachträglich VOR bereits
   empfangene einsortieren („vorsortiert"): der fremde Nachzügler
   (seq 95 trifft ein, 96–100 sind schon da) und das eigene Confirm,
   dessen autoritative seq vor schon empfangenen Tail-Events landet.
   Einsortieren ist damit die EINZIGE Störung des committed Teils —
   und sie ist als Dateneigenschaft erkennbar (Position der
   eintreffenden Row).
2. **Ein Reducer-Contract hat keine zwei Rechenwege.** Mit
   `apply(state, event)` sind „inkrementell" und „rebuild" derselbe
   Fold, nur verschieden weit ausgeführt:
   `fold(fold(init, präfix), suffix) == fold(init, präfix ++ suffix)`
   per Konstruktion. Kein Paritätsproblem (§1), kein still
   divergierender Fast Path.
3. **Die Deopt-Guards sind Daten-Eigenschaften, im ZSet prüfbar.**
   Ob ein State-Snapshot bei Position P noch gültig ist, entscheidet
   allein: ist eine committete Row mit Position ≤ P eingetroffen oder
   geändert worden? Das steht im Batch. (Compaction, Kettenbruch,
   `replace_data` ⇒ alle Snapshots verwerfen.) Gegensatz dazu: den
   BESTEHENDEN `&[T]`-Contract inkrementell auszuführen bliebe
   verworfen, weil die nötige Annahme („project ist ein Links-Fold")
   eine unprüfbare Code-Eigenschaft wäre.

### 9.2 Contracts (nicht verhandelbar)

1. **Datengetrieben, keine Sync-Kopplung.** „Committed" erfährt die
   Engine ausschließlich aus den Spalten (seq, committed-Flag) — §4.6:
   Bestätigt-Sein ist eine Daten-Eigenschaft. KEIN Verdict-Pfad in die
   Engine (das wäre der verworfene DerivedStream-Schnitt). Alle
   Snapshots sind jederzeit REIN aus den Rows rekonstruierbar.
2. **Rows bleiben die Wahrheit, Fold-States sind Caches.** Commands
   appenden weiterhin Log-Rows (`append_to` unverändert); die States
   folgen den Rows, nie umgekehrt. Sonst gäbe es zwei
   Rebase-Mechanismen im System (Row-Invert der Sync-Schicht +
   State-Replay), die übereinstimmen müssten; Echo/Tail-Idempotenz
   (PK `command_id`) und „die Daten allein genügen zur Rekonstruktion"
   gingen verloren. Fold-ab-0 über die Rows ist jederzeit der korrekte
   Boden.

### 9.3 Mechanik (v1: periodische Snapshots — bewusst stumpf)

UMGESETZT IN NOCH EINFACHERER FORM (siehe §8): statt eines Rings alle
X Events hält die Engine pro (Projektion, Partition) genau EINEN
Snapshot — den State des kompletten committed Präfixes, gültig solange
die aktuelle committed-seq-Liste die memoisierte fortsetzt
(`starts_with`). Kosten pro Derive damit O(neue committete + Pendings);
jede Invalidierung fällt auf Fold-ab-null zurück (die Ring-Erholung
entfiele nur in diesem seltenen Fall). Der Rest dieses Abschnitts
dokumentiert das ursprüngliche Ring-Design als Ausbaustufe, falls
Messung den Fold-ab-null-Rückfall je als Problem zeigt.

Entscheidung (Design-Diskussion): KEINE exakte Wasserlinien-/Loch-
Buchhaltung, kein `S_committed`/`S_optimistic`-Doppelbuffer (frühere
Skizze). Stattdessen hält die Engine pro (Projektion, Key) einen Ring
von State-Snapshots: **alle X committeten Events ein `clone()` des
Fold-States** (X = Tuning-Konstante). Gerendert wird IMMER per Fold ab
dem letzten gültigen Snapshot:

```
zustand = fold(snapshot.state,
               committed Rows mit Position > snapshot.pos
               ++ eigene Pendings nach provisorischer seq)
```

Regeln:
- **Snapshots backen NUR committete Events ein.** Pendings können
  verschwinden (Reject) oder wandern (Confirm mit anderer seq); da sie
  ohnehin ans Ende sortieren, liegen sie immer im frisch gefoldeten
  Tail. Kosten pro Render: O(X + Pendings) — bewusst kein O(1); dafür
  entfällt jede Loch-Buchhaltung.
- **EINE Invalidierungsregel:** jede committete Row, deren Position
  ≤ Snapshot-Position ist (neu eintreffend ODER geändert), verwirft
  diesen und alle späteren Snapshots. Das deckt den fremden
  Nachzügler UND das vorsortierte eigene Confirm mit derselben Regel.
  Kein gültiger Snapshot mehr ⇒ Fold ab 0 (bzw. ab Snap-Row) — der
  immer korrekte Boden. Verworfene Snapshots werden beim nächsten
  Fold-Durchlauf an den X-Grenzen neu gelegt.
- **Loch-Semantik bleibt Produkt-Sache in `apply`:** ob über Lücken in
  der committed seq gefoldet wird (Selbstheilung beim Nachzügler via
  Invalidierung) oder der State „am Loch stoppt", ist dem
  Snapshot-Mechanismus egal.
- `replace_data`, Install, Compaction, Kettenbruch: alle Snapshots
  verwerfen, Fold ab 0.

Output unverändert: gefoldeter Zustand → `render(state, ctx, out)` →
Rows → `multiset_diff` gegen `last_render`, Delta in den Batch.
Diff-Pfad, Ownership, Requirements-Slots (§4.2/M2) ändern sich NICHT —
die Knotensorte ist nur eine andere Strategie, `neu` zu berechnen.

### 9.4 Produkt-Contract

- **M6a — generiertes Event-Schema (UMGESETZT, siehe §8):**
  `#[projection_row]` auf einem Struct (eigenes Attribut, Spiegel zu
  `#[row]`; zunächst als `#[projection]` mit Item-Dispatch
  ausgeliefert, dann umbenannt — siehe §8). Das Framework
  generiert die Log-Row (`command_id`, `<partition>`, `seq`,
  `committed`, `payload`) aus der Deklaration — die
  `append_to`-Formkonvention ist generierter Code statt geprüfter
  Konvention. Die Spalten existieren weiter (sie SIND die Wire- und
  Wahrheitsform); niemand schreibt sie mehr von Hand. (Das
  ursprüngliche `key = ...`-Argument ist durch die
  Partition-Konsolidierung abgelöst — §9.6: die Partition wird aus der
  strikten Form inferiert.)
- **M6b — Fold-Contract (UMGESETZT als DER Contract der Impl-Form;
  Ausführung inkrementell per Frontier-Snapshot, siehe §8):**
  Das Impl-Target ist der State selbst, `Default + Clone` (im Shim
  erzwungen);
  `apply(&mut self, &LogRow) -> Result<(), String>`;
  `render(&self, &RenderCtx<'_>, &mut Out) -> Result<(), String>`.
  `apply` muss deterministisch sein (gleiche Purity-Regel wie
  `project`). Reads NUR in `render` — sonst backt der State veraltete
  Read-Werte ein; unter dem Snapshot-Ring re-rendert Read-Dirty nur,
  foldet nicht neu (unter der heutigen Recompute-Ausführung wird
  refoldet — per Determinismus dasselbe Ergebnis).
  Präzisierung der Schnittstelle:
  - `apply` bekommt die typisierte LOG-ROW (die M6a-generierte
    Struktur), NICHT den vordecodierten RPC-Command. Drei Gründe:
    (1) `seq` ist Fold-Input — die Loch-Regel (§9.3) ist sonst in
    `apply` nicht implementierbar; (2) `committed` ist Fold-Input —
    Views, die Pending-Zustand anzeigen, brauchen es; (3) die
    Decode-Politik gehört ins Produkt: ein unbekannter Command-Typ
    (neuere Client-Version im zweiten Tab, via Tail) darf wahlweise
    übersprungen (Forward-Kompatibilität, server-äquivalentes No-op)
    oder als Fehler gemeldet werden. Decodiert wird per generiertem
    `decode::<C>()` (§9.6) — eine Zeile, `?` genügt; `Err` ⇒
    `DeriveFailure` der Partition (Slot-Pin wie beim
    Recompute-Knoten). Der Shim reicht typisierte Rows via
    `from_cells` — exakt das Muster des M3-Shims.
  - Registrierung über die `#[projection]`-Impl-Form —
    `outputs(...)`/`reads(...)` unverändert, Ownership und DAG gelten
    identisch. `apply`/`render` IST die Impl-Form (die frühere
    `project`-Slice-Form wurde entfernt — es gibt keinen zweiten
    Produkt-Contract); für Registry, Requirements-Slots und Diff-Pfad
    ist der Fold von jeder anderen `Projection`-Trait-Impl
    ununterscheidbar.

### 9.5 Abgrenzung / Bau-Kriterium

- Der Fold ist der EINE Produkt-Contract; der Recompute-Knoten ist
  seine heutige AUSFÜHRUNG, kein zweiter Contract. Sichten, die kein
  Fold sind (CRUD-/Nicht-Log-Quellen, LWW/Dedupe/eigene Ordnung,
  Multi-Source-Interleaving), sind kein Macro-Fall — falls je
  gebraucht: Hand-Implementierung des row-level `Projection`-Traits
  (Engine-Schnittstelle) oder später die SQL-IVM-Knotensorte.
- Preis der umgesetzten Frontier-Snapshot-Ausführung: EIN geklonter
  State + die committed-seq-Liste pro live Partition, zusätzlich zu
  `last_render` — Speicher folgt der Datenpräsenz, kein Tuning-Knopf.
- Bau-Kriterium für MEHR (der §9.3-Ring, per-Partition-Read-Tracking):
  MESSEN (gleiche Haltung wie beim Read-Dirty-Fanout, §7). Der Ring
  lohnt erst, wenn der Fold-ab-null-Rückfall nach Backfill nachweislich
  wehtut; die Snapshot-Konvention deckelt N ohnehin, weil auch der
  Ladepfad keine unbeschränkten Logs verträgt.

### 9.6 Partition statt Key — DX-Konsolidierung (UMGESETZT)

Terminologie-Entscheidung: Die Stream-Identität des Logs heißt
**Partition** (statt „Key") — der Begriff sagt, was sie ist: die
Partitionierung EINER Tabelle in viele unabhängige Logs (einer pro
Dokument), mit den bekannten Konsequenzen: `seq` zählt PRO Partition,
Recompute kostet O(Partition), Loading-/Fehler-Zustand und Lifecycle
sind pro Partition isoliert (§3.8), M6b-Fold-State ist pro Partition.

Relevanz-Einordnung (entschieden): Im CODE ist die Partition fast
vollständig Implementationsdetail; als DESIGN-Konzept bleibt sie die
eine Entscheidung pro Vertical — die Partitionsgranularität ist die
Kosten- und Isolationsgrenze. Konkret pro Ebene:

- **Log-Deklaration: KEIN Attribut-Argument nötig.** Die strikte
  M6a-Form (genau `command_id` + EIN weiteres Feld) macht die Partition
  inferierbar — das eine weitere Feld IST sie:
  `#[projection_row] pub struct DraftLog { pub command_id: i64, pub doc_id: i64 }`.
  Das `key = ...`-Argument entfällt. Ein explizites
  `#[partition]`-Feld-Attribut ist reserviert für den Fall, dass die
  Log-Form später weitere Spalten erlaubt (Server-Chain-Spalten §4.4)
  und die Inferenz mehrdeutig wird.
- **Command: `#[partition]` am Feld — die EINE sichtbare Stelle.**
  Das Macro muss wissen, welches Command-Feld den Partitionswert
  trägt; fremde Typen kann es nicht inspizieren, und
  Laufzeit-Extraktion aus dem Payload-JSON würde einen Compile-Fehler
  gegen einen Laufzeitfehler tauschen (gegen die Linie „Formfehler
  sind Compile-Fehler"). Form:
  `#[rpc_command(append_to = DraftLog)]` + `#[partition]` am Feld
  statt `key = doc_id` im Attribut. Ein falsch markiertes Feld ist ein
  Compile-Fehler am generierten Struct-Literal.
- **Impl-Form: inferiert.** Die Log-Form implementiert
  `tables::ProjectionLog` (Trait-Const `PARTITION_COLUMN`); die
  Projektion nimmt die Partition vom Row-Typ aus `apply`. `key = ...`
  ist KOMPLETT entfernt (es existierte kurz als Override für
  Nicht-Log-Quellen der `project`-Slice-Form; beide sind gestrichen —
  die Quelle einer Projektion ist immer ein Log).
- **Frontend: Wert ja, Begriff nein.** Die Subscription braucht
  naturgemäß den Partitionswert („Dokument 17"), aber als Argument
  des generierten TS-Builders — der Begriff taucht im Produkt-Code
  nicht auf. (Das `requires`-JSON-Feld heißt `partition`; der
  TS-Builder selbst kommt mit M4.)
- **Generierte Fold-Helfer** (im selben Zug): `decode::<C>()`
  (payload → RPC-Command), `in_fold_order(&[Row])` (committed nach
  seq, dann Pendings nach provisorischer seq), `is_committed()` —
  ersetzt den identischen Vorspann jeder Projektion und ist die
  Vorarbeit für M6b (dieselbe Ordnung). Umgesetzt als
  Default-Methoden von `tables::ProjectionLog` über den drei
  generierten Accessoren. Produkt-Sache bleiben: unfoldbare Events
  überspringen, Domain-Mapping, Loch-Regel.

Umgesetzt (Details §8): Marker + Inferenz in tables-macros/rpc-command,
`tables::ProjectionLog` (Const + Helfer), `key =` → Override-Only,
Engine-Begriffe (`PartitionedSource.partition_column`,
`rows_for_partition`, `DeriveFailure::partition`, Requirements
`partition_repr`, `requires`-JSON `partition`) inkl.
tables-codegen-Scan. Das Slot-Key-FORMAT `projected:<id>:<wert>` blieb
unverändert.

## 10. Code-Pointer (Ist-Stand)

- `crates/sync/sync-client/src/client.rs` — Reconcile (Invert+Apply-Batches),
  `execute` → `notify` (Hook-Einstieg Pfad 2).
- `crates/sync/sync-client/src/stream.rs` — Pending-Buchhaltung (bleibt).
- `crates/core/database-reactive/src/reactive_database.rs` — apply/notify
  (Hook-Heimat).
- `crates/core/database/src/execute/apply.rs` — `apply_zset`-Semantik.
- `crates/requirements/src/store/slot.rs` — SlotKind/Lifecycle/Status.
- `crates/tables/tables-codegen/src/emit.rs` — `register_all_tables`-Muster.
- `crates/rpc-command/rpc-command/src/lib.rs` — Macro-Familie
  (`#[rpc_command]`, `#[rpc_command_enum]`) + `payload_json`.
- `crates/sync/sync/src/append.rs` — Append-Primitive (`next_seq`,
  `append_row`) für hand-geschriebene `execute_optimistic`.
- `crates/sync/sync-client/src/wasm/mod.rs` — `define_wasm_api!`.
- `examples/invoice-demo/` — Referenz-DX (BlurInput → patch → execute;
  `shared/domain/*/command/*.rs`; `frontend/apps/wasm/src/lib.rs`).
