//! Commit-chain-v2 gap-repair core (design §11.4).
//!
//! When the server links a confirmed row onto a `server_parent_id` the
//! client never fetched (another writer was ahead of it on that
//! partition), the client holds a *committed* row whose ancestor is
//! missing — a chain gap. Repair is a backward walk: fetch the unknown
//! parent by PK, then its parent, until the committed chain is contiguous
//! from `ROOT`; each fetched row is applied to the reactive database,
//! which re-folds the affected projection partition automatically.
//!
//! This module is the pure, host-testable core: it computes *which*
//! parents are missing right now. The wasm side ([`crate::wasm`]) drives
//! the async fetch/apply loop around it. Kept decoupled from `tables` the
//! same way [`sync::append`] is — the chain columns are referenced by
//! name, not by importing the projection layer.

use std::collections::HashSet;

use database::Database;
use sql_engine::storage::{CellValue, Uuid};

/// Nil UUID — the chain root sentinel (design §11), mirroring
/// `tables::ROOT_PARENT`. `ROOT` is never a real row, so it is never
/// "missing".
const ROOT_PARENT: Uuid = Uuid([0u8; 16]);

/// The `server_parent_id`s that committed rows in `table` point at but the
/// client does not hold as a `command_id` — the immediate gap frontier
/// (design §11.4). Empty means the committed chain is contiguous: nothing
/// to repair. Result is deduplicated and sorted for deterministic fetches.
///
/// `table` must have `command_id` (Uuid PK) and `server_parent_id`
/// (nullable Uuid) columns, looked up by name. A row is *committed* when
/// its `server_parent_id` is non-NULL; `ROOT` parents and parents already
/// present locally are not gaps.
pub fn missing_parents(db: &Database, table: &str) -> Vec<Uuid> {
    let Some(t) = db.table(table) else {
        return Vec::new();
    };
    let col = |name: &str| t.schema.columns.iter().position(|c| c.name == name);
    let (Some(command_id_idx), Some(server_parent_idx)) =
        (col("command_id"), col("server_parent_id"))
    else {
        return Vec::new();
    };

    let mut known: HashSet<Uuid> = HashSet::new();
    let mut referenced: Vec<Uuid> = Vec::new();
    for r in t.row_ids().filter(|&r| !t.is_deleted(r)) {
        if let CellValue::Uuid(b) = t.get(r, command_id_idx) {
            known.insert(Uuid(b));
        }
        // Only committed rows (server_parent_id = Some) carry an
        // authoritative predecessor to backfill.
        if let CellValue::Uuid(b) = t.get(r, server_parent_idx) {
            referenced.push(Uuid(b));
        }
    }

    let mut missing: Vec<Uuid> = referenced
        .into_iter()
        .filter(|p| *p != ROOT_PARENT && !known.contains(p))
        .collect();
    missing.sort();
    missing.dedup();
    missing
}

/// Of `ids`, the ones this client does not already hold as a `command_id`
/// in `table`. Used to bootstrap: the server's chain heads are fetched, but
/// only the *unknown* ones — re-applying a head already present would insert
/// a duplicate row and double-count the fold. An absent table (nothing
/// registered yet) means every id is unknown.
pub fn unknown_ids(db: &Database, table: &str, ids: &[Uuid]) -> Vec<Uuid> {
    let Some(t) = db.table(table) else {
        return ids.to_vec();
    };
    let Some(command_id_idx) = t.schema.columns.iter().position(|c| c.name == "command_id") else {
        return ids.to_vec();
    };

    let mut known: HashSet<Uuid> = HashSet::new();
    for r in t.row_ids().filter(|&r| !t.is_deleted(r)) {
        if let CellValue::Uuid(b) = t.get(r, command_id_idx) {
            known.insert(Uuid(b));
        }
    }
    ids.iter().copied().filter(|id| !known.contains(id)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
    use sql_engine::storage::CellValue;

    fn uuid(n: u8) -> Uuid {
        let mut b = [0u8; 16];
        b[15] = n;
        Uuid(b)
    }

    /// A minimal `ledger_log`-shaped table: command_id, server_parent_id.
    fn db_with_rows(rows: &[(Uuid, Option<Uuid>)]) -> Database {
        let schema = TableSchema {
            name: "ledger_log".into(),
            columns: vec![
                ColumnSchema { name: "command_id".into(), data_type: DataType::Uuid, nullable: false },
                ColumnSchema { name: "server_parent_id".into(), data_type: DataType::Uuid, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut db = Database::new();
        db.create_table(schema).unwrap();
        for (id, parent) in rows {
            let parent_cell = match parent {
                Some(p) => CellValue::Uuid(p.0),
                None => CellValue::Null,
            };
            db.insert("ledger_log", &[CellValue::Uuid(id.0), parent_cell]).unwrap();
        }
        db
    }

    #[test]
    fn contiguous_committed_chain_has_no_gap() {
        // ROOT → a → b, all present and committed. Nothing missing.
        let db = db_with_rows(&[
            (uuid(1), Some(ROOT_PARENT)),
            (uuid(2), Some(uuid(1))),
        ]);
        assert!(missing_parents(&db, "ledger_log").is_empty());
    }

    #[test]
    fn pending_rows_reference_no_parent_to_fetch() {
        // A pending row (server_parent_id = NULL) never names a gap, even
        // though its predecessor is unknown — only the server's link does.
        let db = db_with_rows(&[(uuid(9), None)]);
        assert!(missing_parents(&db, "ledger_log").is_empty());
    }

    #[test]
    fn unknown_committed_parent_is_a_gap() {
        // The client holds `b` (committed after `a`), but not `a`.
        // `a` is the gap frontier to backfill.
        let db = db_with_rows(&[(uuid(2), Some(uuid(1)))]);
        assert_eq!(missing_parents(&db, "ledger_log"), vec![uuid(1)]);
    }

    #[test]
    fn gaps_are_deduped_and_sorted() {
        // Two committed rows both point at the same unknown parent, plus a
        // second distinct unknown parent — one deduped, sorted frontier.
        let db = db_with_rows(&[
            (uuid(5), Some(uuid(3))),
            (uuid(6), Some(uuid(3))),
            (uuid(7), Some(uuid(1))),
        ]);
        assert_eq!(missing_parents(&db, "ledger_log"), vec![uuid(1), uuid(3)]);
    }

    #[test]
    fn missing_table_is_no_gap() {
        let db = Database::new();
        assert!(missing_parents(&db, "ledger_log").is_empty());
    }

    #[test]
    fn unknown_ids_filters_out_rows_already_held() {
        // Client holds uuid(1) and uuid(2). Of a heads answer [1, 2, 3],
        // only 3 is unknown — re-fetching 1/2 would double-count the fold.
        let db = db_with_rows(&[(uuid(1), Some(ROOT_PARENT)), (uuid(2), Some(uuid(1)))]);
        assert_eq!(
            unknown_ids(&db, "ledger_log", &[uuid(1), uuid(2), uuid(3)]),
            vec![uuid(3)]
        );
    }

    #[test]
    fn unknown_ids_on_empty_client_returns_all() {
        // Fresh bootstrap: table absent → every head is unknown and fetched.
        let db = Database::new();
        assert_eq!(
            unknown_ids(&db, "ledger_log", &[uuid(1), uuid(2)]),
            vec![uuid(1), uuid(2)]
        );
    }
}
