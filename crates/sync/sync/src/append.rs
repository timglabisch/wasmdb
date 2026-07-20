//! Building blocks for the optimistic execution of event-append commands.
//!
//! An event-append command's whole optimistic effect is ONE new row in
//! its log table (`docs/wasmdb-projections-design.md` §4.4): derived
//! state is the projection engine's job, and because every command is
//! its own disjoint row, the sync layer's invert-based reconcile stays
//! correct. A command hand-writes `execute_optimistic` and appends its
//! event through these helpers (`client_head` + `append_row`, with
//! `rpc_command::payload_json` for the payload) — appending is an effect
//! the command performs, not its identity, so the same call can come from
//! any trigger (an HTTP API, an MCP tool).

use std::collections::HashSet;

use database::Database;
use sql_engine::storage::{CellValue, Uuid, ZSet};
use sql_engine::DbTable;

use crate::command::CommandError;

/// Nil UUID — the chain root sentinel (design §11), mirroring
/// `tables::ROOT_PARENT`. Defined locally so this crate stays decoupled
/// from `tables`: like the column names below, the chain convention is
/// referenced by value, not by importing the projection layer.
const ROOT_PARENT: Uuid = Uuid([0u8; 16]);

/// The current client-chain head for `partition` in `R`'s log table: the
/// PK of the partition's chain TAIL — the row that no other row of that
/// partition references via `client_parent_id`. `ROOT_PARENT` when the
/// partition is empty. A new optimistic row sets this as its
/// `client_parent_id` (design §11), extending the client's chain.
///
/// `R` must have `command_id` (Uuid PK) and `client_parent_id` (Uuid)
/// columns; `partition_column` is looked up by name. Uses a single-column
/// index when one exists, else scans. A clean linear chain has exactly one
/// tail; if it ever forks, the max PK is chosen for a deterministic result.
pub fn client_head<R: DbTable>(
    db: &Database,
    partition_column: &str,
    partition: &CellValue,
) -> Result<Uuid, CommandError> {
    let schema = R::schema();
    let column = |name: &str| {
        schema
            .columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                CommandError::ExecutionFailed(format!(
                    "log table '{}' has no '{name}' column",
                    R::TABLE
                ))
            })
    };
    let partition_idx = schema
        .columns
        .iter()
        .position(|c| c.name == partition_column)
        .ok_or_else(|| {
            CommandError::ExecutionFailed(format!(
                "log table '{}' has no partition column '{partition_column}'",
                R::TABLE
            ))
        })?;
    let command_id_idx = column("command_id")?;
    let client_parent_idx = column("client_parent_id")?;
    let Some(t) = db.table(R::TABLE) else {
        return Err(CommandError::ExecutionFailed(format!(
            "log table '{}' is not registered",
            R::TABLE
        )));
    };

    // The tail is the command_id that no row in the partition names as its
    // client parent. Collect both sets, then subtract.
    let mut ids: Vec<Uuid> = Vec::new();
    let mut parents: HashSet<Uuid> = HashSet::new();
    let mut consider = |row_id: usize| {
        if let CellValue::Uuid(b) = t.get(row_id, command_id_idx) {
            ids.push(Uuid(b));
        }
        if let CellValue::Uuid(b) = t.get(row_id, client_parent_idx) {
            parents.insert(Uuid(b));
        }
    };
    if let Some(idx) = t.index_for_column(partition_idx) {
        if let Some(rids) = idx.lookup_eq(std::slice::from_ref(partition)) {
            for &r in rids.iter().filter(|&&r| !t.is_deleted(r)) {
                consider(r);
            }
        }
    } else {
        for r in t.row_ids().filter(|&r| t.get(r, partition_idx) == *partition) {
            consider(r);
        }
    }
    Ok(ids
        .into_iter()
        .filter(|id| !parents.contains(id))
        .max()
        .unwrap_or(ROOT_PARENT))
}

/// Insert the built log row and return the +1 ZSet describing exactly it
/// — the append-only optimistic effect.
pub fn append_row<R: DbTable>(db: &mut Database, row: R) -> Result<ZSet, CommandError> {
    let cells = row.into_cells();
    db.insert(R::TABLE, &cells)
        .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
    let mut zset = ZSet::new();
    zset.insert(R::TABLE.into(), cells);
    Ok(zset)
}
