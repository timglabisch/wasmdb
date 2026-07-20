//! Building blocks for the optimistic execution of event-append commands.
//!
//! An event-append command's whole optimistic effect is ONE new row in
//! its log table (`docs/wasmdb-projections-design.md` §4.4): derived
//! state is the projection engine's job, and because every command is
//! its own disjoint row, the sync layer's invert-based reconcile stays
//! correct. A command hand-writes `execute_optimistic` and appends its
//! event through these helpers (`next_seq` + `append_row`, with
//! `rpc_command::payload_json` for the payload) — appending is an effect
//! the command performs, not its identity, so the same call can come from
//! any trigger (an HTTP API, an MCP tool).

use database::Database;
use sql_engine::storage::{CellValue, ZSet};
use sql_engine::DbTable;

use crate::command::CommandError;

/// Next provisional sequence number for `partition` in `R`'s log table:
/// `max(seq) + 1` over the live rows of that partition, `0` for the
/// first event. Provisional — the server assigns the authoritative `seq`
/// when it accepts the command into the chain.
///
/// `R` must have a `seq` I64 column; `partition_column` is looked up by
/// name. Uses a single-column index when one exists, else scans.
pub fn next_seq<R: DbTable>(
    db: &Database,
    partition_column: &str,
    partition: &CellValue,
) -> Result<i64, CommandError> {
    let schema = R::schema();
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
    let seq_idx = schema
        .columns
        .iter()
        .position(|c| c.name == "seq")
        .ok_or_else(|| {
            CommandError::ExecutionFailed(format!(
                "log table '{}' has no 'seq' column",
                R::TABLE
            ))
        })?;
    let Some(t) = db.table(R::TABLE) else {
        return Err(CommandError::ExecutionFailed(format!(
            "log table '{}' is not registered",
            R::TABLE
        )));
    };

    let mut max: Option<i64> = None;
    let mut consider = |row_id: usize| {
        if let CellValue::I64(s) = t.get(row_id, seq_idx) {
            max = Some(max.map_or(s, |m| m.max(s)));
        }
    };
    if let Some(idx) = t.index_for_column(partition_idx) {
        if let Some(ids) = idx.lookup_eq(std::slice::from_ref(partition)) {
            for &r in ids.iter().filter(|&&r| !t.is_deleted(r)) {
                consider(r);
            }
        }
    } else {
        for r in t.row_ids().filter(|&r| t.get(r, partition_idx) == *partition) {
            consider(r);
        }
    }
    Ok(max.map_or(0, |m| m + 1))
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
