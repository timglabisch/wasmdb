//! Phase 0 of query execution: resolve all `PlanSource::Requirement` entries.
//!
//! For every requirement in the plan (main query + every materialization
//! sub-plan), this phase:
//!   1. Resolves the requirement's args against `ctx.bound_values` / `params`.
//!   2. Invokes the registered async fetcher with the resolved args.
//!   3. Upserts each returned row into the backing `row_table` (persistent —
//!      data has the lifetime of the DB, not the query).
//!   4. Extracts the PK tuples from the returned rows and stores them in a
//!      `RequirementsResult`, keyed by `(caller_id, resolved_args)`.
//!
//! Phase 3's `scan_requirement` then reads those PK tuples from the
//! `RequirementsResult` instead of invoking fetchers itself — the data is
//! already in the DB.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sql_parser::ast::Value;

use crate::planner::shared::plan::{
    PlanFilterPredicate, PlanSelect, PlanSource, PlanSourceEntry, RequirementArg,
};
use crate::planner::sql::plan::ExecutionPlan;
use crate::storage::{CellValue, Table};

use super::{cell_to_value, value_to_cell, ExecuteError, ParamValue, Params};

// ── Fetcher runtime ───────────────────────────────────────────────────────

/// Future returned by an async fetcher. Fetchers receive owned args (so the
/// future can outlive the call site) and yield full rows — cells in the
/// `row_table`'s column order. Phase 0 upserts these into the row_table and
/// extracts the PK tuples for Phase 3 to scan.
///
/// `+ Send` so that a `Database` holding a `FetcherRuntime` is itself `Send`,
/// enabling `Arc<Mutex<Database>>`-style sharing in axum handlers.
pub type FetcherFuture = Pin<Box<dyn Future<Output = Result<Vec<Vec<CellValue>>, String>> + Send>>;

/// A registered fetcher. Invoked during Phase 0 (`resolve_requirements`).
/// `Send + Sync` so `Database` stays `Send`; wrapped in `Arc` so cloning a
/// `FetcherRuntime` / `Database` shares closure identity rather than
/// forcing every closure to be `Clone` (most capture Arc-wrapped state
/// like DB pools).
pub type AsyncFetcherFn = Arc<dyn Fn(Vec<Value>) -> FetcherFuture + Send + Sync>;

/// Registry of fetcher implementations, keyed by `"{schema}::{function}"`.
pub type FetcherRuntime = HashMap<String, AsyncFetcherFn>;

// ── Requirements result ───────────────────────────────────────────────────

/// Key into [`RequirementsResult`]: the fully-resolved caller identity at a
/// given point in the query — `(caller_id, resolved_args_as_cells)`.
/// `Value` isn't `Hash` (because of `Float(f64)`), so we key by
/// `Vec<CellValue>` which is.
pub type RequirementKey = (String, Vec<CellValue>);

/// Proof that Phase 0 has executed for a given plan. Produced by
/// [`resolve_requirements`] and consumed by [`super::execute_plan`].
///
/// For each `(caller_id, args)` invocation in the plan, stores the PK tuples
/// the fetcher produced. Phase 3's `scan_requirement` consults this map to
/// learn which rows of the row_table the invocation covers — the rows
/// themselves already live in the DB (upserted by Phase 0).
#[derive(Debug, Default, Clone)]
pub struct RequirementsResult {
    pub pk_sets: HashMap<RequirementKey, Vec<Vec<Value>>>,
}

impl RequirementsResult {
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, caller_id: &str, args: &[Value]) -> Option<&[Vec<Value>]> {
        let key_args: Vec<CellValue> = args.iter().map(value_to_cell).collect();
        self.pk_sets
            .get(&(caller_id.to_string(), key_args))
            .map(|v| v.as_slice())
    }
}

/// Resolve every requirement in `plan` against `fetchers` and upsert the
/// returned rows into `db`. Returns a `RequirementsResult` that Phase 3 uses
/// to look up which PKs each invocation produced.
///
/// Requirements cannot currently depend on each other's output — args must
/// resolve from `params` + `plan.bound_values` alone. A fetcher that needs
/// data from another fetcher's result is out of scope for this phase.
pub async fn resolve_requirements(
    db: &mut HashMap<String, Table>,
    plan: &ExecutionPlan,
    params: &Params,
    fetchers: &FetcherRuntime,
) -> Result<RequirementsResult, ExecuteError> {
    let mut result = RequirementsResult::default();
    let mut seen: HashSet<RequirementKey> = HashSet::new();

    let invocations = collect_invocations(plan);
    for inv in invocations {
        let resolved_args = resolve_args(&plan.bound_values, params, &inv)?;
        let arg_cells: Vec<CellValue> = resolved_args.iter().map(value_to_cell).collect();
        let key: RequirementKey = (inv.caller_id.clone(), arg_cells);

        if !seen.insert(key.clone()) {
            continue;
        }

        let fetcher = fetchers.get(&inv.caller_id).ok_or_else(|| {
            ExecuteError::CallerError(format!("fetcher `{}` not registered", inv.caller_id))
        })?;

        let rows = fetcher(resolved_args.clone()).await.map_err(|e| {
            ExecuteError::CallerError(format!("fetcher `{}` failed: {e}", inv.caller_id))
        })?;

        let table = db.get_mut(&inv.row_table).ok_or_else(|| {
            ExecuteError::TableNotFound(inv.row_table.clone())
        })?;
        let pk_columns = table.schema.primary_key.clone();
        if pk_columns.is_empty() {
            return Err(ExecuteError::CallerError(format!(
                "fetcher `{}` row_table `{}` has no primary key",
                inv.caller_id, inv.row_table,
            )));
        }
        let expected_col_count = table.schema.columns.len();

        let mut pk_tuples: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.into_iter().enumerate() {
            if row.len() != expected_col_count {
                return Err(ExecuteError::CallerError(format!(
                    "fetcher `{}` row {row_idx}: returned {} cells, row_table `{}` has {} columns",
                    inv.caller_id, row.len(), inv.row_table, expected_col_count,
                )));
            }
            let pk: Vec<Value> = pk_columns.iter().map(|&c| cell_to_value(&row[c])).collect();
            table.upsert_by_pk(&row).map_err(|e| {
                ExecuteError::CallerError(format!(
                    "fetcher `{}` upsert into `{}` failed: {e}",
                    inv.caller_id, inv.row_table,
                ))
            })?;
            pk_tuples.push(pk);
        }

        result.pk_sets.insert(key, pk_tuples);
    }

    Ok(result)
}

/// One invocation site of a caller in the plan.
struct Invocation {
    caller_id: String,
    row_table: String,
    args: Vec<RequirementArg>,
}

fn collect_invocations(plan: &ExecutionPlan) -> Vec<Invocation> {
    let mut out = Vec::new();
    collect_from_select(&plan.main, &mut out);
    for step in &plan.materializations {
        collect_from_select(&step.plan, &mut out);
    }
    out
}

fn collect_from_select(select: &PlanSelect, out: &mut Vec<Invocation>) {
    for entry in &select.sources {
        collect_from_source_entry(entry, out);
    }
    // `filter` can contain nested materialized predicates but no sub-SELECTs
    // at this stage — all Subquery ASTs have been lowered into materializations.
    let _ = select.filter.clone();
}

fn collect_from_source_entry(entry: &PlanSourceEntry, out: &mut Vec<Invocation>) {
    if let PlanSource::Requirement { caller_id, row_table, args, .. } = &entry.source {
        out.push(Invocation {
            caller_id: caller_id.clone(),
            row_table: row_table.clone(),
            args: args.clone(),
        });
    }
    // `entry.pre_filter` and `entry.join.on` are predicate trees over the
    // source's columns; they don't contain nested sources.
    let _ = entry.pre_filter.clone();
    if let Some(_j) = &entry.join {
        let _ = PlanFilterPredicate::None;
    }
}

fn resolve_args(
    bound_values: &HashMap<String, Value>,
    params: &Params,
    inv: &Invocation,
) -> Result<Vec<Value>, ExecuteError> {
    inv.args.iter().enumerate().map(|(idx, arg)| match arg {
        RequirementArg::Placeholder(name) => {
            if let Some(v) = bound_values.get(name) {
                return Ok(v.clone());
            }
            if let Some(pv) = params.get(name) {
                return param_value_to_value(pv).ok_or_else(|| ExecuteError::BindError(format!(
                    "fetcher `{}` arg {idx}: placeholder :{name} is a list, expected scalar",
                    inv.caller_id,
                )));
            }
            Err(ExecuteError::BindError(format!(
                "fetcher `{}` arg {idx}: missing value for placeholder :{name}",
                inv.caller_id,
            )))
        }
    }).collect()
}

fn param_value_to_value(pv: &ParamValue) -> Option<Value> {
    match pv {
        ParamValue::Int(n) => Some(Value::Int(*n)),
        ParamValue::Text(s) => Some(Value::Text(s.clone())),
        ParamValue::Null => Some(Value::Null),
        ParamValue::IntList(_) | ParamValue::TextList(_) => None,
    }
}

