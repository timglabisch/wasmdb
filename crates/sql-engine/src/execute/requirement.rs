//! Phase 0 of query execution: resolve all `PlanSource::Requirement` entries.
//!
//! Split into three explicit sub-phases so callers can avoid holding
//! `&mut db` across `.await`:
//!
//!   0a. [`collect_fetch_plan`] — sync, reads `&plan` + `&params`. Produces a
//!       deduplicated `Vec<ResolvedInvocation>`.
//!   0b. [`fetch_requirements`] — async, takes **no** DB reference. Invokes
//!       all registered fetchers in parallel via the internal `join_all`.
//!   0c. [`apply_fetched`] — sync, `&mut db`. Upserts every returned row into
//!       the backing `row_table` and extracts the PK tuples.
//!
//! Phase 3's `scan_requirement` then reads those PK tuples from the
//! `RequirementsResult` — the rows themselves live in the DB.
//!
//! [`resolve_requirements`] stays as a convenience wrapper that runs all
//! three in sequence; the sql-engine pipeline still uses it.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
/// Native builds carry `+ Send` so a `Database` holding a `FetcherRuntime`
/// can cross threads (Axum handlers wrap it in `Arc<Mutex<_>>`). The wasm
/// client runs single-threaded and the HTTP fetcher uses `JsFuture`, which
/// holds `Rc<RefCell<_>>` and can't be `Send` — the bound is dropped there.
#[cfg(not(target_arch = "wasm32"))]
pub type FetcherFuture =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<CellValue>>, String>> + Send>>;

#[cfg(target_arch = "wasm32")]
pub type FetcherFuture =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<CellValue>>, String>>>>;

/// A registered fetcher. Invoked during Phase 0 (`fetch_requirements`).
/// Native builds require `Send + Sync` so `Database` stays `Send`; on wasm
/// both bounds are dropped for the same reason `FetcherFuture` loses `Send`
/// there. `Arc` so cloning shares closure identity rather than forcing every
/// closure to be `Clone`.
#[cfg(not(target_arch = "wasm32"))]
pub type AsyncFetcherFn = Arc<dyn Fn(Vec<Value>) -> FetcherFuture + Send + Sync>;

#[cfg(target_arch = "wasm32")]
pub type AsyncFetcherFn = Arc<dyn Fn(Vec<Value>) -> FetcherFuture>;

/// Registry of fetcher implementations, keyed by `"{schema}::{function}"`.
pub type FetcherRuntime = HashMap<String, AsyncFetcherFn>;

// ── Requirements result ───────────────────────────────────────────────────

/// Key into [`RequirementsResult`]: the fully-resolved caller identity at a
/// given point in the query — `(caller_id, resolved_args_as_cells)`.
/// `Value` isn't `Hash` (because of `Float(f64)`), so we key by
/// `Vec<CellValue>` which is.
pub type RequirementKey = (String, Vec<CellValue>);

/// Proof that Phase 0 has executed for a given plan. Produced by
/// [`apply_fetched`] and consumed by [`super::execute_plan`].
///
/// For each `(caller_id, args)` invocation in the plan, stores the PK tuples
/// the fetcher produced. Phase 3's `scan_requirement` consults this map to
/// learn which rows of the row_table the invocation covers — the rows
/// themselves already live in the DB (upserted by Phase 0c).
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

// ── Phase-split types ─────────────────────────────────────────────────────

/// Phase 0a output: a single deduplicated caller invocation with all args
/// already resolved to concrete [`Value`]s. Ready for the fetcher call,
/// independent of any DB state.
#[derive(Debug, Clone)]
pub struct ResolvedInvocation {
    pub caller_id: String,
    pub row_table: String,
    pub resolved_args: Vec<Value>,
    /// Cached `(caller_id, arg_cells)` for use as `RequirementKey` without
    /// recomputing in phase 0c.
    pub key: RequirementKey,
}

/// Phase 0b output: the fetcher rows collected per invocation. Kept as a
/// plain `Vec` (not keyed) because `apply_fetched` consumes each entry
/// once and needs the associated `row_table` name for the upsert.
#[derive(Debug, Default)]
pub struct FetchedRequirements {
    pub entries: Vec<FetchedEntry>,
}

#[derive(Debug)]
pub struct FetchedEntry {
    pub key: RequirementKey,
    pub row_table: String,
    pub rows: Vec<Vec<CellValue>>,
}

// ── Public phase functions ────────────────────────────────────────────────

/// Phase 0a — sync. Walks the plan, resolves each caller invocation's args
/// against `plan.bound_values` + `params`, dedupes by `(caller_id, args)`.
pub fn collect_fetch_plan(
    plan: &ExecutionPlan,
    params: &Params,
) -> Result<Vec<ResolvedInvocation>, ExecuteError> {
    let mut out = Vec::new();
    let mut seen: HashSet<RequirementKey> = HashSet::new();

    for inv in collect_invocations(plan) {
        let resolved_args = resolve_args(&plan.bound_values, params, &inv)?;
        let arg_cells: Vec<CellValue> = resolved_args.iter().map(value_to_cell).collect();
        let key: RequirementKey = (inv.caller_id.clone(), arg_cells);

        if !seen.insert(key.clone()) {
            continue;
        }

        out.push(ResolvedInvocation {
            caller_id: inv.caller_id,
            row_table: inv.row_table,
            resolved_args,
            key,
        });
    }

    Ok(out)
}

/// Phase 0b — async. Invokes all fetchers **in parallel** via [`join_all`].
/// Takes no DB reference; safe to run without holding any `&mut` over an
/// `.await`. A missing fetcher or a fetcher error fails the whole set.
pub async fn fetch_requirements(
    invocations: &[ResolvedInvocation],
    fetchers: &FetcherRuntime,
) -> Result<FetchedRequirements, ExecuteError> {
    // Look up fetcher Arcs up-front so the async block below only holds
    // clones — it doesn't need to borrow `fetchers` across the await.
    let mut prepared: Vec<(ResolvedInvocation, AsyncFetcherFn)> =
        Vec::with_capacity(invocations.len());
    for inv in invocations {
        let fetcher = fetchers.get(&inv.caller_id).ok_or_else(|| {
            ExecuteError::CallerError(format!("fetcher `{}` not registered", inv.caller_id))
        })?;
        prepared.push((inv.clone(), fetcher.clone()));
    }

    let futs: Vec<_> = prepared
        .into_iter()
        .map(|(inv, fetcher)| async move {
            let rows = fetcher(inv.resolved_args.clone()).await.map_err(|e| {
                ExecuteError::CallerError(format!("fetcher `{}` failed: {e}", inv.caller_id))
            })?;
            Ok::<FetchedEntry, ExecuteError>(FetchedEntry {
                key: inv.key,
                row_table: inv.row_table,
                rows,
            })
        })
        .collect();

    let results = join_all(futs).await;
    let mut entries = Vec::with_capacity(results.len());
    for r in results {
        entries.push(r?);
    }
    Ok(FetchedRequirements { entries })
}

/// Phase 0c — sync. Upserts every fetched row into its `row_table` and
/// produces the `RequirementsResult` that Phase 3 reads.
pub fn apply_fetched(
    db: &mut HashMap<String, Table>,
    fetched: FetchedRequirements,
) -> Result<RequirementsResult, ExecuteError> {
    let mut result = RequirementsResult::default();

    for FetchedEntry { key, row_table, rows } in fetched.entries {
        let table = db.get_mut(&row_table).ok_or_else(|| {
            ExecuteError::TableNotFound(row_table.clone())
        })?;
        let pk_columns = table.schema.primary_key.clone();
        if pk_columns.is_empty() {
            return Err(ExecuteError::CallerError(format!(
                "fetcher `{}` row_table `{}` has no primary key",
                key.0, row_table,
            )));
        }
        let expected_col_count = table.schema.columns.len();

        let mut pk_tuples: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.into_iter().enumerate() {
            if row.len() != expected_col_count {
                return Err(ExecuteError::CallerError(format!(
                    "fetcher `{}` row {row_idx}: returned {} cells, row_table `{}` has {} columns",
                    key.0, row.len(), row_table, expected_col_count,
                )));
            }
            let pk: Vec<Value> = pk_columns.iter().map(|&c| cell_to_value(&row[c])).collect();
            table.upsert_by_pk(&row).map_err(|e| {
                ExecuteError::CallerError(format!(
                    "fetcher `{}` upsert into `{}` failed: {e}",
                    key.0, row_table,
                ))
            })?;
            pk_tuples.push(pk);
        }

        result.pk_sets.insert(key, pk_tuples);
    }

    Ok(result)
}

/// Convenience wrapper: runs 0a → 0b → 0c in sequence. Holds `&mut db`
/// across the fetcher awaits — use the split phases if you need to drop
/// the `&mut` during fetching (e.g. the wasm client does this to allow
/// parallel `query_async` calls).
pub async fn resolve_requirements(
    db: &mut HashMap<String, Table>,
    plan: &ExecutionPlan,
    params: &Params,
    fetchers: &FetcherRuntime,
) -> Result<RequirementsResult, ExecuteError> {
    let invocations = collect_fetch_plan(plan, params)?;
    let fetched = fetch_requirements(&invocations, fetchers).await?;
    apply_fetched(db, fetched)
}

// ── Internals ─────────────────────────────────────────────────────────────

/// One invocation site of a caller in the plan (pre-arg-resolution).
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

// ── Parallel joiner (minimal `join_all`) ──────────────────────────────────
//
// Polls every future each wake-up so all fetchers make progress concurrently.
// Independent of `Send`/`Sync` — inherits from the inner future, which is
// what the `FetcherFuture` cfg-split needs (wasm: !Send, native: Send).

enum JoinSlot<F: Future> {
    Pending(Pin<Box<F>>),
    Done(F::Output),
    Taken,
}

struct JoinAll<F: Future> {
    slots: Vec<JoinSlot<F>>,
}

// `Pin<Box<F>>` pins the inner future; the outer `JoinAll` itself never needs
// to stay pinned (we only mutate the `Vec`, not the futures in place). Marking
// it `Unpin` unlocks plain `&mut`-access via `Pin::get_mut`.
impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = Pin::into_inner(self);
        let mut all_done = true;
        for slot in this.slots.iter_mut() {
            if let JoinSlot::Pending(f) = slot {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => *slot = JoinSlot::Done(v),
                    Poll::Pending => all_done = false,
                }
            }
        }
        if !all_done {
            return Poll::Pending;
        }
        let out = this
            .slots
            .iter_mut()
            .map(|slot| match std::mem::replace(slot, JoinSlot::Taken) {
                JoinSlot::Done(v) => v,
                _ => unreachable!("every slot must be Done when all_done is true"),
            })
            .collect();
        Poll::Ready(out)
    }
}

fn join_all<F: Future>(futs: Vec<F>) -> JoinAll<F> {
    JoinAll {
        slots: futs.into_iter().map(|f| JoinSlot::Pending(Box::pin(f))).collect(),
    }
}
