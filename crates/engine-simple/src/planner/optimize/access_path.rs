//! Choose scan method and join strategy based on available indexes.
//!
//! Runs after `or_to_in` and `pushdown` — needs the final predicates.
//! Examines each source's `pre_filter` and the table's indexes to decide
//! whether to use an index scan, and checks join conditions for index lookup joins.

use std::collections::HashMap;

use schema_engine::schema::{self, IndexSchema, IndexType, TableSchema};

use crate::planner::plan::*;

/// Populate `scan_method` and join `strategy` for each source in the plan.
/// When an index scan is chosen, `pre_filter` is narrowed to only the residual
/// predicates not covered by the index — the executor always applies `pre_filter`.
pub fn rewrite(plan: &mut PlanSelect, table_schemas: &HashMap<String, TableSchema>) {
    for source in &mut plan.sources {
        // If a previous access_path run already split the predicates,
        // reconstruct the full pre_filter before re-choosing.
        let full_filter = reconstruct_full_filter(
            std::mem::replace(&mut source.scan_method, PlanScanMethod::Full),
            std::mem::replace(&mut source.pre_filter, PlanFilterPredicate::None),
        );
        source.pre_filter = full_filter;

        if let Some(ts) = table_schemas.get(&source.table) {
            let indexes = effective_indexes(ts);
            let (method, residual) = choose_scan_method(&source.pre_filter, &indexes);
            source.scan_method = method;
            source.pre_filter = residual;
        }
    }
    for i in 1..plan.sources.len() {
        if let Some(ref join) = plan.sources[i].join {
            if let Some(ts) = table_schemas.get(&plan.sources[i].table) {
                let indexes = effective_indexes(ts);
                let strategy = choose_join_strategy(&join.on, i, &indexes);
                plan.sources[i].join.as_mut().unwrap().strategy = strategy;
            }
        }
    }
}

/// Reconstruct the full predicate from a previous access_path split.
/// Combines index_predicates (from scan_method) back with pre_filter (residual).
fn reconstruct_full_filter(
    scan_method: PlanScanMethod,
    pre_filter: PlanFilterPredicate,
) -> PlanFilterPredicate {
    let index_preds = match scan_method {
        PlanScanMethod::Index { index_predicates, .. } => index_predicates,
        PlanScanMethod::Full => return pre_filter,
    };

    let mut all: Vec<PlanFilterPredicate> = index_preds;
    if !matches!(pre_filter, PlanFilterPredicate::None) {
        all.push(pre_filter);
    }

    match all.len() {
        0 => PlanFilterPredicate::None,
        _ => all.into_iter()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap(),
    }
}

// ── Effective indexes (includes auto-PK-Hash) ────────────────────────────

fn effective_indexes(ts: &TableSchema) -> Vec<IndexSchema> {
    schema::effective_indexes(ts)
}

// ── Predicate classification ─────────────────────────────────────────────

fn flatten_ands(pred: &PlanFilterPredicate) -> Vec<&PlanFilterPredicate> {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            let mut leaves = flatten_ands(l);
            leaves.extend(flatten_ands(r));
            leaves
        }
        other => vec![other],
    }
}

enum PredClass {
    Eq,
    Range,
    In,
    Other,
}

fn classify_pred(pred: &PlanFilterPredicate) -> PredClass {
    match pred {
        PlanFilterPredicate::Equals { .. } => PredClass::Eq,
        PlanFilterPredicate::GreaterThan { .. }
        | PlanFilterPredicate::GreaterThanOrEqual { .. }
        | PlanFilterPredicate::LessThan { .. }
        | PlanFilterPredicate::LessThanOrEqual { .. } => PredClass::Range,
        PlanFilterPredicate::In { .. } => PredClass::In,
        _ => PredClass::Other,
    }
}

fn leaf_column(pred: &PlanFilterPredicate) -> Option<usize> {
    match pred {
        PlanFilterPredicate::Equals { col, .. }
        | PlanFilterPredicate::GreaterThan { col, .. }
        | PlanFilterPredicate::GreaterThanOrEqual { col, .. }
        | PlanFilterPredicate::LessThan { col, .. }
        | PlanFilterPredicate::LessThanOrEqual { col, .. }
        | PlanFilterPredicate::In { col, .. } => Some(col.col),
        _ => None,
    }
}

// ── Scan method selection ────────────────────────────────────────────────

/// Returns (scan_method, new_pre_filter). When Index is chosen, the returned
/// pre_filter contains only the residual predicates not covered by the index.
fn choose_scan_method(
    pre_filter: &PlanFilterPredicate,
    indexes: &[IndexSchema],
) -> (PlanScanMethod, PlanFilterPredicate) {
    if matches!(pre_filter, PlanFilterPredicate::None) {
        return (PlanScanMethod::Full, PlanFilterPredicate::None);
    }

    let leaves = flatten_ands(pre_filter);

    let mut seen_cols = Vec::new();
    let mut indexable: Vec<(usize, usize, &PlanFilterPredicate)> = Vec::new();
    for (li, leaf) in leaves.iter().enumerate() {
        if let Some(col) = leaf_column(leaf) {
            if !seen_cols.contains(&col) {
                seen_cols.push(col);
                indexable.push((li, col, leaf));
            }
        }
    }

    if indexable.is_empty() {
        return (PlanScanMethod::Full, pre_filter.clone());
    }

    let mut best_score: (usize, u8) = (0, 0);
    let mut best_used: Vec<usize> = Vec::new();
    let mut best_index_columns: Vec<usize> = Vec::new();
    let mut best_prefix_len: usize = 0;
    let mut best_is_hash: bool = false;
    let mut best_can_use: bool = false;

    for idx in indexes {
        let idx_cols = &idx.columns;
        let mut prefix_eq_count: usize = 0;
        let mut has_range_on_last = false;
        let mut has_in_on_last = false;
        let mut used_leaves: Vec<usize> = Vec::new();

        for &col in idx_cols {
            if let Some(&(li, _, pred)) = indexable.iter().find(|(_, c, _)| *c == col) {
                match classify_pred(pred) {
                    PredClass::Eq => {
                        prefix_eq_count += 1;
                        used_leaves.push(li);
                    }
                    PredClass::Range => {
                        has_range_on_last = true;
                        used_leaves.push(li);
                        break;
                    }
                    PredClass::In => {
                        has_in_on_last = true;
                        used_leaves.push(li);
                        break;
                    }
                    PredClass::Other => break,
                }
            } else {
                break;
            }
        }

        let prefix_len = used_leaves.len();
        if prefix_len == 0 { continue; }

        let is_full_key_eq = !has_range_on_last && !has_in_on_last && prefix_eq_count == idx_cols.len();
        let is_hash = idx.index_type == IndexType::Hash;
        let tie_break = if is_full_key_eq && is_hash { 2 } else if is_full_key_eq { 1 } else { 0 };
        let score = (prefix_len, tie_break);
        if score <= best_score { continue; }

        // Check if this index type can actually handle the request.
        // Hash indexes can only do full-key eq lookups, not prefix or range.
        let can_use = if is_hash {
            is_full_key_eq || (has_in_on_last && prefix_eq_count + 1 == idx_cols.len())
        } else {
            true // BTree can handle any prefix/range combo
        };

        if !can_use { continue; }

        best_score = score;
        best_used = used_leaves;
        best_index_columns = idx_cols.clone();
        best_prefix_len = prefix_len;
        best_is_hash = is_hash;
        best_can_use = true;
    }

    if !best_can_use {
        return (PlanScanMethod::Full, pre_filter.clone());
    }

    // Build index_predicates (used leaves) and residual (unused leaves).
    let index_predicates: Vec<PlanFilterPredicate> = best_used.iter()
        .map(|&li| (*leaves[li]).clone())
        .collect();

    let remaining: Vec<&PlanFilterPredicate> = leaves.iter().enumerate()
        .filter(|(li, _)| !best_used.contains(li))
        .map(|(_, leaf)| *leaf)
        .collect();

    let residual = match remaining.len() {
        0 => PlanFilterPredicate::None,
        _ => remaining.into_iter().cloned()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap(),
    };

    let method = PlanScanMethod::Index {
        index_columns: best_index_columns,
        prefix_len: best_prefix_len,
        is_hash: best_is_hash,
        index_predicates,
    };
    (method, residual)
}

// ── Join strategy selection ──────────────────────────────────────────────

fn choose_join_strategy(
    on: &PlanFilterPredicate,
    right_source: usize,
    right_indexes: &[IndexSchema],
) -> PlanJoinStrategy {
    if let PlanFilterPredicate::ColumnEquals { left, right } = on {
        let (left_col, right_col) = if right.source == right_source {
            (*left, right.col)
        } else if left.source == right_source {
            (*right, left.col)
        } else {
            return PlanJoinStrategy::NestedLoop;
        };

        for idx in right_indexes {
            if idx.columns.first() == Some(&right_col) {
                return PlanJoinStrategy::IndexLookup {
                    left_col,
                    right_col,
                    index_columns: idx.columns.clone(),
                    is_hash: idx.index_type == IndexType::Hash,
                };
            }
        }
    }
    PlanJoinStrategy::NestedLoop
}
