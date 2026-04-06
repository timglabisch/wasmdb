//! Choose which index (if any) to use for a table scan.
//!
//! Examines the source's `pre_filter` predicates against available indexes,
//! scores each index by prefix length and type, and returns the best choice.

use schema_engine::schema::{self, IndexSchema, IndexType, TableSchema};

use crate::planner::plan::*;

// ── Predicate helpers ────────────────────────────────────────────────────

enum PredClass {
    Eq,
    Range,
    In,
    Other,
}

fn classify(pred: &PlanFilterPredicate) -> PredClass {
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

fn pred_column(pred: &PlanFilterPredicate) -> Option<usize> {
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

fn flatten_ands(pred: &PlanFilterPredicate) -> Vec<&PlanFilterPredicate> {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            let mut out = flatten_ands(l);
            out.extend(flatten_ands(r));
            out
        }
        other => vec![other],
    }
}

struct IndexablePredicate<'a> {
    pred_idx: usize,
    col: usize,
    pred: &'a PlanFilterPredicate,
}

/// Extract indexable predicates, deduplicated by column.
fn indexable_predicates<'a>(preds: &[&'a PlanFilterPredicate]) -> Vec<IndexablePredicate<'a>> {
    let mut seen_cols = Vec::new();
    let mut out = Vec::new();
    for (i, &pred) in preds.iter().enumerate() {
        if let Some(col) = pred_column(pred) {
            if !seen_cols.contains(&col) {
                seen_cols.push(col);
                out.push(IndexablePredicate { pred_idx: i, col, pred });
            }
        }
    }
    out
}

fn build_post_filter(preds: &[&PlanFilterPredicate], used: &[usize]) -> PlanFilterPredicate {
    let remaining: Vec<PlanFilterPredicate> = preds.iter().enumerate()
        .filter(|(i, _)| !used.contains(i))
        .map(|(_, p)| (*p).clone())
        .collect();

    match remaining.len() {
        0 => PlanFilterPredicate::None,
        _ => remaining.into_iter()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap(),
    }
}

// ── Index candidate scoring ──────────────────────────────────────────────

/// Lookup complexity, ordered worst → best for use in `max_by_key`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LookupCost {
    /// O(n) — partial prefix, needs scan within index range.
    LogN,
    /// O(log n) — full-key BTree lookup.
    LogNFullKey,
    /// O(1) — full-key Hash lookup.
    Constant,
}

/// Ranks index candidates. Higher is better (used with `max_by_key`).
/// Primary: how many predicates the index covers. Secondary: lookup cost.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexScore {
    matched_predicates: usize,
    cost: LookupCost,
}

struct IndexCandidate {
    score: IndexScore,
    used_preds: Vec<usize>,
    index_columns: Vec<usize>,
    is_hash: bool,
}

/// Result of matching predicates against an index's column prefix.
struct PrefixMatch {
    eq_count: usize,
    has_range: bool,
    has_in: bool,
    used_preds: Vec<usize>,
    num_index_columns: usize,
}

impl PrefixMatch {
    fn is_full_key_eq(&self) -> bool {
        !self.has_range && !self.has_in && self.eq_count == self.num_index_columns
    }
}

/// Match predicates to index columns in order. Shared across index types.
fn match_prefix(
    idx_columns: &[usize],
    indexable: &[IndexablePredicate],
) -> Option<PrefixMatch> {
    let mut eq_count: usize = 0;
    let mut has_range = false;
    let mut has_in = false;
    let mut used_preds: Vec<usize> = Vec::new();

    for &col in idx_columns {
        let Some(entry) = indexable.iter().find(|e| e.col == col) else {
            break;
        };
        match classify(entry.pred) {
            PredClass::Eq => {
                eq_count += 1;
                used_preds.push(entry.pred_idx);
            }
            PredClass::Range => {
                has_range = true;
                used_preds.push(entry.pred_idx);
                break;
            }
            PredClass::In => {
                has_in = true;
                used_preds.push(entry.pred_idx);
                break;
            }
            PredClass::Other => break,
        }
    }

    if used_preds.is_empty() {
        return None;
    }

    Some(PrefixMatch {
        eq_count,
        has_range,
        has_in,
        used_preds,
        num_index_columns: idx_columns.len(),
    })
}

fn score_btree(idx: &IndexSchema, m: PrefixMatch) -> Option<IndexCandidate> {
    let cost = if m.is_full_key_eq() { LookupCost::LogNFullKey } else { LookupCost::LogN };
    Some(IndexCandidate {
        score: IndexScore { matched_predicates: m.used_preds.len(), cost },
        used_preds: m.used_preds,
        index_columns: idx.columns.clone(),
        is_hash: false,
    })
}

fn score_hash(idx: &IndexSchema, m: PrefixMatch) -> Option<IndexCandidate> {
    // Hash can only do full-key eq or full-key eq+IN on last column.
    let full_key_in = m.has_in && m.eq_count + 1 == m.num_index_columns;
    if !m.is_full_key_eq() && !full_key_in {
        return None;
    }
    let cost = if m.is_full_key_eq() { LookupCost::Constant } else { LookupCost::Constant };
    Some(IndexCandidate {
        score: IndexScore { matched_predicates: m.used_preds.len(), cost },
        used_preds: m.used_preds,
        index_columns: idx.columns.clone(),
        is_hash: true,
    })
}

fn score_index(
    idx: &IndexSchema,
    indexable: &[IndexablePredicate],
) -> Option<IndexCandidate> {
    let m = match_prefix(&idx.columns, indexable)?;
    match idx.index_type {
        IndexType::BTree => score_btree(idx, m),
        IndexType::Hash => score_hash(idx, m),
    }
}

// ── Public API ───────────────────────────────────────────────────────────

/// Choose the best scan method for a source. Returns `(scan_method, new_pre_filter)`.
/// When an index is chosen, `new_pre_filter` contains only the post_filter predicates.
pub fn choose(
    pre_filter: &PlanFilterPredicate,
    ts: &TableSchema,
) -> (PlanScanMethod, PlanFilterPredicate) {
    let indexes = schema::effective_indexes(ts);
    if matches!(pre_filter, PlanFilterPredicate::None) || indexes.is_empty() {
        return (PlanScanMethod::Full, pre_filter.clone());
    }

    let preds = flatten_ands(pre_filter);
    let indexable = indexable_predicates(&preds);
    if indexable.is_empty() {
        return (PlanScanMethod::Full, pre_filter.clone());
    }

    // Fast path: PK equality covers all indexable predicates — skip scoring.
    if let Some(result) = try_pk_lookup(&preds, &indexable, &indexes, &ts.primary_key) {
        return result;
    }

    let Some(best) = indexes.iter()
        .filter_map(|idx| score_index(idx, &indexable))
        .max_by_key(|c| c.score)
    else {
        return (PlanScanMethod::Full, pre_filter.clone());
    };

    let index_predicates: Vec<PlanFilterPredicate> = best.used_preds.iter()
        .map(|&i| (*preds[i]).clone())
        .collect();
    let post_filter = build_post_filter(&preds, &best.used_preds);

    let method = PlanScanMethod::Index {
        index_columns: best.index_columns,
        prefix_len: best.score.matched_predicates,
        is_hash: best.is_hash,
        index_predicates,
    };
    (method, post_filter)
}

/// If all indexable predicates are Eq on PK columns, skip scoring.
/// This is the best possible scan — no other index can beat a full PK hit.
/// Only applies when there are no extra indexable predicates that a composite
/// index could additionally cover.
fn try_pk_lookup(
    preds: &[&PlanFilterPredicate],
    indexable: &[IndexablePredicate],
    indexes: &[IndexSchema],
    primary_key: &[usize],
) -> Option<(PlanScanMethod, PlanFilterPredicate)> {
    if primary_key.is_empty() {
        return None;
    }

    // Every PK column must have an Eq predicate.
    let mut pk_pred_indices = Vec::new();
    for &pk_col in primary_key {
        let entry = indexable.iter().find(|e| e.col == pk_col)?;
        if !matches!(classify(entry.pred), PredClass::Eq) {
            return None;
        }
        pk_pred_indices.push(entry.pred_idx);
    }

    // Only use fast-path when PK covers ALL indexable predicates.
    // If there are extra indexable columns, a composite index might be better.
    if pk_pred_indices.len() != indexable.len() {
        return None;
    }

    // Find the index that covers exactly the PK (prefer Hash).
    let idx = indexes.iter()
        .filter(|idx| idx.columns == primary_key)
        .max_by_key(|idx| if idx.index_type == IndexType::Hash { 1u8 } else { 0 })?;

    let index_predicates: Vec<PlanFilterPredicate> = pk_pred_indices.iter()
        .map(|&i| (*preds[i]).clone())
        .collect();
    let post_filter = build_post_filter(preds, &pk_pred_indices);

    let method = PlanScanMethod::Index {
        index_columns: idx.columns.clone(),
        prefix_len: primary_key.len(),
        is_hash: idx.index_type == IndexType::Hash,
        index_predicates,
    };
    Some((method, post_filter))
}
