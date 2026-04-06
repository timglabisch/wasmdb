//! Choose which index (if any) to use for a table scan.
//!
//! Examines the source's `pre_filter` predicates against available indexes,
//! scores each index by prefix length and type, and returns the best choice.

use schema_engine::schema::{IndexSchema, IndexType};

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
        if let Some(col) = leaf_column(pred) {
            if !seen_cols.contains(&col) {
                seen_cols.push(col);
                out.push(IndexablePredicate { pred_idx: i, col, pred });
            }
        }
    }
    out
}

fn build_residual(preds: &[&PlanFilterPredicate], used: &[usize]) -> PlanFilterPredicate {
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

/// Primary: more matched columns is better. Secondary: Hash full-key eq wins ties.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexScore {
    prefix_len: usize,
    tie_break: u8, // 2=Hash full-key, 1=BTree full-key, 0=partial
}

struct IndexCandidate {
    score: IndexScore,
    used_preds: Vec<usize>,
    index_columns: Vec<usize>,
    is_hash: bool,
}

fn score_index(
    idx: &IndexSchema,
    indexable: &[IndexablePredicate],
) -> Option<IndexCandidate> {
    let mut prefix_eq_count: usize = 0;
    let mut has_range = false;
    let mut has_in = false;
    let mut used_preds: Vec<usize> = Vec::new();

    for &col in &idx.columns {
        let Some(leaf) = indexable.iter().find(|l| l.col == col) else {
            break;
        };
        match classify(leaf.pred) {
            PredClass::Eq => {
                prefix_eq_count += 1;
                used_preds.push(leaf.pred_idx);
            }
            PredClass::Range => {
                has_range = true;
                used_preds.push(leaf.pred_idx);
                break;
            }
            PredClass::In => {
                has_in = true;
                used_preds.push(leaf.pred_idx);
                break;
            }
            PredClass::Other => break,
        }
    }

    if used_preds.is_empty() {
        return None;
    }

    let is_full_key_eq = !has_range && !has_in && prefix_eq_count == idx.columns.len();
    let is_hash = idx.index_type == IndexType::Hash;

    // Hash indexes can only do full-key eq or full-key IN.
    if is_hash && !is_full_key_eq && !(has_in && prefix_eq_count + 1 == idx.columns.len()) {
        return None;
    }

    let tie_break = match (is_full_key_eq, is_hash) {
        (true, true) => 2,  // Hash full-key eq — best
        (true, false) => 1, // BTree full-key eq
        _ => 0,
    };

    Some(IndexCandidate {
        score: IndexScore { prefix_len: used_preds.len(), tie_break },
        used_preds,
        index_columns: idx.columns.clone(),
        is_hash,
    })
}

// ── Public API ───────────────────────────────────────────────────────────

/// Choose the best scan method for a source. Returns `(scan_method, new_pre_filter)`.
/// When an index is chosen, `new_pre_filter` contains only the residual predicates.
pub fn choose(
    pre_filter: &PlanFilterPredicate,
    indexes: &[IndexSchema],
) -> (PlanScanMethod, PlanFilterPredicate) {
    if matches!(pre_filter, PlanFilterPredicate::None) || indexes.is_empty() {
        return (PlanScanMethod::Full, pre_filter.clone());
    }

    let preds = flatten_ands(pre_filter);
    let indexable = indexable_predicates(&preds);
    if indexable.is_empty() {
        return (PlanScanMethod::Full, pre_filter.clone());
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
    let residual = build_residual(&preds, &best.used_preds);

    let method = PlanScanMethod::Index {
        index_columns: best.index_columns,
        prefix_len: best.score.prefix_len,
        is_hash: best.is_hash,
        index_predicates,
    };
    (method, residual)
}
