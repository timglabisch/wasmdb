//! Choose which index (if any) to use for a table scan.
//!
//! Examines the source's `pre_filter` predicates against available indexes,
//! scores each index by prefix length and type, and returns the best choice.

use schema_engine::schema::{IndexSchema, IndexType};

use crate::planner::plan::*;

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
pub fn choose(
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

        // Hash indexes can only do full-key eq lookups, not prefix or range.
        let can_use = if is_hash {
            is_full_key_eq || (has_in_on_last && prefix_eq_count + 1 == idx_cols.len())
        } else {
            true
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
