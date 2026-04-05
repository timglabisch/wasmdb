//! Optimization passes on a built PlanSelect.
//!
//! Applied after AST translation, before execution.
//! Each pass transforms the plan in-place.

use query_engine::ast;

use super::plan::*;

/// Run all optimization passes on a plan.
pub fn run(plan: &mut PlanSelect) {
    rewrite_or_to_in(plan);
    pushdown_filters(plan);
}

// ── OR → IN rewrite ──────────────────────────────────────────────────────

/// Rewrite `col = A OR col = B` into `col IN (A, B)` across the plan.
fn rewrite_or_to_in(plan: &mut PlanSelect) {
    plan.filter = optimize_or_to_in(std::mem::replace(&mut plan.filter, PlanFilterPredicate::None));
    for source in &mut plan.sources {
        if let Some(ref mut join) = source.join {
            join.on = optimize_or_to_in(std::mem::replace(&mut join.on, PlanFilterPredicate::None));
        }
    }
}

fn optimize_or_to_in(pred: PlanFilterPredicate) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::Or(l, r) => {
            let l = optimize_or_to_in(*l);
            let r = optimize_or_to_in(*r);
            let or_pred = PlanFilterPredicate::Or(Box::new(l), Box::new(r));
            try_merge_or_to_in(&or_pred).unwrap_or(or_pred)
        }
        PlanFilterPredicate::And(l, r) => {
            PlanFilterPredicate::And(
                Box::new(optimize_or_to_in(*l)),
                Box::new(optimize_or_to_in(*r)),
            )
        }
        other => other,
    }
}

/// If the predicate is an OR-chain where every leaf is `Equals` or `In` on the
/// same column, merge into a single `In`.
fn try_merge_or_to_in(pred: &PlanFilterPredicate) -> Option<PlanFilterPredicate> {
    let mut col: Option<ColumnRef> = None;
    let mut values = Vec::new();
    if collect_or_eq_values(pred, &mut col, &mut values) {
        Some(PlanFilterPredicate::In { col: col?, values })
    } else {
        None
    }
}

fn collect_or_eq_values(
    pred: &PlanFilterPredicate,
    col: &mut Option<ColumnRef>,
    values: &mut Vec<ast::Value>,
) -> bool {
    match pred {
        PlanFilterPredicate::Equals { col: c, value } => {
            if col.map_or(true, |existing| existing == *c) {
                *col = Some(*c);
                values.push(value.clone());
                true
            } else {
                false
            }
        }
        PlanFilterPredicate::In { col: c, values: vs } => {
            if col.map_or(true, |existing| existing == *c) {
                *col = Some(*c);
                values.extend(vs.iter().cloned());
                true
            } else {
                false
            }
        }
        PlanFilterPredicate::Or(l, r) => {
            collect_or_eq_values(l, col, values) && collect_or_eq_values(r, col, values)
        }
        _ => false,
    }
}

// ── Filter pushdown ──────────────────────────────────────────────────────

/// Push single-source predicates from `plan.filter` into `source.pre_filter`.
fn pushdown_filters(plan: &mut PlanSelect) {
    let filter = std::mem::replace(&mut plan.filter, PlanFilterPredicate::None);
    let conjuncts = flatten_and_conjuncts(filter);

    let mut remaining = Vec::new();
    for conjunct in conjuncts {
        let refs = predicate_column_refs(&conjunct);
        let first_source = refs.first().map(|r| r.source);
        let single_source = first_source.filter(|&s| refs.iter().all(|r| r.source == s));

        match single_source {
            Some(src_idx) => {
                let existing = std::mem::replace(
                    &mut plan.sources[src_idx].pre_filter,
                    PlanFilterPredicate::None,
                );
                plan.sources[src_idx].pre_filter = match existing {
                    PlanFilterPredicate::None => conjunct,
                    other => PlanFilterPredicate::And(Box::new(other), Box::new(conjunct)),
                };
            }
            None => remaining.push(conjunct),
        }
    }

    plan.filter = match remaining.len() {
        0 => PlanFilterPredicate::None,
        _ => remaining
            .into_iter()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap(),
    };
}

fn flatten_and_conjuncts(pred: PlanFilterPredicate) -> Vec<PlanFilterPredicate> {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            let mut out = flatten_and_conjuncts(*l);
            out.extend(flatten_and_conjuncts(*r));
            out
        }
        PlanFilterPredicate::None => vec![],
        other => vec![other],
    }
}

// ── Predicate utilities ──────────────────────────────────────────────────

pub fn predicate_column_refs(pred: &PlanFilterPredicate) -> Vec<ColumnRef> {
    match pred {
        PlanFilterPredicate::Equals { col, .. }
        | PlanFilterPredicate::NotEquals { col, .. }
        | PlanFilterPredicate::GreaterThan { col, .. }
        | PlanFilterPredicate::GreaterThanOrEqual { col, .. }
        | PlanFilterPredicate::LessThan { col, .. }
        | PlanFilterPredicate::LessThanOrEqual { col, .. }
        | PlanFilterPredicate::IsNull { col }
        | PlanFilterPredicate::IsNotNull { col } => vec![*col],

        PlanFilterPredicate::ColumnEquals { left, right }
        | PlanFilterPredicate::ColumnNotEquals { left, right }
        | PlanFilterPredicate::ColumnGreaterThan { left, right }
        | PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right }
        | PlanFilterPredicate::ColumnLessThan { left, right }
        | PlanFilterPredicate::ColumnLessThanOrEqual { left, right } => {
            vec![*left, *right]
        }

        PlanFilterPredicate::In { col, .. }
        | PlanFilterPredicate::InMaterialized { col, .. }
        | PlanFilterPredicate::CompareMaterialized { col, .. } => vec![*col],

        PlanFilterPredicate::And(l, r) | PlanFilterPredicate::Or(l, r) => {
            let mut v = predicate_column_refs(l);
            v.extend(predicate_column_refs(r));
            v
        }
        PlanFilterPredicate::None => vec![],
    }
}
