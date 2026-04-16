//! Rewrite `col = A OR col = B` into `col IN (A, B)` so index scans can be used.
//!
//! Works recursively on nested OR chains and merges existing IN predicates.
//! Only merges when ALL branches of the OR are equality/IN on the same column.

use sql_parser::ast;

use crate::planner::plan::*;

pub fn rewrite(plan: &mut PlanSelect) {
    plan.filter = normalize(std::mem::replace(&mut plan.filter, PlanFilterPredicate::None));
    for source in &mut plan.sources {
        source.pre_filter = normalize(std::mem::replace(&mut source.pre_filter, PlanFilterPredicate::None));
        if let Some(ref mut join) = source.join {
            join.on = normalize(std::mem::replace(&mut join.on, PlanFilterPredicate::None));
        }
    }
}

/// Recursively rewrite OR-chains of equalities/INs on the same column into a
/// single `In`. Exposed so the reactive optimizer can apply the same
/// normalization to REACTIVE() predicates.
pub(crate) fn normalize(pred: PlanFilterPredicate) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::Or(l, r) => {
            let l = normalize(*l);
            let r = normalize(*r);
            let or_pred = PlanFilterPredicate::Or(Box::new(l), Box::new(r));
            try_merge(&or_pred).unwrap_or(or_pred)
        }
        PlanFilterPredicate::And(l, r) => {
            PlanFilterPredicate::And(
                Box::new(normalize(*l)),
                Box::new(normalize(*r)),
            )
        }
        other => other,
    }
}

/// If the predicate is an OR-chain where every leaf is `Equals` or `In` on the
/// same column, merge into a single `In`.
fn try_merge(pred: &PlanFilterPredicate) -> Option<PlanFilterPredicate> {
    let mut col: Option<ColumnRef> = None;
    let mut values = Vec::new();
    if collect(pred, &mut col, &mut values) {
        Some(PlanFilterPredicate::In { col: col?, values })
    } else {
        None
    }
}

fn collect(
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
            collect(l, col, values) && collect(r, col, values)
        }
        _ => false,
    }
}
