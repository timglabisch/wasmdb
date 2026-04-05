//! Rewrite `col = A OR col = B` into `col IN (A, B)` so index scans can be used.
//!
//! Works recursively on nested OR chains and merges existing IN predicates.
//! Only merges when ALL branches of the OR are equality/IN on the same column.

use query_engine::ast;

use crate::planner::plan::*;

pub fn rewrite(plan: &mut PlanSelect) {
    plan.filter = optimize(std::mem::replace(&mut plan.filter, PlanFilterPredicate::None));
    for source in &mut plan.sources {
        if let Some(ref mut join) = source.join {
            join.on = optimize(std::mem::replace(&mut join.on, PlanFilterPredicate::None));
        }
    }
}

fn optimize(pred: PlanFilterPredicate) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::Or(l, r) => {
            let l = optimize(*l);
            let r = optimize(*r);
            let or_pred = PlanFilterPredicate::Or(Box::new(l), Box::new(r));
            try_merge(&or_pred).unwrap_or(or_pred)
        }
        PlanFilterPredicate::And(l, r) => {
            PlanFilterPredicate::And(
                Box::new(optimize(*l)),
                Box::new(optimize(*r)),
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
