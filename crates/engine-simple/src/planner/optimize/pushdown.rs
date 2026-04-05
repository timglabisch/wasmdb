//! Push single-source predicates from `plan.filter` into `source.pre_filter`.
//!
//! After this pass, predicates that reference only one table are evaluated
//! during the scan of that table (via `filter_batch`), reducing the number
//! of rows that enter joins and post-filters.

use crate::planner::plan::*;

use super::predicate_column_refs;

pub fn rewrite(plan: &mut PlanSelect) {
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
