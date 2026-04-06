//! Physical plan optimization: choose concrete scan methods and join strategies.
//!
//! Runs after logical optimizations (`or_to_in`, `pushdown`).
//! Converts the logical plan into a physical plan by selecting indexes
//! for scans and choosing join algorithms.

mod join_strategy;
mod scan_method;

use std::collections::HashMap;

use schema_engine::schema::{self, TableSchema};

use crate::planner::plan::*;

/// Populate `scan_method` and join `strategy` for each source in the plan.
/// When an index scan is chosen, `pre_filter` is narrowed to only the post_filter
/// predicates not covered by the index — the executor always applies `pre_filter`.
pub fn rewrite(plan: &mut PlanSelect, table_schemas: &HashMap<String, TableSchema>) {
    for source in &mut plan.sources {
        // If a previous run already split the predicates,
        // reconstruct the full pre_filter before re-choosing.
        let full_filter = reconstruct_full_filter(
            std::mem::replace(&mut source.scan_method, PlanScanMethod::Full),
            std::mem::replace(&mut source.pre_filter, PlanFilterPredicate::None),
        );
        source.pre_filter = full_filter;

        if let Some(ts) = table_schemas.get(&source.table) {
            let (method, post_filter) = scan_method::choose(&source.pre_filter, ts);
            source.scan_method = method;
            source.pre_filter = post_filter;
        }
    }
    for i in 1..plan.sources.len() {
        if let Some(ref join) = plan.sources[i].join {
            if let Some(ts) = table_schemas.get(&plan.sources[i].table) {
                let indexes = schema::effective_indexes(ts);
                let strategy = join_strategy::choose(&join.on, i, &indexes);
                plan.sources[i].join.as_mut().unwrap().strategy = strategy;
            }
        }
    }
}

/// Reconstruct the full predicate from a previous split.
/// Combines index_predicates (from scan_method) back with pre_filter (post_filter).
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
