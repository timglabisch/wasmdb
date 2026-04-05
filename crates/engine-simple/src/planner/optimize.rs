//! Optimization passes on a built PlanSelect.
//!
//! Applied after AST translation, before execution.
//! Each pass is in its own submodule. New passes are added here.

mod or_to_in;
mod pushdown;

use super::plan::*;

/// Run all optimization passes on a plan (order matters).
pub fn run(plan: &mut PlanSelect) {
    or_to_in::rewrite(plan);
    pushdown::rewrite(plan);
}

// ── Predicate utilities (shared across passes) ───────────────────────────

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
