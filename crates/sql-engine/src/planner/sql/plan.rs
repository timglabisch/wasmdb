//! SQL-specific plan types: execution plan, materialization, pretty-printing.

use std::collections::HashMap;

use sql_parser::ast::Value;

use crate::planner::shared::plan::PlanSelect;

/// Top-level execution plan: materialization steps + main query.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Materialization steps in bottom-up order (inner-most subquery first).
    pub materializations: Vec<MaterializeStep>,
    /// Main query — may contain InMaterialized/CompareMaterialized predicates.
    pub main: PlanSelect,
    /// Values bound to internal placeholders (auto-platzhalterisierte Literale
    /// aus Caller-Args). Flow from `PlanContext.bound_values` through the
    /// planner to the executor, where `RequirementArg::Placeholder`-Namen
    /// gegen diese Map aufgelöst werden.
    pub bound_values: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct MaterializeStep {
    pub plan: PlanSelect,
    pub kind: MaterializeKind,
}

#[derive(Debug, Clone, Copy)]
pub enum MaterializeKind {
    /// 1 column, 1 row — scalar value for comparison.
    Scalar,
    /// 1 column, N rows — value list for IN.
    List,
}

// ── Pretty printer ───────────────────────────────────────────────────────

impl ExecutionPlan {
    pub fn pretty_print(&self) -> String {
        let mut out = String::new();
        for (i, mat) in self.materializations.iter().enumerate() {
            let kind = match mat.kind {
                MaterializeKind::Scalar => "Scalar",
                MaterializeKind::List => "List",
            };
            out.push_str(&format!("Materialize step={i} kind={kind}\n"));
            mat.plan.pretty_print_to(&mut out, 1);
        }
        out.push_str("Select\n");
        self.main.pretty_print_to(&mut out, 1);
        out
    }
}
