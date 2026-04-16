//! Reactive condition extraction from the AST.
//!
//! Walks REACTIVE() expressions in SELECT columns and WHERE clause,
//! producing logical `ReactiveCondition`s with the full predicate.

use sql_parser::ast;

use crate::planner::plan::{PlanSelect, PlanSourceEntry};
use crate::planner::translate;
use crate::planner::PlanError;
use super::plan::{ReactiveCondition, ReactiveConditionKind};

/// Extract reactive conditions from REACTIVE expressions in result columns and WHERE clause.
pub fn extract_reactive_conditions(
    select: &ast::AstSelect,
    plan: &PlanSelect,
) -> Result<Vec<ReactiveCondition>, PlanError> {
    let mut conditions = Vec::new();

    // 1. From SELECT result columns
    for rc in &select.result_columns {
        if let ast::AstExpr::Reactive(inner) = &rc.expr {
            conditions.push(decompose_condition(inner, &plan.sources)?);
        }
    }

    // 2. From WHERE clause (walk the filter expressions for reactive() calls)
    for expr in &select.filter {
        extract_reactive_from_expr(expr, &plan.sources, &mut conditions)?;
    }

    Ok(conditions)
}

/// Recursively walk an expression tree to find reactive() calls.
fn extract_reactive_from_expr(
    expr: &ast::AstExpr,
    sources: &[PlanSourceEntry],
    conditions: &mut Vec<ReactiveCondition>,
) -> Result<(), PlanError> {
    match expr {
        ast::AstExpr::Reactive(inner) => {
            conditions.push(decompose_condition(inner, sources)?);
        }
        ast::AstExpr::Binary { left, right, .. } => {
            extract_reactive_from_expr(left, sources, conditions)?;
            extract_reactive_from_expr(right, sources, conditions)?;
        }
        _ => {}
    }
    Ok(())
}

/// Decompose a REACTIVE inner expression into a ReactiveCondition.
///
/// Phase 1 only: produces logical conditions with the full predicate.
/// No lookup key extraction — that is done by the optimizer (Phase 2).
///
/// If the inner expression is a plain column reference → TableLevel.
/// Otherwise → Condition with the full predicate.
fn decompose_condition(
    expr: &ast::AstExpr,
    sources: &[PlanSourceEntry],
) -> Result<ReactiveCondition, PlanError> {
    // Plain column reference → table-level
    if let ast::AstExpr::Column(_) = expr {
        let cr = translate::resolve_to_column_ref(expr, sources)?;
        return Ok(ReactiveCondition {
            table: sources[cr.source].table.clone(),
            kind: ReactiveConditionKind::TableLevel,
            source_idx: cr.source,
        });
    }

    // Expression with conditions → Condition with full predicate
    let dummy_schemas = std::collections::HashMap::new();
    let query_schemas = sources
        .iter()
        .map(|s| (s.table.clone(), s.schema.clone()))
        .collect();
    let mut ctx = crate::planner::PlanContext {
        table_schemas: &dummy_schemas,
        query_schemas,
        materializations: Vec::new(),
    };

    let predicate = translate::plan_expr_to_predicate(expr, sources, &mut ctx)?;

    // Determine the table from column refs in the predicate
    let col_refs = predicate.column_refs();
    if col_refs.is_empty() {
        return Err(PlanError::UnsupportedExpr(
            "REACTIVE condition must reference at least one column".into(),
        ));
    }
    let source_idx = col_refs[0].source;

    Ok(ReactiveCondition {
        table: sources[source_idx].table.clone(),
        kind: ReactiveConditionKind::Condition {
            filter: predicate,
        },
        source_idx,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;
    use sql_parser::schema::{ColumnDef, Schema};
    use crate::planner::plan::{PlanFilterPredicate, PlanScanMethod};

    fn users_sources() -> Vec<PlanSourceEntry> {
        vec![PlanSourceEntry {
            table: "users".into(),
            schema: Schema::new(vec![
                ColumnDef { table: Some("users".into()), name: "id".into() },
                ColumnDef { table: Some("users".into()), name: "name".into() },
                ColumnDef { table: Some("users".into()), name: "age".into() },
            ]),
            join: None,
            pre_filter: PlanFilterPredicate::None,
            scan_method: PlanScanMethod::Full,
        }]
    }

    #[test]
    fn test_decompose_single_equality() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "id".into() })),
            op: ast::Operator::Eq,
            right: Box::new(ast::AstExpr::Literal(Value::Placeholder("uid".into()))),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.table, "users");
        match &cond.kind {
            ReactiveConditionKind::Condition { filter } => {
                assert!(matches!(filter, PlanFilterPredicate::Equals { .. }));
            }
            _ => panic!("expected Condition"),
        }
    }

    #[test]
    fn test_decompose_column_ref_table_level() {
        let sources = users_sources();
        let expr = ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "id".into() });
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.table, "users");
        assert!(matches!(cond.kind, ReactiveConditionKind::TableLevel));
    }

    #[test]
    fn test_decompose_non_equality_produces_condition() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "age".into() })),
            op: ast::Operator::Gt,
            right: Box::new(ast::AstExpr::Literal(Value::Int(18))),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.table, "users");
        match &cond.kind {
            ReactiveConditionKind::Condition { filter } => {
                assert!(matches!(filter, PlanFilterPredicate::GreaterThan { .. }));
            }
            _ => panic!("expected Condition with filter, not TableLevel"),
        }
    }
}
