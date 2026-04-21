//! Reactive condition extraction from the AST.
//!
//! Walks REACTIVE() expressions in SELECT columns and WHERE clause,
//! producing logical `ReactiveCondition`s with the full predicate.

use sql_parser::ast;

use crate::planner::shared::plan::{PlanSelect, PlanSourceEntry};
use crate::planner::shared::translate;
use crate::planner::PlanError;
use super::{ReactiveCondition, ReactiveConditionKind};

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

/// Recursively check whether the expression tree contains any subquery node.
fn contains_subquery(expr: &ast::AstExpr) -> bool {
    match expr {
        ast::AstExpr::Subquery(_) | ast::AstExpr::InSubquery { .. } => true,
        ast::AstExpr::Binary { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        ast::AstExpr::Aggregate { arg, .. } => contains_subquery(arg),
        ast::AstExpr::InList { expr, values } => {
            contains_subquery(expr) || values.iter().any(contains_subquery)
        }
        ast::AstExpr::Reactive(inner) => contains_subquery(inner),
        ast::AstExpr::Column(_) | ast::AstExpr::Literal(_) => false,
    }
}

/// Decompose a REACTIVE inner expression into a ReactiveCondition.
///
/// Phase 1 only: produces logical conditions with the full predicate.
/// No lookup key extraction — that is done by the optimizer (Phase 2).
///
/// If the inner expression is a plain column reference → TableLevel.
/// Otherwise → Condition with the full predicate.
///
/// ## Subqueries are rejected
///
/// Any subquery form (`IN (SELECT ...)`, `op (SELECT ...)`) inside a REACTIVE()
/// argument is rejected with a plan error. Subqueries *outside* REACTIVE() (e.g.
/// in the WHERE clause) are unaffected.
///
/// Reasoning: the current reactive model is a single-row point-lookup invalidator
/// (reverse index on column values + per-row verify filter). A subquery makes the
/// predicate depend on rows of *another* table, so:
/// - A mutation on that other table can invalidate the subscription without ever
///   touching the subscribed table — the reverse index has no entry for that.
/// - Re-evaluating the subquery on every mutation would defeat the O(1) property.
/// - Snapshotting the subquery at subscribe time silently goes stale.
///
/// Proper support requires incremental view maintenance (track the subquery as
/// its own reactive view, propagate changes through join operators). That's a
/// different architectural layer than what this module implements.
fn decompose_condition(
    expr: &ast::AstExpr,
    sources: &[PlanSourceEntry],
) -> Result<ReactiveCondition, PlanError> {
    // Plain column reference → table-level
    if let ast::AstExpr::Column(_) = expr {
        let cr = translate::resolve_to_column_ref(expr, sources)?;
        return Ok(ReactiveCondition {
            table: sources[cr.source].alias().to_string(),
            kind: ReactiveConditionKind::TableLevel,
            source_idx: cr.source,
        });
    }

    // Reject subqueries early — see doc comment above. Without this check
    // `plan_expr_to_predicate` would recursively plan the subquery into a
    // local `PlanContext`, return an `InMaterialized { mat_id }` / `CompareMaterialized`,
    // and then drop the context — leaving a dangling mat_id in the predicate.
    if contains_subquery(expr) {
        return Err(PlanError::UnsupportedExpr(
            "subqueries are not supported inside REACTIVE(...) — the current \
             reactive model cannot track cross-table dependencies introduced \
             by a subquery. Move the subquery to the WHERE clause, or express \
             the condition without a subquery.".into(),
        ));
    }

    // Expression with conditions → Condition with full predicate.
    // The `PlanContext` is required by `plan_expr_to_predicate` for subquery
    // materialization, but we've guaranteed above that no subquery cases fire,
    // so `materializations` stays empty.
    let dummy_schemas = std::collections::HashMap::new();
    let dummy_requirements = crate::planner::requirement::RequirementRegistry::new();
    let query_schemas = sources
        .iter()
        .map(|s| (s.alias().to_string(), s.schema().clone()))
        .collect();
    let mut ctx = crate::planner::PlanContext {
        table_schemas: &dummy_schemas,
        requirements: &dummy_requirements,
        query_schemas,
        materializations: Vec::new(),
        bound_values: std::collections::HashMap::new(),
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
        table: sources[source_idx].alias().to_string(),
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
    use crate::planner::shared::plan::{PlanFilterPredicate, PlanScanMethod, PlanSource};

    fn users_sources() -> Vec<PlanSourceEntry> {
        vec![PlanSourceEntry {
            source: PlanSource::Table {
                name: "users".into(),
                schema: Schema::new(vec![
                    ColumnDef { table: Some("users".into()), name: "id".into() },
                    ColumnDef { table: Some("users".into()), name: "name".into() },
                    ColumnDef { table: Some("users".into()), name: "age".into() },
                ]),
                scan_method: PlanScanMethod::Full,
            },
            join: None,
            pre_filter: PlanFilterPredicate::None,
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

    /// Dummy subquery — contents don't matter, we only care about the AST shape.
    fn dummy_subquery() -> Box<ast::AstSelect> {
        Box::new(ast::AstSelect {
            sources: vec![ast::AstSourceEntry { source: ast::AstSource::Table("orders".into()), alias: None, join: None }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        })
    }

    #[test]
    fn test_reject_in_subquery_inside_reactive() {
        // REACTIVE(users.id IN (SELECT ...)) → error
        let sources = users_sources();
        let expr = ast::AstExpr::InSubquery {
            expr: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                table: "users".into(),
                column: "id".into(),
            })),
            subquery: dummy_subquery(),
        };
        let err = decompose_condition(&expr, &sources).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("subqueries are not supported"), "got: {msg}");
    }

    #[test]
    fn test_reject_scalar_subquery_inside_reactive() {
        // REACTIVE(users.age > (SELECT ...)) → error
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                table: "users".into(),
                column: "age".into(),
            })),
            op: ast::Operator::Gt,
            right: Box::new(ast::AstExpr::Subquery(dummy_subquery())),
        };
        let err = decompose_condition(&expr, &sources).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }

    #[test]
    fn test_reject_subquery_nested_inside_and() {
        // REACTIVE(users.id = 1 AND users.id IN (SELECT ...)) → also rejected
        let sources = users_sources();
        let eq = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "id".into() })),
            op: ast::Operator::Eq,
            right: Box::new(ast::AstExpr::Literal(Value::Int(1))),
        };
        let in_sq = ast::AstExpr::InSubquery {
            expr: Box::new(ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "id".into() })),
            subquery: dummy_subquery(),
        };
        let expr = ast::AstExpr::Binary {
            left: Box::new(eq),
            op: ast::Operator::And,
            right: Box::new(in_sq),
        };
        let err = decompose_condition(&expr, &sources).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }
}
