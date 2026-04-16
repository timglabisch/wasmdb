//! Reactive type definitions and condition extraction from the AST.

use sql_parser::ast::{self, Value};

use super::plan::{PlanSelect, PlanSourceEntry, PlanFilterPredicate, ColumnRef};
use super::translate;
use super::PlanError;

// ── Type definitions ─────────────────────────────────────────────────────

/// An equality predicate that becomes a reverse-index key.
#[derive(Debug, Clone)]
pub struct ReactiveKey {
    pub col: usize,
    pub value: Value,
}

/// One REACTIVE condition — either table-level or fine-grained with index keys.
#[derive(Debug, Clone)]
pub struct ReactiveCondition {
    pub table: String,
    pub kind: ReactiveConditionKind,
    pub source_idx: usize,
}

/// Whether a reactive condition is table-level or fine-grained.
#[derive(Debug, Clone)]
pub enum ReactiveConditionKind {
    /// Table-level: any change to the table triggers invalidation.
    /// Produced by `reactive(column_ref)`.
    TableLevel,
    /// Fine-grained: only changes matching the condition trigger invalidation.
    /// Produced by `reactive(expr)` where expr contains equality predicates.
    Condition {
        eq_keys: Vec<ReactiveKey>,
        verify_filter: PlanFilterPredicate,
    },
}

// ── Extraction ───────────────────────────────────────────────────────────

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
/// If the inner expression is a plain column reference → TableLevel.
/// Otherwise → decompose into index keys + verify filter.
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

    // Expression with conditions → fine-grained
    let dummy_schemas = std::collections::HashMap::new();
    let query_schemas = sources
        .iter()
        .map(|s| (s.table.clone(), s.schema.clone()))
        .collect();
    let mut ctx = super::PlanContext {
        table_schemas: &dummy_schemas,
        query_schemas,
        materializations: Vec::new(),
    };

    let predicate = translate::plan_expr_to_predicate(expr, sources, &mut ctx)?;
    let (eq_keys, verify_filter) = split_eq_keys(&predicate);

    if eq_keys.is_empty() {
        // No equality predicates → treat as table-level for all referenced tables
        let col_refs = predicate.column_refs();
        if let Some(first) = col_refs.first() {
            return Ok(ReactiveCondition {
                table: sources[first.source].table.clone(),
                kind: ReactiveConditionKind::TableLevel,
                source_idx: first.source,
            });
        }
        return Err(PlanError::UnsupportedExpr(
            "REACTIVE condition must reference at least one column".into(),
        ));
    }

    // All index keys must reference the same table.
    let source_idx = eq_keys[0].col_ref.source;
    for key in &eq_keys[1..] {
        if key.col_ref.source != source_idx {
            return Err(PlanError::UnsupportedExpr(
                "REACTIVE equality predicates must all reference the same table".into(),
            ));
        }
    }

    let table = sources[source_idx].table.clone();

    Ok(ReactiveCondition {
        table,
        kind: ReactiveConditionKind::Condition {
            eq_keys: eq_keys
                .into_iter()
                .map(|k| ReactiveKey {
                    col: k.col_ref.col,
                    value: k.value,
                })
                .collect(),
            verify_filter,
        },
        source_idx,
    })
}

/// Intermediate representation for extracted index keys.
struct ExtractedKey {
    col_ref: ColumnRef,
    value: sql_parser::ast::Value,
}

/// Split an AND chain into index keys (equality predicates) and remaining verify filter.
fn split_eq_keys(
    pred: &PlanFilterPredicate,
) -> (Vec<ExtractedKey>, PlanFilterPredicate) {
    let mut keys = Vec::new();
    let mut rest = Vec::new();
    flatten_and(pred, &mut keys, &mut rest);
    let verify = PlanFilterPredicate::combine_and(rest);
    (keys, verify)
}

/// Recursively flatten AND nodes, extracting Equals predicates as index keys.
fn flatten_and(
    pred: &PlanFilterPredicate,
    keys: &mut Vec<ExtractedKey>,
    rest: &mut Vec<PlanFilterPredicate>,
) {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            flatten_and(l, keys, rest);
            flatten_and(r, keys, rest);
        }
        PlanFilterPredicate::Equals { col, value } => {
            keys.push(ExtractedKey {
                col_ref: *col,
                value: value.clone(),
            });
        }
        other => {
            rest.push(other.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;
    use sql_parser::schema::{ColumnDef, Schema};
    use super::super::plan::PlanScanMethod;

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
            ReactiveConditionKind::Condition { eq_keys, verify_filter } => {
                assert_eq!(eq_keys.len(), 1);
                assert_eq!(eq_keys[0].col, 0);
                assert!(matches!(eq_keys[0].value, Value::Placeholder(ref n) if n == "uid"));
                assert!(matches!(verify_filter, PlanFilterPredicate::None));
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
    fn test_decompose_no_equality_falls_back_to_table_level() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef { table: "users".into(), column: "age".into() })),
            op: ast::Operator::Gt,
            right: Box::new(ast::AstExpr::Literal(Value::Int(18))),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.table, "users");
        assert!(matches!(cond.kind, ReactiveConditionKind::TableLevel));
    }
}
