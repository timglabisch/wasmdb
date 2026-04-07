//! Extracts and decomposes INVALIDATE_ON conditions from the AST.

use sql_parser::ast;

use super::plan::*;
use super::translate;
use super::PlanError;

/// Extract ReactiveMetadata from INVALIDATE_ON expressions in result columns.
pub fn extract_reactive_metadata(
    select: &ast::AstSelect,
    plan: &PlanSelect,
) -> Result<Option<ReactiveMetadata>, PlanError> {
    let invalidate_exprs: Vec<(usize, &ast::AstExpr)> = select
        .result_columns
        .iter()
        .enumerate()
        .filter_map(|(i, rc)| match &rc.expr {
            ast::AstExpr::InvalidateOn(inner) => Some((i, inner.as_ref())),
            _ => None,
        })
        .collect();

    if invalidate_exprs.is_empty() {
        return Ok(None);
    }

    let mut conditions = Vec::new();
    for (_idx, expr) in &invalidate_exprs {
        conditions.push(decompose_condition(expr, &plan.sources)?);
    }

    let strategy = classify_strategy(plan);

    Ok(Some(ReactiveMetadata {
        conditions,
        strategy,
    }))
}

/// Decompose an INVALIDATE_ON inner expression into InvalidationKeys + verify filter.
fn decompose_condition(
    expr: &ast::AstExpr,
    sources: &[PlanSourceEntry],
) -> Result<InvalidationCondition, PlanError> {
    // We need a dummy PlanContext to call plan_expr_to_predicate.
    // The context is only needed for subquery handling which we don't support here.
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
    let (index_keys, verify_filter) = split_index_keys(&predicate, sources);

    if index_keys.is_empty() {
        return Err(PlanError::UnsupportedExpr(
            "INVALIDATE_ON requires at least one equality predicate for the reverse index".into(),
        ));
    }

    // All index keys must reference the same table.
    let source_idx = index_keys[0].col_ref.source;
    for key in &index_keys[1..] {
        if key.col_ref.source != source_idx {
            return Err(PlanError::UnsupportedExpr(
                "INVALIDATE_ON equality predicates must all reference the same table".into(),
            ));
        }
    }

    let table = sources[source_idx].table.clone();

    Ok(InvalidationCondition {
        table: table.clone(),
        index_keys: index_keys
            .into_iter()
            .map(|k| InvalidationKey {
                table: table.clone(),
                col: k.col_ref.col,
                value: k.value,
            })
            .collect(),
        verify_filter,
        source_idx,
    })
}

/// Intermediate representation for extracted index keys.
struct ExtractedKey {
    col_ref: ColumnRef,
    value: sql_parser::ast::Value,
}

/// Split an AND chain into index keys (equality predicates) and remaining verify filter.
fn split_index_keys(
    pred: &PlanFilterPredicate,
    _sources: &[PlanSourceEntry],
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

/// Classify invalidation strategy based on plan shape.
fn classify_strategy(plan: &PlanSelect) -> InvalidationStrategy {
    if plan.sources.len() > 1 {
        return InvalidationStrategy::Invalidate;
    }
    if !plan.aggregates.is_empty() {
        return InvalidationStrategy::Invalidate;
    }
    InvalidationStrategy::ReExecute
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::Value;
    use sql_parser::schema::{ColumnDef, Schema};

    fn users_sources() -> Vec<PlanSourceEntry> {
        vec![PlanSourceEntry {
            table: "users".into(),
            schema: Schema::new(vec![
                ColumnDef {
                    table: Some("users".into()),
                    name: "id".into(),
                },
                ColumnDef {
                    table: Some("users".into()),
                    name: "name".into(),
                },
                ColumnDef {
                    table: Some("users".into()),
                    name: "age".into(),
                },
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
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                table: "users".into(),
                column: "id".into(),
            })),
            op: ast::Operator::Eq,
            right: Box::new(ast::AstExpr::Literal(Value::Placeholder("uid".into()))),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.table, "users");
        assert_eq!(cond.index_keys.len(), 1);
        assert_eq!(cond.index_keys[0].col, 0);
        assert!(matches!(cond.index_keys[0].value, Value::Placeholder(ref n) if n == "uid"));
        assert!(matches!(cond.verify_filter, PlanFilterPredicate::None));
    }

    #[test]
    fn test_decompose_equality_plus_range() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Binary {
                left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                    table: "users".into(),
                    column: "id".into(),
                })),
                op: ast::Operator::Eq,
                right: Box::new(ast::AstExpr::Literal(Value::Placeholder("uid".into()))),
            }),
            op: ast::Operator::And,
            right: Box::new(ast::AstExpr::Binary {
                left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                    table: "users".into(),
                    column: "age".into(),
                })),
                op: ast::Operator::Gt,
                right: Box::new(ast::AstExpr::Literal(Value::Int(18))),
            }),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.index_keys.len(), 1);
        assert_eq!(cond.index_keys[0].col, 0);
        assert!(matches!(
            cond.verify_filter,
            PlanFilterPredicate::GreaterThan { .. }
        ));
    }

    #[test]
    fn test_decompose_two_equalities() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Binary {
                left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                    table: "users".into(),
                    column: "id".into(),
                })),
                op: ast::Operator::Eq,
                right: Box::new(ast::AstExpr::Literal(Value::Placeholder("uid".into()))),
            }),
            op: ast::Operator::And,
            right: Box::new(ast::AstExpr::Binary {
                left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                    table: "users".into(),
                    column: "name".into(),
                })),
                op: ast::Operator::Eq,
                right: Box::new(ast::AstExpr::Literal(Value::Text("active".into()))),
            }),
        };
        let cond = decompose_condition(&expr, &sources).unwrap();
        assert_eq!(cond.index_keys.len(), 2);
        assert!(matches!(cond.verify_filter, PlanFilterPredicate::None));
    }

    #[test]
    fn test_decompose_no_equality_error() {
        let sources = users_sources();
        let expr = ast::AstExpr::Binary {
            left: Box::new(ast::AstExpr::Column(ast::AstColumnRef {
                table: "users".into(),
                column: "age".into(),
            })),
            op: ast::Operator::Gt,
            right: Box::new(ast::AstExpr::Literal(Value::Int(18))),
        };
        let err = decompose_condition(&expr, &sources).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }

    #[test]
    fn test_classify_single_table_reexecute() {
        let plan = PlanSelect {
            sources: users_sources(),
            filter: PlanFilterPredicate::None,
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };
        assert_eq!(classify_strategy(&plan), InvalidationStrategy::ReExecute);
    }

    #[test]
    fn test_classify_multi_table_invalidate() {
        let mut sources = users_sources();
        sources.push(PlanSourceEntry {
            table: "orders".into(),
            schema: Schema::new(vec![]),
            join: None,
            pre_filter: PlanFilterPredicate::None,
            scan_method: PlanScanMethod::Full,
        });
        let plan = PlanSelect {
            sources,
            filter: PlanFilterPredicate::None,
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };
        assert_eq!(classify_strategy(&plan), InvalidationStrategy::Invalidate);
    }
}
