use std::collections::HashMap;

use crate::ast;
use crate::plan::*;
use crate::schema::Schema;

#[derive(Debug)]
pub enum PlanError {
    UnknownTable(String),
    UnknownColumn { table: Option<String>, column: String },
    UnsupportedExpr(String),
    EmptySources,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::UnknownTable(t) => write!(f, "unknown table: {t}"),
            PlanError::UnknownColumn { table, column } => match table {
                Some(t) => write!(f, "unknown column: {t}.{column}"),
                None => write!(f, "unknown column: {column}"),
            },
            PlanError::UnsupportedExpr(msg) => write!(f, "unsupported expression: {msg}"),
            PlanError::EmptySources => write!(f, "query has no sources"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Translate an AstSelect into an executable PlanSelect.
pub fn plan_select(
    select: &ast::AstSelect,
    table_schemas: &HashMap<String, Schema>,
) -> Result<PlanSelect, PlanError> {
    if select.sources.is_empty() {
        return Err(PlanError::EmptySources);
    }

    let mut combined_schema = Schema::new(vec![]);
    let mut sources = Vec::new();

    for entry in &select.sources {
        let table_schema = table_schemas
            .get(&entry.table)
            .ok_or_else(|| PlanError::UnknownTable(entry.table.clone()))?
            .clone();

        combined_schema = Schema::merge(&combined_schema, &table_schema);

        let join = match &entry.join {
            Some(jc) => {
                let on = plan_filter(&jc.on, &combined_schema)?;
                Some(PlanJoin {
                    join_type: jc.join_type,
                    on,
                })
            }
            None => None,
        };

        sources.push(PlanSourceEntry {
            table: entry.table.clone(),
            schema: table_schema,
            join,
        });
    }

    let filter = plan_filter(&select.filter, &combined_schema)?;

    let group_by = select
        .group_by
        .iter()
        .map(|expr| resolve_to_column_idx(expr, &combined_schema))
        .collect::<Result<Vec<_>, _>>()?;

    let aggregates = select
        .aggregates
        .iter()
        .map(|agg| plan_aggregate(agg, &combined_schema))
        .collect::<Result<Vec<_>, _>>()?;

    let result_columns = select
        .result_columns
        .iter()
        .map(|rc| plan_result_column(rc, &combined_schema))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PlanSelect {
        sources,
        filter,
        group_by,
        aggregates,
        result_columns,
        schema: combined_schema,
    })
}

fn plan_filter(
    exprs: &[ast::AstExpr],
    schema: &Schema,
) -> Result<PlanFilterPredicate, PlanError> {
    let mut predicates: Vec<PlanFilterPredicate> = exprs
        .iter()
        .map(|e| plan_expr_to_predicate(e, schema))
        .collect::<Result<Vec<_>, _>>()?;

    match predicates.len() {
        0 => Ok(PlanFilterPredicate::None),
        1 => Ok(predicates.remove(0)),
        _ => {
            let mut combined = predicates.remove(0);
            for p in predicates {
                combined = PlanFilterPredicate::And(Box::new(combined), Box::new(p));
            }
            Ok(combined)
        }
    }
}

fn plan_expr_to_predicate(
    expr: &ast::AstExpr,
    schema: &Schema,
) -> Result<PlanFilterPredicate, PlanError> {
    match expr {
        ast::AstExpr::Binary { left, op, right } => {
            if *op == ast::Operator::And {
                let l = plan_expr_to_predicate(left, schema)?;
                let r = plan_expr_to_predicate(right, schema)?;
                return Ok(PlanFilterPredicate::And(Box::new(l), Box::new(r)));
            }
            if *op == ast::Operator::Or {
                let l = plan_expr_to_predicate(left, schema)?;
                let r = plan_expr_to_predicate(right, schema)?;
                return Ok(PlanFilterPredicate::Or(Box::new(l), Box::new(r)));
            }

            match (left.as_ref(), right.as_ref()) {
                (ast::AstExpr::Column(col), ast::AstExpr::Literal(val)) => {
                    let idx = resolve_column_ref(col, schema)?;
                    column_value_predicate(idx, *op, val.clone())
                }
                (ast::AstExpr::Literal(val), ast::AstExpr::Column(col)) => {
                    let idx = resolve_column_ref(col, schema)?;
                    column_value_predicate(idx, flip_op(*op)?, val.clone())
                }
                (ast::AstExpr::Column(left_col), ast::AstExpr::Column(right_col)) => {
                    let left_idx = resolve_column_ref(left_col, schema)?;
                    let right_idx = resolve_column_ref(right_col, schema)?;
                    column_column_predicate(left_idx, *op, right_idx)
                }
                _ => Err(PlanError::UnsupportedExpr(
                    "only Column/Literal operands supported".into(),
                )),
            }
        }
        _ => Err(PlanError::UnsupportedExpr(
            "filter must be a binary expression".into(),
        )),
    }
}

fn resolve_column_ref(col: &ast::AstColumnRef, schema: &Schema) -> Result<usize, PlanError> {
    schema
        .resolve(col.table.as_deref(), &col.column)
        .ok_or_else(|| PlanError::UnknownColumn {
            table: col.table.clone(),
            column: col.column.clone(),
        })
}

fn resolve_to_column_idx(expr: &ast::AstExpr, schema: &Schema) -> Result<usize, PlanError> {
    match expr {
        ast::AstExpr::Column(col) => resolve_column_ref(col, schema),
        _ => Err(PlanError::UnsupportedExpr(
            "expected a column reference".into(),
        )),
    }
}

fn column_value_predicate(
    column_idx: usize,
    op: ast::Operator,
    value: ast::Value,
) -> Result<PlanFilterPredicate, PlanError> {
    match op {
        ast::Operator::Eq => Ok(PlanFilterPredicate::Equals { column_idx, value }),
        ast::Operator::Neq => Ok(PlanFilterPredicate::NotEquals { column_idx, value }),
        ast::Operator::Gt => Ok(PlanFilterPredicate::GreaterThan { column_idx, value }),
        ast::Operator::Gte => Ok(PlanFilterPredicate::GreaterThanOrEqual { column_idx, value }),
        ast::Operator::Lt => Ok(PlanFilterPredicate::LessThan { column_idx, value }),
        ast::Operator::Lte => Ok(PlanFilterPredicate::LessThanOrEqual { column_idx, value }),
        _ => Err(PlanError::UnsupportedExpr(format!(
            "{op:?} not supported for column/value comparison"
        ))),
    }
}

fn column_column_predicate(
    left_idx: usize,
    op: ast::Operator,
    right_idx: usize,
) -> Result<PlanFilterPredicate, PlanError> {
    match op {
        ast::Operator::Eq => Ok(PlanFilterPredicate::ColumnEquals { left_idx, right_idx }),
        ast::Operator::Neq => Ok(PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx }),
        ast::Operator::Gt => Ok(PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx }),
        ast::Operator::Gte => Ok(PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx }),
        ast::Operator::Lt => Ok(PlanFilterPredicate::ColumnLessThan { left_idx, right_idx }),
        ast::Operator::Lte => Ok(PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx }),
        _ => Err(PlanError::UnsupportedExpr(format!(
            "{op:?} not supported for column/column comparison"
        ))),
    }
}

fn flip_op(op: ast::Operator) -> Result<ast::Operator, PlanError> {
    match op {
        ast::Operator::Eq => Ok(ast::Operator::Eq),
        ast::Operator::Neq => Ok(ast::Operator::Neq),
        ast::Operator::Gt => Ok(ast::Operator::Lt),
        ast::Operator::Lt => Ok(ast::Operator::Gt),
        ast::Operator::Gte => Ok(ast::Operator::Lte),
        ast::Operator::Lte => Ok(ast::Operator::Gte),
        _ => Err(PlanError::UnsupportedExpr(format!(
            "cannot flip operator {op:?}"
        ))),
    }
}

fn plan_aggregate(
    agg: &ast::AstAggregate,
    schema: &Schema,
) -> Result<PlanAggregate, PlanError> {
    let column_idx = resolve_to_column_idx(&agg.expr, schema)?;
    Ok(PlanAggregate {
        func: agg.func,
        column_idx,
    })
}

fn plan_result_column(
    rc: &ast::AstResultColumn,
    schema: &Schema,
) -> Result<PlanResultColumn, PlanError> {
    let column_idx = resolve_to_column_idx(&rc.expr, schema)?;
    Ok(PlanResultColumn {
        column_idx,
        alias: rc.alias.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use crate::schema::ColumnDef;

    fn users_schema() -> Schema {
        Schema::new(vec![
            ColumnDef { table: Some("users".into()), name: "id".into() },
            ColumnDef { table: Some("users".into()), name: "name".into() },
            ColumnDef { table: Some("users".into()), name: "age".into() },
        ])
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            ColumnDef { table: Some("orders".into()), name: "id".into() },
            ColumnDef { table: Some("orders".into()), name: "user_id".into() },
            ColumnDef { table: Some("orders".into()), name: "amount".into() },
        ])
    }

    fn table_schemas() -> HashMap<String, Schema> {
        let mut m = HashMap::new();
        m.insert("users".into(), users_schema());
        m.insert("orders".into(), orders_schema());
        m
    }

    #[test]
    fn test_simple_scan_with_filter() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "users".into(), join: None }],
            filter: vec![AstExpr::Binary {
                left: Box::new(AstExpr::Column(AstColumnRef {
                    table: Some("users".into()),
                    column: "age".into(),
                })),
                op: Operator::Gt,
                right: Box::new(AstExpr::Literal(Value::Int(18))),
            }],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![
                AstResultColumn { expr: AstExpr::Column(AstColumnRef { table: None, column: "name".into() }), alias: None },
                AstResultColumn { expr: AstExpr::Column(AstColumnRef { table: None, column: "age".into() }), alias: None },
            ],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();

        assert!(matches!(plan.filter, PlanFilterPredicate::GreaterThan { column_idx: 2, .. }));
        assert_eq!(plan.result_columns[0].column_idx, 1);
        assert_eq!(plan.result_columns[1].column_idx, 2);
    }

    #[test]
    fn test_join_with_column_comparison() {
        let select = AstSelect {
            sources: vec![
                AstSourceEntry { table: "users".into(), join: None },
                AstSourceEntry {
                    table: "orders".into(),
                    join: Some(AstJoinClause {
                        join_type: JoinType::Inner,
                        on: vec![AstExpr::Binary {
                            left: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("users".into()),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("orders".into()),
                                column: "user_id".into(),
                            })),
                        }],
                    }),
                },
            ],
            filter: vec![],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();

        let join = plan.sources[1].join.as_ref().unwrap();
        assert!(matches!(
            join.on,
            PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 }
        ));
    }

    #[test]
    fn test_three_table_join() {
        let mut schemas = table_schemas();
        schemas.insert(
            "products".into(),
            Schema::new(vec![
                ColumnDef { table: Some("products".into()), name: "id".into() },
                ColumnDef { table: Some("products".into()), name: "name".into() },
            ]),
        );

        let select = AstSelect {
            sources: vec![
                AstSourceEntry { table: "users".into(), join: None },
                AstSourceEntry {
                    table: "orders".into(),
                    join: Some(AstJoinClause {
                        join_type: JoinType::Inner,
                        on: vec![AstExpr::Binary {
                            left: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("users".into()),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("orders".into()),
                                column: "user_id".into(),
                            })),
                        }],
                    }),
                },
                AstSourceEntry {
                    table: "products".into(),
                    join: Some(AstJoinClause {
                        join_type: JoinType::Left,
                        on: vec![AstExpr::Binary {
                            left: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("orders".into()),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: Some("products".into()),
                                column: "id".into(),
                            })),
                        }],
                    }),
                },
            ],
            filter: vec![],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let plan = plan_select(&select, &schemas).unwrap();

        assert_eq!(plan.sources.len(), 3);
        assert_eq!(plan.schema.columns.len(), 8);

        let join1 = plan.sources[1].join.as_ref().unwrap();
        assert!(matches!(join1.on, PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 4 }));

        let join2 = plan.sources[2].join.as_ref().unwrap();
        assert!(matches!(join2.on, PlanFilterPredicate::ColumnEquals { left_idx: 3, right_idx: 6 }));
        assert_eq!(join2.join_type, JoinType::Left);
    }

    #[test]
    fn test_and_combined_filter() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "users".into(), join: None }],
            filter: vec![
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: None, column: "age".into() })),
                    op: Operator::Gt,
                    right: Box::new(AstExpr::Literal(Value::Int(18))),
                },
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: None, column: "name".into() })),
                    op: Operator::Eq,
                    right: Box::new(AstExpr::Literal(Value::Text("Alice".into()))),
                },
            ],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();
        assert!(matches!(plan.filter, PlanFilterPredicate::And(_, _)));
    }

    #[test]
    fn test_literal_on_left_flips() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "users".into(), join: None }],
            filter: vec![AstExpr::Binary {
                left: Box::new(AstExpr::Literal(Value::Int(18))),
                op: Operator::Lt,
                right: Box::new(AstExpr::Column(AstColumnRef { table: None, column: "age".into() })),
            }],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();
        assert!(matches!(plan.filter, PlanFilterPredicate::GreaterThan { column_idx: 2, .. }));
    }

    #[test]
    fn test_unknown_table_error() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "nonexistent".into(), join: None }],
            filter: vec![],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        assert!(matches!(err, PlanError::UnknownTable(_)));
    }

    #[test]
    fn test_empty_sources_error() {
        let select = AstSelect {
            sources: vec![],
            filter: vec![],
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        assert!(matches!(err, PlanError::EmptySources));
    }
}
