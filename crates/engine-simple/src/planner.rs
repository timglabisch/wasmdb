pub mod plan;
mod materialize;
mod optimize;
mod translate;

use std::collections::HashMap;

use query_engine::ast;
use query_engine::schema::Schema;
use plan::*;

pub use optimize::predicate_column_refs;

// ── Context ───────────────────────────────────────────────────────────────

struct PlanContext<'a> {
    schemas: &'a HashMap<String, Schema>,
    materializations: Vec<MaterializeStep>,
}

impl<'a> PlanContext<'a> {
    fn add_materialization(&mut self, plan: PlanSelect, kind: MaterializeKind) -> usize {
        let id = self.materializations.len();
        self.materializations.push(MaterializeStep { plan, kind });
        id
    }
}

// ── Error ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum PlanError {
    UnknownTable(String),
    UnknownColumn { table: String, column: String },
    UnsupportedExpr(String),
    EmptySources,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::UnknownTable(t) => write!(f, "unknown table: {t}"),
            PlanError::UnknownColumn { table, column } => {
                write!(f, "unknown column: {table}.{column}")
            }
            PlanError::UnsupportedExpr(msg) => write!(f, "unsupported expression: {msg}"),
            PlanError::EmptySources => write!(f, "query has no sources"),
        }
    }
}

impl std::error::Error for PlanError {}

// ── Entry points ──────────────────────────────────────────────────────────

/// Translate an AstSelect into an ExecutionPlan with materialization steps for subqueries.
pub fn plan(
    ast: &ast::AstSelect,
    table_schemas: &HashMap<String, Schema>,
) -> Result<ExecutionPlan, PlanError> {
    let mut ctx = PlanContext {
        schemas: table_schemas,
        materializations: Vec::new(),
    };
    let main = plan_select_ctx(ast, &mut ctx)?;
    Ok(ExecutionPlan {
        materializations: ctx.materializations,
        main,
    })
}

/// Translate an AstSelect into a PlanSelect (convenience wrapper).
/// Errors if the AST contains subqueries — use `plan()` instead.
pub fn plan_select(
    select: &ast::AstSelect,
    table_schemas: &HashMap<String, Schema>,
) -> Result<PlanSelect, PlanError> {
    let ep = plan(select, table_schemas)?;
    if !ep.materializations.is_empty() {
        return Err(PlanError::UnsupportedExpr(
            "unexpected subqueries; use plan() instead".into(),
        ));
    }
    Ok(ep.main)
}

// ── Internal pipeline ─────────────────────────────────────────────────────

/// Build a PlanSelect: translate AST → raw plan → optimize.
fn plan_select_ctx(
    select: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanSelect, PlanError> {
    let mut plan = translate::build_raw_plan(select, ctx)?;
    optimize::run(&mut plan);
    Ok(plan)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use query_engine::ast::*;
    use query_engine::schema::ColumnDef;

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
                    table: "users".into(),
                    column: "age".into(),
                })),
                op: Operator::Gt,
                right: Box::new(AstExpr::Literal(Value::Int(18))),
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![
                AstResultColumn { expr: AstExpr::Column(AstColumnRef { table: "users".into(), column: "name".into() }), alias: None },
                AstResultColumn { expr: AstExpr::Column(AstColumnRef { table: "users".into(), column: "age".into() }), alias: None },
            ],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();

        assert!(matches!(plan.filter, PlanFilterPredicate::None));
        assert!(matches!(
            plan.sources[0].pre_filter,
            PlanFilterPredicate::GreaterThan { col: ColumnRef { source: 0, col: 2 }, .. }
        ));
        assert!(matches!(plan.result_columns[0], PlanResultColumn::Column { col: ColumnRef { source: 0, col: 1 }, .. }));
        assert!(matches!(plan.result_columns[1], PlanResultColumn::Column { col: ColumnRef { source: 0, col: 2 }, .. }));
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
                                table: "users".into(),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: "orders".into(),
                                column: "user_id".into(),
                            })),
                        }],
                    }),
                },
            ],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();

        let join = plan.sources[1].join.as_ref().unwrap();
        assert!(matches!(
            join.on,
            PlanFilterPredicate::ColumnEquals {
                left: ColumnRef { source: 0, col: 0 },
                right: ColumnRef { source: 1, col: 1 },
            }
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
                                table: "users".into(),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: "orders".into(),
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
                                table: "orders".into(),
                                column: "id".into(),
                            })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef {
                                table: "products".into(),
                                column: "id".into(),
                            })),
                        }],
                    }),
                },
            ],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &schemas).unwrap();

        assert_eq!(plan.sources.len(), 3);

        let join1 = plan.sources[1].join.as_ref().unwrap();
        assert!(matches!(join1.on, PlanFilterPredicate::ColumnEquals {
            left: ColumnRef { source: 0, col: 0 },
            right: ColumnRef { source: 1, col: 1 },
        }));

        let join2 = plan.sources[2].join.as_ref().unwrap();
        assert!(matches!(join2.on, PlanFilterPredicate::ColumnEquals {
            left: ColumnRef { source: 1, col: 0 },
            right: ColumnRef { source: 2, col: 0 },
        }));
        assert_eq!(join2.join_type, JoinType::Left);
    }

    #[test]
    fn test_and_combined_filter() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "users".into(), join: None }],
            filter: vec![
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "age".into() })),
                    op: Operator::Gt,
                    right: Box::new(AstExpr::Literal(Value::Int(18))),
                },
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "name".into() })),
                    op: Operator::Eq,
                    right: Box::new(AstExpr::Literal(Value::Text("Alice".into()))),
                },
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();
        assert!(matches!(plan.filter, PlanFilterPredicate::None));
        assert!(matches!(plan.sources[0].pre_filter, PlanFilterPredicate::And(_, _)));
    }

    #[test]
    fn test_literal_on_left_flips() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "users".into(), join: None }],
            filter: vec![AstExpr::Binary {
                left: Box::new(AstExpr::Literal(Value::Int(18))),
                op: Operator::Lt,
                right: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "age".into() })),
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();
        assert!(matches!(plan.filter, PlanFilterPredicate::None));
        assert!(matches!(
            plan.sources[0].pre_filter,
            PlanFilterPredicate::GreaterThan { col: ColumnRef { source: 0, col: 2 }, .. }
        ));
    }

    #[test]
    fn test_unknown_table_error() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { table: "nonexistent".into(), join: None }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        assert!(matches!(err, PlanError::UnknownTable(_)));
    }

    #[test]
    fn test_pushdown_cross_source_stays() {
        let select = AstSelect {
            sources: vec![
                AstSourceEntry { table: "users".into(), join: None },
                AstSourceEntry {
                    table: "orders".into(),
                    join: Some(AstJoinClause {
                        join_type: JoinType::Inner,
                        on: vec![AstExpr::Binary {
                            left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "id".into() })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef { table: "orders".into(), column: "user_id".into() })),
                        }],
                    }),
                },
            ],
            filter: vec![AstExpr::Binary {
                left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "id".into() })),
                op: Operator::Eq,
                right: Box::new(AstExpr::Column(AstColumnRef { table: "orders".into(), column: "user_id".into() })),
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();
        assert!(matches!(plan.filter, PlanFilterPredicate::ColumnEquals { .. }));
        assert!(matches!(plan.sources[0].pre_filter, PlanFilterPredicate::None));
        assert!(matches!(plan.sources[1].pre_filter, PlanFilterPredicate::None));
    }

    #[test]
    fn test_pushdown_mixed() {
        let select = AstSelect {
            sources: vec![
                AstSourceEntry { table: "users".into(), join: None },
                AstSourceEntry {
                    table: "orders".into(),
                    join: Some(AstJoinClause {
                        join_type: JoinType::Inner,
                        on: vec![AstExpr::Binary {
                            left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "id".into() })),
                            op: Operator::Eq,
                            right: Box::new(AstExpr::Column(AstColumnRef { table: "orders".into(), column: "user_id".into() })),
                        }],
                    }),
                },
            ],
            filter: vec![
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: "users".into(), column: "age".into() })),
                    op: Operator::Gt,
                    right: Box::new(AstExpr::Literal(Value::Int(18))),
                },
                AstExpr::Binary {
                    left: Box::new(AstExpr::Column(AstColumnRef { table: "orders".into(), column: "amount".into() })),
                    op: Operator::Gt,
                    right: Box::new(AstExpr::Literal(Value::Int(50))),
                },
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas()).unwrap();

        assert!(matches!(plan.filter, PlanFilterPredicate::None));
        assert!(matches!(
            plan.sources[0].pre_filter,
            PlanFilterPredicate::GreaterThan { col: ColumnRef { source: 0, col: 2 }, .. }
        ));
        assert!(matches!(
            plan.sources[1].pre_filter,
            PlanFilterPredicate::GreaterThan { col: ColumnRef { source: 1, col: 2 }, .. }
        ));
    }

    #[test]
    fn test_empty_sources_error() {
        let select = AstSelect {
            sources: vec![],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        assert!(matches!(err, PlanError::EmptySources));
    }
}
