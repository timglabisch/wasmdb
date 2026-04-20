pub mod shared;
pub mod sql;
pub mod reactive;
pub mod requirement;

use std::collections::HashMap;

use sql_parser::ast;
use sql_parser::schema::{ColumnDef, Schema};
use crate::schema::TableSchema;
use shared::plan::*;
use shared::translate;


// ── Context ───────────────────────────────────────────────────────────────

pub struct PlanContext<'a> {
    pub table_schemas: &'a HashMap<String, TableSchema>,
    pub query_schemas: HashMap<String, Schema>,
    pub materializations: Vec<sql::plan::MaterializeStep>,
}

fn derive_query_schema(table_name: &str, ts: &TableSchema) -> Schema {
    Schema::new(
        ts.columns.iter().map(|c| ColumnDef {
            table: Some(table_name.into()),
            name: c.name.clone(),
        }).collect()
    )
}

impl<'a> PlanContext<'a> {
    pub(crate) fn add_materialization(&mut self, plan: PlanSelect, kind: sql::plan::MaterializeKind) -> usize {
        let id = self.materializations.len();
        self.materializations.push(sql::plan::MaterializeStep { plan, kind });
        id
    }
}

pub(crate) fn make_plan_context<'a>(table_schemas: &'a HashMap<String, TableSchema>) -> PlanContext<'a> {
    let query_schemas: HashMap<String, Schema> = table_schemas.iter()
        .map(|(name, ts)| (name.clone(), derive_query_schema(name, ts)))
        .collect();
    PlanContext {
        table_schemas,
        query_schemas,
        materializations: Vec::new(),
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

// ── Internal pipeline ─────────────────────────────────────────────────────

/// Build a PlanSelect: translate AST → raw plan → optimize.
pub(crate) fn plan_select_ctx(
    select: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanSelect, PlanError> {
    let mut plan = translate::build_raw_plan(select, ctx)?;
    sql::optimize::run(&mut plan, ctx.table_schemas);
    Ok(plan)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::sql::plan_select;
    use sql_parser::ast::*;
    use crate::schema::{ColumnSchema, DataType};

    fn users_table_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn orders_table_schema() -> TableSchema {
        TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn table_schemas() -> HashMap<String, TableSchema> {
        let mut m = HashMap::new();
        m.insert("users".into(), users_table_schema());
        m.insert("orders".into(), orders_table_schema());
        m
    }

    #[test]
    fn test_simple_scan_with_filter() {
        let select = AstSelect {
            sources: vec![AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None }],
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
                AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None },
                AstSourceEntry {
                    source: AstSource::Table("orders".into()),
                    alias: None,
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
            TableSchema {
                name: "products".into(),
                columns: vec![
                    ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                    ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ],
                primary_key: vec![0],
                indexes: vec![],
            },
        );

        let select = AstSelect {
            sources: vec![
                AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None },
                AstSourceEntry {
                    source: AstSource::Table("orders".into()),
                    alias: None,
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
                    source: AstSource::Table("products".into()),
                    alias: None,
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
            sources: vec![AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None }],
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
            sources: vec![AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None }],
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
            sources: vec![AstSourceEntry { source: AstSource::Table("nonexistent".into()), alias: None, join: None }],
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
                AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None },
                AstSourceEntry {
                    source: AstSource::Table("orders".into()),
                    alias: None,
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
                AstSourceEntry { source: AstSource::Table("users".into()), alias: None, join: None },
                AstSourceEntry {
                    source: AstSource::Table("orders".into()),
                    alias: None,
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

    // ── Guards for parser features not yet consumed by the planner ──
    //
    // These tests pin down the transitional state: the parser accepts
    // `schema.fn(args)` call syntax and FROM-clause aliases, but the
    // planner doesn't know what to do with them yet. Until the fetcher-
    // registry work lands
    // (see /Users/timglabisch/.claude/plans/fetcher-tables.md), the
    // planner must *reject* them with a clear UnsupportedExpr instead
    // of silently treating `foo.bar(...)` as a scan over table `bar`.

    #[test]
    fn test_planner_rejects_call() {
        let select = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Call {
                    schema: "customers".into(),
                    function: "by_owner".into(),
                    args: vec![AstExpr::Literal(Value::Int(42))],
                },
                alias: None,
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        match err {
            PlanError::UnsupportedExpr(msg) => {
                assert!(msg.contains("customers"), "missing schema in error: {msg}");
                assert!(msg.contains("by_owner"), "missing fn name in error: {msg}");
            }
            other => panic!("expected UnsupportedExpr for call, got {other:?}"),
        }
    }

    #[test]
    fn test_planner_rejects_from_alias() {
        let select = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Table("users".into()),
                alias: Some("u".into()),
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        match err {
            PlanError::UnsupportedExpr(msg) => {
                assert!(msg.contains("alias"), "missing 'alias' in error: {msg}");
            }
            other => panic!("expected UnsupportedExpr for alias, got {other:?}"),
        }
    }

    #[test]
    fn test_planner_rejects_call_in_join_position() {
        // Even a call source that appears as the right-hand side of a
        // JOIN must be rejected — the guard runs on every source entry,
        // not only the first.
        let select = AstSelect {
            sources: vec![
                AstSourceEntry {
                    source: AstSource::Table("users".into()),
                    alias: None,
                    join: None,
                },
                AstSourceEntry {
                    source: AstSource::Call {
                        schema: "orders".into(),
                        function: "for_user".into(),
                        args: vec![AstExpr::Literal(Value::Int(1))],
                    },
                    alias: None,
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

        let err = plan_select(&select, &table_schemas()).unwrap_err();
        assert!(
            matches!(&err, PlanError::UnsupportedExpr(msg) if msg.contains("for_user")),
            "expected UnsupportedExpr mentioning the call, got {err:?}",
        );
    }

    #[test]
    fn test_planner_accepts_plain_table_with_no_alias() {
        // Regression anchor: the guard above must NOT trip on normal
        // plain-table FROM clauses (schema=None + alias=None).
        let select = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Table("users".into()),
                alias: None,
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let plan = plan_select(&select, &table_schemas())
            .expect("plain table must plan successfully");
        assert_eq!(plan.sources.len(), 1);
        assert_eq!(plan.sources[0].table, "users");
    }
}
