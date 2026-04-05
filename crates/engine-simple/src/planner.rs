pub mod plan;

use std::collections::HashMap;

use query_engine::ast;
use query_engine::schema::Schema;
use plan::*;

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

fn plan_select_ctx(
    select: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanSelect, PlanError> {
    if select.sources.is_empty() {
        return Err(PlanError::EmptySources);
    }

    let mut sources = Vec::new();

    for entry in &select.sources {
        let table_schema = ctx.schemas
            .get(&entry.table)
            .ok_or_else(|| PlanError::UnknownTable(entry.table.clone()))?
            .clone();

        sources.push(PlanSourceEntry {
            table: entry.table.clone(),
            schema: table_schema,
            join: None,
            pre_filter: PlanFilterPredicate::None,
        });

        // Resolve join condition against all sources added so far.
        if let Some(jc) = &entry.join {
            let on = plan_filter(&jc.on, &sources, ctx)?;
            sources.last_mut().unwrap().join = Some(PlanJoin {
                join_type: jc.join_type,
                on,
            });
        }
    }

    let filter = plan_filter(&select.filter, &sources, ctx)?;

    let group_by = select
        .group_by
        .iter()
        .map(|expr| resolve_to_column_ref(expr, &sources))
        .collect::<Result<Vec<_>, _>>()?;

    let result_columns = select
        .result_columns
        .iter()
        .map(|rc| plan_result_column(rc, &sources))
        .collect::<Result<Vec<_>, _>>()?;

    let aggregates = result_columns
        .iter()
        .filter_map(|rc| match rc {
            PlanResultColumn::Aggregate { func, col, .. } => {
                Some(PlanAggregate { func: *func, col: *col })
            }
            _ => None,
        })
        .collect();

    let order_by = select
        .order_by
        .iter()
        .map(|spec| {
            let col = resolve_to_column_ref(&spec.expr, &sources)?;
            Ok(PlanOrderSpec {
                col,
                direction: spec.direction,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let limit = select.limit.map(|n| n as usize);

    let mut plan = PlanSelect {
        sources,
        filter,
        group_by,
        aggregates,
        order_by,
        limit,
        result_columns,
    };

    plan.filter = optimize_or_to_in(plan.filter);
    for source in &mut plan.sources {
        if let Some(ref mut join) = source.join {
            join.on = optimize_or_to_in(join.on.clone());
        }
    }

    pushdown_filters(&mut plan);

    Ok(plan)
}

// ── OR → IN rewrite ──────────────────────────────────────────────────────

/// Rewrite `col = A OR col = B` into `col IN (A, B)` so index scans can be used.
/// Works recursively on nested OR chains and merges existing IN predicates.
fn optimize_or_to_in(pred: PlanFilterPredicate) -> PlanFilterPredicate {
    match pred {
        PlanFilterPredicate::Or(l, r) => {
            let l = optimize_or_to_in(*l);
            let r = optimize_or_to_in(*r);
            let or_pred = PlanFilterPredicate::Or(Box::new(l), Box::new(r));
            try_merge_or_to_in(&or_pred).unwrap_or(or_pred)
        }
        PlanFilterPredicate::And(l, r) => {
            PlanFilterPredicate::And(
                Box::new(optimize_or_to_in(*l)),
                Box::new(optimize_or_to_in(*r)),
            )
        }
        other => other,
    }
}

/// If the predicate is an OR-chain where every leaf is `Equals` or `In` on the
/// same column, merge into a single `In`.
fn try_merge_or_to_in(pred: &PlanFilterPredicate) -> Option<PlanFilterPredicate> {
    let mut col: Option<ColumnRef> = None;
    let mut values = Vec::new();
    if collect_or_eq_values(pred, &mut col, &mut values) {
        Some(PlanFilterPredicate::In { col: col?, values })
    } else {
        None
    }
}

fn collect_or_eq_values(
    pred: &PlanFilterPredicate,
    col: &mut Option<ColumnRef>,
    values: &mut Vec<ast::Value>,
) -> bool {
    match pred {
        PlanFilterPredicate::Equals { col: c, value } => {
            if col.map_or(true, |existing| existing == *c) {
                *col = Some(*c);
                values.push(value.clone());
                true
            } else {
                false
            }
        }
        PlanFilterPredicate::In { col: c, values: vs } => {
            if col.map_or(true, |existing| existing == *c) {
                *col = Some(*c);
                values.extend(vs.iter().cloned());
                true
            } else {
                false
            }
        }
        PlanFilterPredicate::Or(l, r) => {
            collect_or_eq_values(l, col, values) && collect_or_eq_values(r, col, values)
        }
        _ => false,
    }
}

fn pushdown_filters(plan: &mut PlanSelect) {
    let filter = std::mem::replace(&mut plan.filter, PlanFilterPredicate::None);
    let conjuncts = flatten_and_conjuncts(filter);

    let mut remaining = Vec::new();
    for conjunct in conjuncts {
        let refs = predicate_column_refs(&conjunct);
        // Check if all column refs belong to the same source.
        let first_source = refs.first().map(|r| r.source);
        let single_source = first_source.filter(|&s| refs.iter().all(|r| r.source == s));

        match single_source {
            Some(src_idx) => {
                let existing = std::mem::replace(
                    &mut plan.sources[src_idx].pre_filter,
                    PlanFilterPredicate::None,
                );
                plan.sources[src_idx].pre_filter = match existing {
                    PlanFilterPredicate::None => conjunct,
                    other => PlanFilterPredicate::And(Box::new(other), Box::new(conjunct)),
                };
            }
            None => remaining.push(conjunct),
        }
    }

    plan.filter = match remaining.len() {
        0 => PlanFilterPredicate::None,
        _ => remaining
            .into_iter()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap(),
    };
}

fn flatten_and_conjuncts(pred: PlanFilterPredicate) -> Vec<PlanFilterPredicate> {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            let mut out = flatten_and_conjuncts(*l);
            out.extend(flatten_and_conjuncts(*r));
            out
        }
        PlanFilterPredicate::None => vec![],
        other => vec![other],
    }
}

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

fn plan_filter(
    exprs: &[ast::AstExpr],
    sources: &[PlanSourceEntry],
    ctx: &mut PlanContext,
) -> Result<PlanFilterPredicate, PlanError> {
    let mut predicates: Vec<PlanFilterPredicate> = exprs
        .iter()
        .map(|e| plan_expr_to_predicate(e, sources, ctx))
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
    sources: &[PlanSourceEntry],
    ctx: &mut PlanContext,
) -> Result<PlanFilterPredicate, PlanError> {
    match expr {
        ast::AstExpr::Binary { left, op, right } => {
            if *op == ast::Operator::And {
                let l = plan_expr_to_predicate(left, sources, ctx)?;
                let r = plan_expr_to_predicate(right, sources, ctx)?;
                return Ok(PlanFilterPredicate::And(Box::new(l), Box::new(r)));
            }
            if *op == ast::Operator::Or {
                let l = plan_expr_to_predicate(left, sources, ctx)?;
                let r = plan_expr_to_predicate(right, sources, ctx)?;
                return Ok(PlanFilterPredicate::Or(Box::new(l), Box::new(r)));
            }

            match (left.as_ref(), right.as_ref()) {
                (ast::AstExpr::Column(col), ast::AstExpr::Literal(val)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    column_value_predicate(cr, *op, val.clone())
                }
                (ast::AstExpr::Literal(val), ast::AstExpr::Column(col)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    column_value_predicate(cr, flip_op(*op)?, val.clone())
                }
                (ast::AstExpr::Column(left_col), ast::AstExpr::Column(right_col)) => {
                    let left_cr = resolve_column_ref(left_col, sources)?;
                    let right_cr = resolve_column_ref(right_col, sources)?;
                    column_column_predicate(left_cr, *op, right_cr)
                }
                (ast::AstExpr::Column(col), ast::AstExpr::Subquery(subquery)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    let subquery_plan = plan_select_ctx(subquery, ctx)?;
                    let mat_id = ctx.add_materialization(subquery_plan, MaterializeKind::Scalar);
                    Ok(PlanFilterPredicate::CompareMaterialized { col: cr, op: *op, mat_id })
                }
                (ast::AstExpr::Subquery(subquery), ast::AstExpr::Column(col)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    let subquery_plan = plan_select_ctx(subquery, ctx)?;
                    let mat_id = ctx.add_materialization(subquery_plan, MaterializeKind::Scalar);
                    Ok(PlanFilterPredicate::CompareMaterialized { col: cr, op: flip_op(*op)?, mat_id })
                }
                _ => Err(PlanError::UnsupportedExpr(
                    "only Column/Literal/Subquery operands supported".into(),
                )),
            }
        }
        ast::AstExpr::InList { expr, values } => {
            let col = resolve_to_column_ref(expr, sources)?;
            let vals: Vec<ast::Value> = values
                .iter()
                .map(|v| match v {
                    ast::AstExpr::Literal(val) => Ok(val.clone()),
                    _ => Err(PlanError::UnsupportedExpr(
                        "IN values must be literals".into(),
                    )),
                })
                .collect::<Result<_, _>>()?;
            Ok(PlanFilterPredicate::In { col, values: vals })
        }
        ast::AstExpr::InSubquery { expr, subquery } => {
            let col = resolve_to_column_ref(expr, sources)?;
            let subquery_plan = plan_select_ctx(subquery, ctx)?;
            let mat_id = ctx.add_materialization(subquery_plan, MaterializeKind::List);
            Ok(PlanFilterPredicate::InMaterialized { col, mat_id })
        }
        _ => Err(PlanError::UnsupportedExpr(
            "filter must be a binary expression, IN, or IN subquery".into(),
        )),
    }
}

fn resolve_column_ref(col: &ast::AstColumnRef, sources: &[PlanSourceEntry]) -> Result<ColumnRef, PlanError> {
    for (source_idx, source) in sources.iter().enumerate() {
        if let Some(col_idx) = source.schema.resolve(&col.table, &col.column) {
            return Ok(ColumnRef { source: source_idx, col: col_idx });
        }
    }
    Err(PlanError::UnknownColumn {
        table: col.table.clone(),
        column: col.column.clone(),
    })
}

fn resolve_to_column_ref(expr: &ast::AstExpr, sources: &[PlanSourceEntry]) -> Result<ColumnRef, PlanError> {
    match expr {
        ast::AstExpr::Column(col) => resolve_column_ref(col, sources),
        _ => Err(PlanError::UnsupportedExpr(
            "expected a column reference".into(),
        )),
    }
}

fn column_value_predicate(
    col: ColumnRef,
    op: ast::Operator,
    value: ast::Value,
) -> Result<PlanFilterPredicate, PlanError> {
    match op {
        ast::Operator::Eq => Ok(PlanFilterPredicate::Equals { col, value }),
        ast::Operator::Neq => Ok(PlanFilterPredicate::NotEquals { col, value }),
        ast::Operator::Gt => Ok(PlanFilterPredicate::GreaterThan { col, value }),
        ast::Operator::Gte => Ok(PlanFilterPredicate::GreaterThanOrEqual { col, value }),
        ast::Operator::Lt => Ok(PlanFilterPredicate::LessThan { col, value }),
        ast::Operator::Lte => Ok(PlanFilterPredicate::LessThanOrEqual { col, value }),
        _ => Err(PlanError::UnsupportedExpr(format!(
            "{op:?} not supported for column/value comparison"
        ))),
    }
}

fn column_column_predicate(
    left: ColumnRef,
    op: ast::Operator,
    right: ColumnRef,
) -> Result<PlanFilterPredicate, PlanError> {
    match op {
        ast::Operator::Eq => Ok(PlanFilterPredicate::ColumnEquals { left, right }),
        ast::Operator::Neq => Ok(PlanFilterPredicate::ColumnNotEquals { left, right }),
        ast::Operator::Gt => Ok(PlanFilterPredicate::ColumnGreaterThan { left, right }),
        ast::Operator::Gte => Ok(PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right }),
        ast::Operator::Lt => Ok(PlanFilterPredicate::ColumnLessThan { left, right }),
        ast::Operator::Lte => Ok(PlanFilterPredicate::ColumnLessThanOrEqual { left, right }),
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

fn plan_result_column(
    rc: &ast::AstResultColumn,
    sources: &[PlanSourceEntry],
) -> Result<PlanResultColumn, PlanError> {
    match &rc.expr {
        ast::AstExpr::Aggregate { func, arg } => {
            let col = resolve_to_column_ref(arg, sources)?;
            Ok(PlanResultColumn::Aggregate {
                func: *func,
                col,
                alias: rc.alias.clone(),
            })
        }
        other => {
            let col = resolve_to_column_ref(other, sources)?;
            Ok(PlanResultColumn::Column {
                col,
                alias: rc.alias.clone(),
            })
        }
    }
}

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

        // Single-source filter gets pushed down
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
