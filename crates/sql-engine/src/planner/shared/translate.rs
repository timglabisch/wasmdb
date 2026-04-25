//! AST → raw PlanSelect translation.
//!
//! Translates AST expressions into plan predicates, resolves column references,
//! and delegates subquery handling to [`super::materialize`].

use sql_parser::ast;

use super::plan::*;
use crate::planner::sql::materialize;
use crate::planner::PlanContext;
use crate::planner::PlanError;
use crate::schema::DataType;

/// Build a raw PlanSelect from an AST (before optimization passes).
pub fn build_raw_plan(
    select: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanSelect, PlanError> {
    if select.sources.is_empty() {
        return Err(PlanError::EmptySources);
    }

    let mut sources = Vec::new();

    for (source_idx, entry) in select.sources.iter().enumerate() {
        if entry.alias.is_some() {
            return Err(PlanError::UnsupportedExpr(
                "FROM-clause alias (`AS name`) not yet supported".into(),
            ));
        }

        let plan_source = match &entry.source {
            ast::AstSource::Table(t) => {
                let schema = ctx.query_schemas
                    .get(t)
                    .ok_or_else(|| PlanError::UnknownTable(t.clone()))?
                    .clone();
                PlanSource::Table {
                    name: t.clone(),
                    schema,
                    scan_method: PlanScanMethod::Full,
                }
            }
            ast::AstSource::Call { schema, function, args } => {
                build_requirement_source(schema, function, args, source_idx, ctx)?
            }
        };

        sources.push(PlanSourceEntry {
            source: plan_source,
            join: None,
            pre_filter: PlanFilterPredicate::None,
        });

        if let Some(jc) = &entry.join {
            let on = plan_filter(&jc.on, &sources, ctx)?;
            sources.last_mut().unwrap().join = Some(PlanJoin {
                join_type: jc.join_type,
                on,
                strategy: PlanJoinStrategy::NestedLoop,
            });
        }
    }

    let filter = plan_filter(&select.filter, &sources, ctx)?;

    let group_by = select
        .group_by
        .iter()
        .map(|expr| resolve_to_column_ref(expr, &sources))
        .collect::<Result<Vec<_>, _>>()?;

    let mut reactive_counter = 0usize;
    let result_columns = select
        .result_columns
        .iter()
        .map(|rc| plan_result_column(rc, &sources, &mut reactive_counter))
        .collect::<Result<Vec<_>, _>>()?;

    let aggregates: Vec<PlanAggregate> = result_columns
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

    let limit = match &select.limit {
        None => None,
        Some(ast::AstLimit::Value(n)) => Some(PlanLimit::Value(*n as usize)),
        Some(ast::AstLimit::Placeholder(name)) => Some(PlanLimit::Placeholder(name.clone())),
    };

    Ok(PlanSelect {
        sources,
        filter,
        group_by,
        aggregates,
        order_by,
        limit,
        result_columns,
    })
}

// ── Requirement source construction ───────────────────────────────────────

fn build_requirement_source(
    schema: &str,
    function: &str,
    args: &[ast::AstExpr],
    source_idx: usize,
    ctx: &mut PlanContext,
) -> Result<PlanSource, PlanError> {
    let caller_id = format!("{schema}::{function}");

    let meta = ctx.requirements
        .get(&caller_id)
        .ok_or_else(|| PlanError::UnknownRequirement(caller_id.clone()))?
        .clone();

    if args.len() != meta.params.len() {
        return Err(PlanError::CallerArgCountMismatch {
            id: caller_id,
            expected: meta.params.len(),
            got: args.len(),
        });
    }

    let mut plan_args = Vec::with_capacity(args.len());
    for (arg_idx, expr) in args.iter().enumerate() {
        let param = &meta.params[arg_idx];
        let placeholder_arg = build_requirement_arg(
            expr,
            param.data_type,
            &caller_id,
            source_idx,
            arg_idx,
            ctx,
        )?;
        plan_args.push(placeholder_arg);
    }

    let row_table = meta.row_table.clone();
    let row_schema = ctx.query_schemas
        .get(&row_table)
        .ok_or_else(|| PlanError::UnknownTable(row_table.clone()))?
        .clone();

    Ok(PlanSource::Requirement {
        alias: row_table.clone(),
        row_table,
        row_schema,
        caller_id,
        args: plan_args,
    })
}

fn build_requirement_arg(
    expr: &ast::AstExpr,
    expected: DataType,
    caller_id: &str,
    source_idx: usize,
    arg_idx: usize,
    ctx: &mut PlanContext,
) -> Result<RequirementArg, PlanError> {
    let lit = match expr {
        ast::AstExpr::Literal(v) => v,
        _ => {
            return Err(PlanError::UnsupportedExpr(format!(
                "caller `{caller_id}` arg {arg_idx}: only literal arguments are supported"
            )));
        }
    };

    match lit {
        ast::Value::Placeholder(name) => Ok(RequirementArg::Placeholder(name.clone())),
        ast::Value::Int(_) => {
            typecheck_arg(expected, DataType::I64, caller_id, arg_idx)?;
            Ok(bind_literal(lit.clone(), source_idx, arg_idx, ctx))
        }
        ast::Value::Text(_) => {
            typecheck_arg(expected, DataType::String, caller_id, arg_idx)?;
            Ok(bind_literal(lit.clone(), source_idx, arg_idx, ctx))
        }
        ast::Value::Uuid(_) => {
            typecheck_arg(expected, DataType::Uuid, caller_id, arg_idx)?;
            Ok(bind_literal(lit.clone(), source_idx, arg_idx, ctx))
        }
        ast::Value::Null => Ok(bind_literal(lit.clone(), source_idx, arg_idx, ctx)),
        ast::Value::Float(_) => Err(PlanError::UnsupportedExpr(format!(
            "caller `{caller_id}` arg {arg_idx}: float literals not supported"
        ))),
        ast::Value::Bool(_) => Err(PlanError::UnsupportedExpr(format!(
            "caller `{caller_id}` arg {arg_idx}: bool literals not supported"
        ))),
    }
}

fn typecheck_arg(
    expected: DataType,
    got: DataType,
    caller_id: &str,
    arg_idx: usize,
) -> Result<(), PlanError> {
    if expected != got {
        return Err(PlanError::CallerArgTypeMismatch {
            id: caller_id.to_string(),
            arg_idx,
            expected: format!("{expected:?}"),
            got: format!("{got:?}"),
        });
    }
    Ok(())
}

fn bind_literal(
    value: ast::Value,
    source_idx: usize,
    arg_idx: usize,
    ctx: &mut PlanContext,
) -> RequirementArg {
    let name = format!("__caller_{source_idx}_arg_{arg_idx}");
    ctx.bound_values.insert(name.clone(), value);
    RequirementArg::Placeholder(name)
}

// ── AST expression → predicate ────────────────────────────────────────────

fn plan_filter(
    exprs: &[ast::AstExpr],
    sources: &[PlanSourceEntry],
    ctx: &mut PlanContext,
) -> Result<PlanFilterPredicate, PlanError> {
    let predicates: Vec<PlanFilterPredicate> = exprs
        .iter()
        .map(|e| plan_expr_to_predicate(e, sources, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PlanFilterPredicate::combine_and(predicates))
}

pub fn plan_expr_to_predicate(
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
                // Subquery operands → delegate to materialize module
                (ast::AstExpr::Column(col), ast::AstExpr::Subquery(subquery)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    materialize::plan_scalar_subquery(cr, *op, subquery, ctx)
                }
                (ast::AstExpr::Subquery(subquery), ast::AstExpr::Column(col)) => {
                    let cr = resolve_column_ref(col, sources)?;
                    materialize::plan_scalar_subquery(cr, flip_op(*op)?, subquery, ctx)
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
        // IN subquery → delegate to materialize module
        ast::AstExpr::InSubquery { expr, subquery } => {
            let col = resolve_to_column_ref(expr, sources)?;
            materialize::plan_in_subquery(col, subquery, ctx)
        }
        // Reactive in WHERE: transparent — inner expression is used as normal filter.
        // The reactive condition is extracted separately by reactive/extract.rs.
        ast::AstExpr::Reactive(inner) => plan_expr_to_predicate(inner, sources, ctx),
        _ => Err(PlanError::UnsupportedExpr(
            "filter must be a binary expression, IN, or IN subquery".into(),
        )),
    }
}

// ── Column resolution ─────────────────────────────────────────────────────

fn resolve_column_ref(col: &ast::AstColumnRef, sources: &[PlanSourceEntry]) -> Result<ColumnRef, PlanError> {
    for (source_idx, source) in sources.iter().enumerate() {
        if let Some(col_idx) = source.schema().resolve(&col.table, &col.column) {
            return Ok(ColumnRef { source: source_idx, col: col_idx });
        }
    }
    Err(PlanError::UnknownColumn {
        table: col.table.clone(),
        column: col.column.clone(),
    })
}

pub fn resolve_to_column_ref(expr: &ast::AstExpr, sources: &[PlanSourceEntry]) -> Result<ColumnRef, PlanError> {
    match expr {
        ast::AstExpr::Column(col) => resolve_column_ref(col, sources),
        _ => Err(PlanError::UnsupportedExpr(
            "expected a column reference".into(),
        )),
    }
}

// ── Predicate constructors ────────────────────────────────────────────────

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

pub fn flip_op(op: ast::Operator) -> Result<ast::Operator, PlanError> {
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

// ── Result column planning ────────────────────────────────────────────────

fn plan_result_column(
    rc: &ast::AstResultColumn,
    sources: &[PlanSourceEntry],
    reactive_counter: &mut usize,
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
        ast::AstExpr::Reactive(_) => {
            let idx = *reactive_counter;
            *reactive_counter += 1;
            Ok(PlanResultColumn::Reactive {
                condition_idx: idx,
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

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::make_plan_context;
    use crate::planner::requirement::{RequirementMeta, RequirementParamDef, RequirementRegistry};
    use crate::schema::{ColumnSchema, DataType, TableSchema};
    use sql_parser::ast::Value;
    use std::collections::HashMap;

    fn customers_schema() -> TableSchema {
        TableSchema {
            name: "customers".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn users_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn schemas_with(items: &[TableSchema]) -> HashMap<String, TableSchema> {
        items.iter().map(|t| (t.name.clone(), t.clone())).collect()
    }

    fn meta(row_table: &str, params: Vec<(&str, DataType)>) -> RequirementMeta {
        RequirementMeta {
            row_table: row_table.into(),
            params: params.into_iter()
                .map(|(n, t)| RequirementParamDef { name: n.into(), data_type: t })
                .collect(),
        }
    }

    fn run(
        sql: &str,
        schemas: &HashMap<String, TableSchema>,
        registry: &RequirementRegistry,
    ) -> Result<(PlanSelect, HashMap<String, Value>), PlanError> {
        let ast = sql_parser::parser::parse(sql).expect("parse");
        let mut ctx = make_plan_context(schemas, registry);
        let plan = build_raw_plan(&ast, &mut ctx)?;
        Ok((plan, ctx.bound_values))
    }

    #[test]
    fn call_with_int_literal_builds_requirement_source() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let (plan, bound) = run(
            "SELECT customers.name FROM customers.by_owner(42)",
            &schemas,
            &registry,
        )
        .expect("plan");

        assert_eq!(plan.sources.len(), 1);
        let src = &plan.sources[0].source;
        match src {
            PlanSource::Requirement { alias, row_table, caller_id, args, .. } => {
                assert_eq!(alias, "customers");
                assert_eq!(row_table, "customers");
                assert_eq!(caller_id, "customers::by_owner");
                assert_eq!(args.len(), 1);
                match &args[0] {
                    RequirementArg::Placeholder(n) => {
                        assert_eq!(n, "__caller_0_arg_0");
                        assert!(matches!(bound.get(n), Some(Value::Int(42))));
                    }
                }
            }
            other => panic!("expected Requirement source, got {other:?}"),
        }
    }

    #[test]
    fn call_with_text_literal_typechecks_against_string_param() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_name".into(),
            meta("customers", vec![("name", DataType::String)]),
        );

        let (_, bound) = run(
            "SELECT customers.id FROM customers.by_name('alice')",
            &schemas,
            &registry,
        )
        .expect("plan");

        assert!(matches!(
            bound.get("__caller_0_arg_0"),
            Some(Value::Text(s)) if s == "alice"
        ));
    }

    #[test]
    fn call_with_user_placeholder_passes_through() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let (plan, bound) = run(
            "SELECT customers.id FROM customers.by_owner(:owner_id)",
            &schemas,
            &registry,
        )
        .expect("plan");

        let PlanSource::Requirement { args, .. } = &plan.sources[0].source else {
            panic!("expected Requirement source");
        };
        match &args[0] {
            RequirementArg::Placeholder(n) => assert_eq!(n, "owner_id"),
        }
        assert!(bound.is_empty(), "user placeholders must not bind values");
    }

    #[test]
    fn call_with_null_literal_bypasses_typecheck() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let (_, bound) = run(
            "SELECT customers.id FROM customers.by_owner(NULL)",
            &schemas,
            &registry,
        )
        .expect("plan");

        assert!(matches!(bound.get("__caller_0_arg_0"), Some(Value::Null)));
    }

    #[test]
    fn call_and_plain_table_join() {
        let schemas = schemas_with(&[customers_schema(), users_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let (plan, _) = run(
            "SELECT customers.name, users.name \
             FROM customers.by_owner(42) \
             INNER JOIN users ON users.id = customers.id",
            &schemas,
            &registry,
        )
        .expect("plan");

        assert_eq!(plan.sources.len(), 2);
        assert!(matches!(plan.sources[0].source, PlanSource::Requirement { .. }));
        assert!(matches!(plan.sources[1].source, PlanSource::Table { .. }));
        assert!(plan.sources[1].join.is_some());
    }

    #[test]
    fn unknown_caller_rejected() {
        let schemas = schemas_with(&[customers_schema()]);
        let registry = RequirementRegistry::new();

        let err = run(
            "SELECT customers.id FROM x.y(1)",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(&err, PlanError::UnknownRequirement(id) if id == "x::y"),
            "got {err:?}"
        );
    }

    #[test]
    fn arg_count_mismatch_rejected() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_both".into(),
            meta(
                "customers",
                vec![("owner_id", DataType::I64), ("name", DataType::String)],
            ),
        );

        let err = run(
            "SELECT customers.id FROM customers.by_both(42)",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(
                &err,
                PlanError::CallerArgCountMismatch { id, expected: 2, got: 1 }
                    if id == "customers::by_both"
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn type_mismatch_rejected() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let err = run(
            "SELECT customers.id FROM customers.by_owner('alice')",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(
                &err,
                PlanError::CallerArgTypeMismatch { id, arg_idx: 0, .. }
                    if id == "customers::by_owner"
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn non_literal_arg_rejected() {
        // Use AST directly: the parser doesn't accept column-refs as
        // function arguments, so we bypass it.
        use sql_parser::ast::*;
        let ast = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Call {
                    schema: "customers".into(),
                    function: "by_owner".into(),
                    args: vec![AstExpr::Column(AstColumnRef {
                        table: "".into(),
                        column: "owner_id".into(),
                    })],
                },
                alias: None,
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![AstResultColumn {
                expr: AstExpr::Column(AstColumnRef { table: "customers".into(), column: "id".into() }),
                alias: None,
            }],
        };

        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );
        let mut ctx = make_plan_context(&schemas, &registry);

        let err = build_raw_plan(&ast, &mut ctx).unwrap_err();
        assert!(
            matches!(&err, PlanError::UnsupportedExpr(msg) if msg.contains("literal")),
            "got {err:?}"
        );
    }

    #[test]
    fn float_arg_rejected() {
        use sql_parser::ast::*;
        let ast = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Call {
                    schema: "customers".into(),
                    function: "by_owner".into(),
                    args: vec![AstExpr::Literal(Value::Float(1.5))],
                },
                alias: None,
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![AstResultColumn {
                expr: AstExpr::Column(AstColumnRef { table: "customers".into(), column: "id".into() }),
                alias: None,
            }],
        };

        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );
        let mut ctx = make_plan_context(&schemas, &registry);

        let err = build_raw_plan(&ast, &mut ctx).unwrap_err();
        assert!(
            matches!(&err, PlanError::UnsupportedExpr(msg) if msg.contains("float")),
            "got {err:?}"
        );
    }

    #[test]
    fn bool_arg_rejected() {
        use sql_parser::ast::*;
        let ast = AstSelect {
            sources: vec![AstSourceEntry {
                source: AstSource::Call {
                    schema: "customers".into(),
                    function: "by_owner".into(),
                    args: vec![AstExpr::Literal(Value::Bool(true))],
                },
                alias: None,
                join: None,
            }],
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![AstResultColumn {
                expr: AstExpr::Column(AstColumnRef { table: "customers".into(), column: "id".into() }),
                alias: None,
            }],
        };

        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );
        let mut ctx = make_plan_context(&schemas, &registry);

        let err = build_raw_plan(&ast, &mut ctx).unwrap_err();
        assert!(
            matches!(&err, PlanError::UnsupportedExpr(msg) if msg.contains("bool")),
            "got {err:?}"
        );
    }

    #[test]
    fn unknown_row_table_rejected() {
        // Registry claims row_table "ghosts" but it's not in table_schemas.
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::missing".into(),
            meta("ghosts", vec![("id", DataType::I64)]),
        );

        let err = run(
            "SELECT customers.id FROM customers.missing(1)",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(&err, PlanError::UnknownTable(t) if t == "ghosts"),
            "got {err:?}"
        );
    }

    #[test]
    fn call_with_uuid_literal_typechecks_against_uuid_param() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_id".into(),
            meta("customers", vec![("external_id", DataType::Uuid)]),
        );

        let (plan, bound) = run(
            "SELECT customers.id FROM customers.by_id(UUID '550e8400-e29b-41d4-a716-446655440000')",
            &schemas,
            &registry,
        )
        .expect("plan");

        let PlanSource::Requirement { args, .. } = &plan.sources[0].source else {
            panic!("expected Requirement source");
        };
        match &args[0] {
            RequirementArg::Placeholder(name) => {
                assert_eq!(name, "__caller_0_arg_0");
                let bound_val = bound.get(name).unwrap();
                let expected = sql_parser::uuid::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
                assert!(matches!(bound_val, Value::Uuid(b) if *b == expected));
            }
        }
    }

    #[test]
    fn call_uuid_literal_against_i64_param_is_type_mismatch() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_id".into(),
            meta("customers", vec![("external_id", DataType::I64)]),
        );

        let err = run(
            "SELECT customers.id FROM customers.by_id(UUID '550e8400-e29b-41d4-a716-446655440000')",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(
                &err,
                PlanError::CallerArgTypeMismatch { id, arg_idx: 0, .. }
                    if id == "customers::by_id"
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn call_int_literal_against_uuid_param_is_type_mismatch() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_id".into(),
            meta("customers", vec![("external_id", DataType::Uuid)]),
        );

        let err = run(
            "SELECT customers.id FROM customers.by_id(42)",
            &schemas,
            &registry,
        )
        .unwrap_err();

        assert!(
            matches!(
                &err,
                PlanError::CallerArgTypeMismatch { id, arg_idx: 0, .. }
                    if id == "customers::by_id"
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn call_with_uuid_placeholder_passes_through_without_typecheck() {
        // User placeholders aren't type-checked at plan time — they're
        // resolved at bind time. Only the registry shape matters.
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_id".into(),
            meta("customers", vec![("external_id", DataType::Uuid)]),
        );

        let (plan, bound) = run(
            "SELECT customers.id FROM customers.by_id(:external_id)",
            &schemas,
            &registry,
        )
        .expect("plan");

        let PlanSource::Requirement { args, .. } = &plan.sources[0].source else {
            panic!("expected Requirement source");
        };
        match &args[0] {
            RequirementArg::Placeholder(name) => assert_eq!(name, "external_id"),
        }
        assert!(bound.is_empty(), "user placeholders must not bind values");
    }

    #[test]
    fn call_with_mixed_uuid_str_int_args() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::find".into(),
            RequirementMeta {
                row_table: "customers".into(),
                params: vec![
                    RequirementParamDef { name: "id".into(), data_type: DataType::Uuid },
                    RequirementParamDef { name: "name".into(), data_type: DataType::String },
                    RequirementParamDef { name: "min_age".into(), data_type: DataType::I64 },
                ],
            },
        );

        let (plan, bound) = run(
            "SELECT customers.id FROM customers.find(\
                UUID '550e8400-e29b-41d4-a716-446655440000', \
                'Alice', \
                30\
             )",
            &schemas,
            &registry,
        )
        .expect("plan");

        let PlanSource::Requirement { args, .. } = &plan.sources[0].source else {
            panic!("expected Requirement source");
        };
        assert_eq!(args.len(), 3);
        let names: Vec<&str> = args.iter().map(|a| match a {
            RequirementArg::Placeholder(n) => n.as_str(),
        }).collect();
        let expected_uuid = sql_parser::uuid::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert!(matches!(bound.get(names[0]), Some(Value::Uuid(b)) if *b == expected_uuid));
        assert!(matches!(bound.get(names[1]), Some(Value::Text(s)) if s == "Alice"));
        assert!(matches!(bound.get(names[2]), Some(Value::Int(30))));
    }

    #[test]
    fn pretty_print_requirement_source() {
        let schemas = schemas_with(&[customers_schema()]);
        let mut registry = RequirementRegistry::new();
        registry.insert(
            "customers::by_owner".into(),
            meta("customers", vec![("owner_id", DataType::I64)]),
        );

        let (plan, _) = run(
            "SELECT customers.name FROM customers.by_owner(42)",
            &schemas,
            &registry,
        )
        .expect("plan");

        let mut rendered = String::new();
        plan.pretty_print_to(&mut rendered, 0);
        assert!(
            rendered.contains("caller=customers::by_owner"),
            "missing caller id: {rendered}"
        );
        assert!(
            rendered.contains("row=customers"),
            "missing row table: {rendered}"
        );
        assert!(
            rendered.contains(":__caller_0_arg_0"),
            "missing placeholder: {rendered}"
        );
    }
}
