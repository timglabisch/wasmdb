//! AST → raw PlanSelect translation.
//!
//! Translates AST expressions into plan predicates, resolves column references,
//! and delegates subquery handling to [`super::materialize`].

use sql_parser::ast;

use super::plan::*;
use crate::planner::sql::materialize;
use crate::planner::PlanContext;
use crate::planner::PlanError;

/// Build a raw PlanSelect from an AST (before optimization passes).
pub fn build_raw_plan(
    select: &ast::AstSelect,
    ctx: &mut PlanContext,
) -> Result<PlanSelect, PlanError> {
    if select.sources.is_empty() {
        return Err(PlanError::EmptySources);
    }

    let mut sources = Vec::new();

    for entry in &select.sources {
        let table_name = match &entry.source {
            ast::AstSource::Table(t) => t,
            ast::AstSource::Call { schema, function, .. } => {
                return Err(PlanError::UnsupportedExpr(format!(
                    "function-call source `{schema}.{function}(...)` not yet supported by the planner",
                )));
            }
        };
        if entry.alias.is_some() {
            return Err(PlanError::UnsupportedExpr(
                "FROM-clause alias (`AS name`) not yet supported".into(),
            ));
        }
        let table_schema = ctx.query_schemas
            .get(table_name)
            .ok_or_else(|| PlanError::UnknownTable(table_name.clone()))?
            .clone();

        sources.push(PlanSourceEntry {
            source: PlanSource::Table {
                name: table_name.clone(),
                schema: table_schema,
                scan_method: PlanScanMethod::Full,
            },
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
