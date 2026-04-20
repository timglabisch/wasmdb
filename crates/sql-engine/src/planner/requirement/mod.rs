//! Requirement planning: what a query needs *before* it can be executed.
//!
//! Orthogonal to the SQL and Reactive planners — a `RequirementPlan` is
//! produced directly from the AST and lists external work the caller
//! must discharge before a result can be computed.
//!
//! Today the only variant is `Fetcher` (an HTTP-backed row delivery for
//! a `schema.function(args)` FROM-clause source). The enum is open so
//! future requirement kinds (cache warmups, external lookups, …) can
//! slot in without churn.

use sql_parser::ast;

use crate::planner::PlanError;
use crate::storage::CellValue;

/// Full requirement set for a single SELECT.
#[derive(Debug, Clone)]
pub struct RequirementPlan {
    pub requirements: Vec<Requirement>,
}

/// One requirement. Open-ended enum; today only fetcher calls.
#[derive(Debug, Clone)]
pub enum Requirement {
    Fetcher(FetcherRequirement),
}

/// A fetcher call extracted from a FROM-clause `schema.function(args)` source.
#[derive(Debug, Clone)]
pub struct FetcherRequirement {
    /// Wire-ID used to dispatch the HTTP call, shaped `"{schema}::{function}"`.
    pub fetcher_id: String,
    /// Logical row table the fetcher populates — currently taken verbatim
    /// from the schema part of the call.
    pub row_table: String,
    /// Const-evaluated arguments in declaration order.
    pub args: Vec<CellValue>,
}

/// Extract requirements from an AST. Walks top-level sources only.
pub fn plan_requirements(ast: &ast::AstSelect) -> Result<RequirementPlan, PlanError> {
    let mut requirements = Vec::new();
    for entry in &ast.sources {
        if let ast::AstSource::Call { schema, function, args } = &entry.source {
            let arg_values = args
                .iter()
                .enumerate()
                .map(|(i, expr)| const_eval_literal(expr, schema, function, i))
                .collect::<Result<Vec<_>, _>>()?;
            requirements.push(Requirement::Fetcher(FetcherRequirement {
                fetcher_id: format!("{schema}::{function}"),
                row_table: schema.clone(),
                args: arg_values,
            }));
        }
    }
    Ok(RequirementPlan { requirements })
}

/// Collapse a single AST literal into a `CellValue`. Non-literals, floats,
/// bools, and placeholders are rejected — MVP keeps fetcher args strictly
/// concrete.
fn const_eval_literal(
    expr: &ast::AstExpr,
    schema: &str,
    function: &str,
    arg_idx: usize,
) -> Result<CellValue, PlanError> {
    let lit = match expr {
        ast::AstExpr::Literal(v) => v,
        _ => {
            return Err(PlanError::UnsupportedExpr(format!(
                "fetcher `{schema}.{function}(...)` arg {arg_idx}: only literal arguments are supported"
            )));
        }
    };
    match lit {
        ast::Value::Int(n) => Ok(CellValue::I64(*n)),
        ast::Value::Text(s) => Ok(CellValue::Str(s.clone())),
        ast::Value::Null => Ok(CellValue::Null),
        other => Err(PlanError::UnsupportedExpr(format!(
            "fetcher `{schema}.{function}(...)` arg {arg_idx}: unsupported literal {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_parser::ast::*;

    fn lit_int(n: i64) -> AstExpr {
        AstExpr::Literal(Value::Int(n))
    }

    fn lit_text(s: &str) -> AstExpr {
        AstExpr::Literal(Value::Text(s.into()))
    }

    fn table(name: &str) -> AstSourceEntry {
        AstSourceEntry {
            source: AstSource::Table(name.into()),
            alias: None,
            join: None,
        }
    }

    fn call(schema: &str, function: &str, args: Vec<AstExpr>) -> AstSourceEntry {
        AstSourceEntry {
            source: AstSource::Call {
                schema: schema.into(),
                function: function.into(),
                args,
            },
            alias: None,
            join: None,
        }
    }

    fn join_of(entry: AstSourceEntry) -> AstSourceEntry {
        AstSourceEntry {
            join: Some(AstJoinClause {
                join_type: JoinType::Inner,
                on: vec![],
            }),
            ..entry
        }
    }

    fn select_with(sources: Vec<AstSourceEntry>) -> AstSelect {
        AstSelect {
            sources,
            filter: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        }
    }

    fn expect_fetcher(req: &Requirement) -> &FetcherRequirement {
        match req {
            Requirement::Fetcher(f) => f,
        }
    }

    #[test]
    fn no_call_no_requirements() {
        let ast = select_with(vec![table("users")]);
        let plan = plan_requirements(&ast).unwrap();
        assert!(plan.requirements.is_empty());
    }

    #[test]
    fn single_fetcher_call() {
        let ast = select_with(vec![call("customers", "by_owner", vec![lit_int(42)])]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 1);
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f.fetcher_id, "customers::by_owner");
        assert_eq!(f.row_table, "customers");
        assert_eq!(f.args, vec![CellValue::I64(42)]);
    }

    #[test]
    fn fetcher_in_join_position() {
        let ast = select_with(vec![
            table("users"),
            join_of(call("invoices", "by_customer", vec![lit_int(42)])),
        ]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 1);
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f.fetcher_id, "invoices::by_customer");
        assert_eq!(f.row_table, "invoices");
    }

    #[test]
    fn two_fetchers_in_one_select() {
        let ast = select_with(vec![
            call("a", "f", vec![lit_int(1)]),
            join_of(call("b", "g", vec![lit_int(2)])),
        ]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 2);
        assert_eq!(expect_fetcher(&plan.requirements[0]).fetcher_id, "a::f");
        assert_eq!(expect_fetcher(&plan.requirements[1]).fetcher_id, "b::g");
    }

    #[test]
    fn string_literal_arg() {
        let ast = select_with(vec![call("contacts", "by_name", vec![lit_text("alice")])]);
        let plan = plan_requirements(&ast).unwrap();
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f.args, vec![CellValue::Str("alice".into())]);
    }

    #[test]
    fn null_literal_arg() {
        let ast = select_with(vec![call(
            "customers",
            "by_owner",
            vec![AstExpr::Literal(Value::Null)],
        )]);
        let plan = plan_requirements(&ast).unwrap();
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f.args, vec![CellValue::Null]);
    }

    #[test]
    fn multiple_args_preserve_order() {
        let ast = select_with(vec![call(
            "a",
            "f",
            vec![lit_int(1), lit_text("x"), lit_int(3)],
        )]);
        let plan = plan_requirements(&ast).unwrap();
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(
            f.args,
            vec![
                CellValue::I64(1),
                CellValue::Str("x".into()),
                CellValue::I64(3),
            ]
        );
    }

    #[test]
    fn non_literal_arg_rejected() {
        let ast = select_with(vec![call(
            "customers",
            "by_owner",
            vec![AstExpr::Column(AstColumnRef {
                table: "".into(),
                column: "x".into(),
            })],
        )]);
        let err = plan_requirements(&ast).unwrap_err();
        match err {
            PlanError::UnsupportedExpr(msg) => {
                assert!(msg.contains("by_owner"));
                assert!(msg.contains("literal"));
            }
            other => panic!("expected UnsupportedExpr, got {other:?}"),
        }
    }

    #[test]
    fn placeholder_arg_rejected() {
        let ast = select_with(vec![call(
            "customers",
            "by_owner",
            vec![AstExpr::Literal(Value::Placeholder("owner_id".into()))],
        )]);
        let err = plan_requirements(&ast).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }

    #[test]
    fn float_arg_rejected() {
        let ast = select_with(vec![call(
            "x",
            "y",
            vec![AstExpr::Literal(Value::Float(1.5))],
        )]);
        let err = plan_requirements(&ast).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }

    #[test]
    fn bool_arg_rejected() {
        let ast = select_with(vec![call(
            "x",
            "y",
            vec![AstExpr::Literal(Value::Bool(true))],
        )]);
        let err = plan_requirements(&ast).unwrap_err();
        assert!(matches!(err, PlanError::UnsupportedExpr(_)));
    }

    #[test]
    fn plain_tables_and_call_mixed() {
        let ast = select_with(vec![
            table("users"),
            join_of(call("invoices", "by_customer", vec![lit_int(7)])),
            join_of(table("contacts")),
        ]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 1);
        let f = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f.fetcher_id, "invoices::by_customer");
    }

    /// End-to-end: real SQL string → parser → requirement plan. Exercises
    /// multiple fetchers with heterogeneous argument types, `AS` aliases,
    /// an `INNER JOIN` onto another fetcher, a plain-table join, and a
    /// `WHERE` clause — none of which the requirement planner cares about,
    /// but they must all pass through without disturbing the extracted set.
    #[test]
    fn complex_multi_fetcher_query_from_sql() {
        let sql = "\
            SELECT c.name, i.number \
            FROM customers.by_owner(42, 'active') AS c \
            INNER JOIN invoices.by_customer(42) AS i ON c.id = i.customer_id \
            INNER JOIN products AS p ON p.id = i.product_id \
            WHERE c.name = 'alice'\
        ";
        let ast = sql_parser::parser::parse(sql).expect("parse");

        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(
            plan.requirements.len(),
            2,
            "one requirement per FROM/JOIN fetcher, plain tables ignored"
        );

        let f0 = expect_fetcher(&plan.requirements[0]);
        assert_eq!(f0.fetcher_id, "customers::by_owner");
        assert_eq!(f0.row_table, "customers");
        assert_eq!(
            f0.args,
            vec![CellValue::I64(42), CellValue::Str("active".into())],
            "mixed-type args preserve order and literal kind"
        );

        let f1 = expect_fetcher(&plan.requirements[1]);
        assert_eq!(f1.fetcher_id, "invoices::by_customer");
        assert_eq!(f1.row_table, "invoices");
        assert_eq!(f1.args, vec![CellValue::I64(42)]);
    }
}
