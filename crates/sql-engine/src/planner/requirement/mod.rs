//! Requirement planning: what a query needs *before* it can be executed.
//!
//! Orthogonal to the SQL and Reactive planners — a `RequirementPlan` is
//! produced directly from the AST and lists external work the caller
//! must discharge before a result can be computed.
//!
//! Today the only variant is `Caller` (an HTTP-backed row delivery for
//! a `schema.function(args)` FROM-clause source). The enum is open so
//! future requirement kinds (cache warmups, external lookups, …) can
//! slot in without churn.

pub mod registry;

pub use registry::{RequirementMeta, RequirementParamDef, RequirementRegistry};

use sql_parser::ast;

use crate::planner::PlanError;
use crate::storage::CellValue;

/// Full requirement set for a single SELECT.
#[derive(Debug, Clone)]
pub struct RequirementPlan {
    pub requirements: Vec<Requirement>,
}

impl RequirementPlan {
    /// Render the plan in a compact human-readable form. Output is stable
    /// enough to assert against directly in tests.
    pub fn pretty_print(&self) -> String {
        let mut out = String::new();
        if self.requirements.is_empty() {
            out.push_str("RequirementPlan (no requirements)\n");
            return out;
        }
        out.push_str(&format!(
            "RequirementPlan ({} requirements)\n",
            self.requirements.len()
        ));
        for (i, req) in self.requirements.iter().enumerate() {
            match req {
                Requirement::Caller(f) => {
                    let args = f
                        .args
                        .iter()
                        .map(fmt_cell)
                        .collect::<Vec<_>>()
                        .join(", ");
                    out.push_str(&format!(
                        "  [{i}] Caller {}({args}) row={}\n",
                        f.caller_id, f.row_table
                    ));
                }
            }
        }
        out
    }
}

fn fmt_cell(v: &CellValue) -> String {
    match v {
        CellValue::I64(n) => n.to_string(),
        CellValue::Str(s) => format!("'{s}'"),
        CellValue::Null => "NULL".to_string(),
    }
}

/// One requirement. Open-ended enum; today only caller invocations.
#[derive(Debug, Clone)]
pub enum Requirement {
    Caller(CallerRequirement),
}

/// A caller invocation extracted from a FROM-clause `schema.function(args)` source.
#[derive(Debug, Clone)]
pub struct CallerRequirement {
    /// Wire-ID used to dispatch the HTTP call, shaped `"{schema}::{function}"`.
    pub caller_id: String,
    /// Logical row table the caller populates — currently taken verbatim
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
            requirements.push(Requirement::Caller(CallerRequirement {
                caller_id: format!("{schema}::{function}"),
                row_table: schema.clone(),
                args: arg_values,
            }));
        }
    }
    Ok(RequirementPlan { requirements })
}

/// Collapse a single AST literal into a `CellValue`. Non-literals, floats,
/// bools, and placeholders are rejected — MVP keeps caller args strictly
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
                "caller `{schema}.{function}(...)` arg {arg_idx}: only literal arguments are supported"
            )));
        }
    };
    match lit {
        ast::Value::Int(n) => Ok(CellValue::I64(*n)),
        ast::Value::Text(s) => Ok(CellValue::Str(s.clone())),
        ast::Value::Null => Ok(CellValue::Null),
        other => Err(PlanError::UnsupportedExpr(format!(
            "caller `{schema}.{function}(...)` arg {arg_idx}: unsupported literal {other:?}"
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

    fn expect_caller(req: &Requirement) -> &CallerRequirement {
        match req {
            Requirement::Caller(f) => f,
        }
    }

    #[test]
    fn no_call_no_requirements() {
        let ast = select_with(vec![table("users")]);
        let plan = plan_requirements(&ast).unwrap();
        assert!(plan.requirements.is_empty());
    }

    #[test]
    fn single_caller_call() {
        let ast = select_with(vec![call("customers", "by_owner", vec![lit_int(42)])]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 1);
        let f = expect_caller(&plan.requirements[0]);
        assert_eq!(f.caller_id, "customers::by_owner");
        assert_eq!(f.row_table, "customers");
        assert_eq!(f.args, vec![CellValue::I64(42)]);
    }

    #[test]
    fn caller_in_join_position() {
        let ast = select_with(vec![
            table("users"),
            join_of(call("invoices", "by_customer", vec![lit_int(42)])),
        ]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 1);
        let f = expect_caller(&plan.requirements[0]);
        assert_eq!(f.caller_id, "invoices::by_customer");
        assert_eq!(f.row_table, "invoices");
    }

    #[test]
    fn two_callers_in_one_select() {
        let ast = select_with(vec![
            call("a", "f", vec![lit_int(1)]),
            join_of(call("b", "g", vec![lit_int(2)])),
        ]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.requirements.len(), 2);
        assert_eq!(expect_caller(&plan.requirements[0]).caller_id, "a::f");
        assert_eq!(expect_caller(&plan.requirements[1]).caller_id, "b::g");
    }

    #[test]
    fn string_literal_arg() {
        let ast = select_with(vec![call("contacts", "by_name", vec![lit_text("alice")])]);
        let plan = plan_requirements(&ast).unwrap();
        let f = expect_caller(&plan.requirements[0]);
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
        let f = expect_caller(&plan.requirements[0]);
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
        let f = expect_caller(&plan.requirements[0]);
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
        let f = expect_caller(&plan.requirements[0]);
        assert_eq!(f.caller_id, "invoices::by_customer");
    }

    /// End-to-end: real SQL string → parser → requirement plan. Exercises
    /// multiple callers with heterogeneous argument types, `AS` aliases,
    /// an `INNER JOIN` onto another caller, a plain-table join, and a
    /// `WHERE` clause — none of which the requirement planner cares about,
    /// but they must all pass through without disturbing the extracted set.
    #[test]
    fn complex_multi_caller_query_from_sql() {
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
            "one requirement per FROM/JOIN caller, plain tables ignored"
        );

        let f0 = expect_caller(&plan.requirements[0]);
        assert_eq!(f0.caller_id, "customers::by_owner");
        assert_eq!(f0.row_table, "customers");
        assert_eq!(
            f0.args,
            vec![CellValue::I64(42), CellValue::Str("active".into())],
            "mixed-type args preserve order and literal kind"
        );

        let f1 = expect_caller(&plan.requirements[1]);
        assert_eq!(f1.caller_id, "invoices::by_customer");
        assert_eq!(f1.row_table, "invoices");
        assert_eq!(f1.args, vec![CellValue::I64(42)]);

        let expected = "\
RequirementPlan (2 requirements)
  [0] Caller customers::by_owner(42, 'active') row=customers
  [1] Caller invoices::by_customer(42) row=invoices
";
        assert_eq!(plan.pretty_print(), expected);
    }

    #[test]
    fn pretty_print_empty_plan() {
        let ast = select_with(vec![table("users")]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(plan.pretty_print(), "RequirementPlan (no requirements)\n");
    }

    /// Integration: a query that uses a *caller-call* FROM-source
    /// (`orders.fetch_by_user(42)`) — routed through all three planners so
    /// you can see how each one reacts to the same input:
    ///
    /// - `RequirementPlan` picks the caller up and lists it as external work.
    /// - `ExecutionPlan` (SQL) resolves the caller against its
    ///   `RequirementRegistry`; with an empty registry the planner
    ///   rejects with `UnknownRequirement`.
    /// - `ReactivePlan` shares the SQL translation step and therefore
    ///   rejects with the same error.
    ///
    /// Asserting the exact outputs pins the current division of labor down:
    /// the requirement layer owns caller metadata; the SQL/Reactive planners
    /// look the caller up but produce no rows without a registered entry.
    #[test]
    fn complex_query_all_three_plans() {
        use std::collections::HashMap;
        use crate::schema::{ColumnSchema, DataType, TableSchema};
        use crate::planner::{sql, reactive};

        let sql_text = "\
            SELECT orders.amount, REACTIVE(orders.user_id = 42) AS inv \
            FROM orders.fetch_by_user(42) \
            WHERE orders.amount > 100 \
            ORDER BY orders.amount DESC \
            LIMIT 10\
        ";
        let ast = sql_parser::parser::parse(sql_text).expect("parse");

        // Plain-table schema registered under the same name as the caller
        // call's schema part. RequirementPlan doesn't need this, but we keep
        // the shape realistic.
        let mut schemas: HashMap<String, TableSchema> = HashMap::new();
        schemas.insert("orders".into(), TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        });

        // (1) RequirementPlan — succeeds; picks up the caller.
        let req_plan = plan_requirements(&ast).unwrap();
        assert_eq!(req_plan.requirements.len(), 1);

        // (2) ExecutionPlan — SQL planner rejects call sources today.
        let empty_requirements = RequirementRegistry::new();
        let exec_err = sql::plan(&ast, &schemas, &empty_requirements).unwrap_err();

        // (3) ReactivePlan — goes through the same SQL translation step, so
        // it rejects too (identical error surface).
        let reactive_err = reactive::plan_reactive(&ast, &schemas, &empty_requirements).unwrap_err();

        let rendered = format!(
            "=== RequirementPlan ===\n{}\
=== ExecutionPlan (error) ===\n{exec_err}\n\
=== ReactivePlan (error) ===\n{reactive_err}\n",
            req_plan.pretty_print(),
        );

        let expected = "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller orders::fetch_by_user(42) row=orders
=== ExecutionPlan (error) ===
unknown requirement: orders::fetch_by_user
=== ReactivePlan (error) ===
unknown requirement: orders::fetch_by_user
";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn pretty_print_null_and_string_args() {
        let ast = select_with(vec![call(
            "x",
            "y",
            vec![AstExpr::Literal(Value::Null), lit_text("hi")],
        )]);
        let plan = plan_requirements(&ast).unwrap();
        assert_eq!(
            plan.pretty_print(),
            "RequirementPlan (1 requirements)\n  [0] Caller x::y(NULL, 'hi') row=x\n",
        );
    }
}
