//! Parameter binding: resolves Value::Placeholder in a plan using provided params.

use sql_parser::ast::Value;

use crate::planner::shared::plan::*;
use crate::planner::sql::plan::ExecutionPlan;
use super::{ExecuteError, Params, ParamValue};

/// Resolve all placeholders in an ExecutionPlan. Returns the plan unchanged if params is empty.
pub fn resolve_plan_params(
    plan: &ExecutionPlan,
    params: &Params,
) -> Result<ExecutionPlan, ExecuteError> {
    if params.is_empty() {
        return Ok(plan.clone());
    }
    let mut resolved = plan.clone();
    resolved.main = resolve_params(&plan.main, params)?;
    for step in &mut resolved.materializations {
        step.plan = resolve_params(&step.plan, params)?;
    }
    Ok(resolved)
}


/// Resolve all Value::Placeholder in a PlanSelect using the given params.
pub fn resolve_params(
    plan: &PlanSelect,
    params: &Params,
) -> Result<PlanSelect, ExecuteError> {
    let mut resolved = plan.clone();

    resolved.filter = resolve_filter(&resolved.filter, params)?;

    for source in &mut resolved.sources {
        source.pre_filter = resolve_filter(&source.pre_filter, params)?;
        if let Some(ref mut join) = source.join {
            join.on = resolve_filter(&join.on, params)?;
        }
        let PlanSource::Table { scan_method, .. } = &mut source.source;
        if let PlanScanMethod::Index { index_predicates, .. } = scan_method {
            for pred in index_predicates.iter_mut() {
                *pred = resolve_filter(pred, params)?;
            }
        }
    }

    if let Some(PlanLimit::Placeholder(ref name)) = resolved.limit {
        match params.get(name) {
            Some(ParamValue::Int(n)) => {
                resolved.limit = Some(PlanLimit::Value(*n as usize));
            }
            Some(_) => return Err(ExecuteError::BindError(
                format!("LIMIT placeholder :{name} must be Int"),
            )),
            None => return Err(ExecuteError::BindError(
                format!("missing parameter :{name}"),
            )),
        }
    }

    Ok(resolved)
}

pub fn resolve_filter(
    pred: &PlanFilterPredicate,
    params: &Params,
) -> Result<PlanFilterPredicate, ExecuteError> {
    match pred {
        PlanFilterPredicate::Equals { col, value } =>
            Ok(PlanFilterPredicate::Equals { col: *col, value: resolve_value(value, params)? }),
        PlanFilterPredicate::NotEquals { col, value } =>
            Ok(PlanFilterPredicate::NotEquals { col: *col, value: resolve_value(value, params)? }),
        PlanFilterPredicate::GreaterThan { col, value } =>
            Ok(PlanFilterPredicate::GreaterThan { col: *col, value: resolve_value(value, params)? }),
        PlanFilterPredicate::GreaterThanOrEqual { col, value } =>
            Ok(PlanFilterPredicate::GreaterThanOrEqual { col: *col, value: resolve_value(value, params)? }),
        PlanFilterPredicate::LessThan { col, value } =>
            Ok(PlanFilterPredicate::LessThan { col: *col, value: resolve_value(value, params)? }),
        PlanFilterPredicate::LessThanOrEqual { col, value } =>
            Ok(PlanFilterPredicate::LessThanOrEqual { col: *col, value: resolve_value(value, params)? }),

        PlanFilterPredicate::In { col, values } => {
            // Single placeholder → expect a list parameter
            if values.len() == 1 {
                if let Value::Placeholder(name) = &values[0] {
                    return match params.get(name) {
                        Some(ParamValue::IntList(list)) => {
                            Ok(PlanFilterPredicate::In {
                                col: *col,
                                values: list.iter().map(|n| Value::Int(*n)).collect(),
                            })
                        }
                        Some(ParamValue::TextList(list)) => {
                            Ok(PlanFilterPredicate::In {
                                col: *col,
                                values: list.iter().map(|s| Value::Text(s.clone())).collect(),
                            })
                        }
                        Some(ParamValue::UuidList(list)) => {
                            Ok(PlanFilterPredicate::In {
                                col: *col,
                                values: list.iter().map(|b| Value::Uuid(*b)).collect(),
                            })
                        }
                        Some(_) => Err(ExecuteError::BindError(
                            format!("IN(:{name}) requires IntList, TextList or UuidList parameter"),
                        )),
                        None => Err(ExecuteError::BindError(
                            format!("missing parameter :{name}"),
                        )),
                    };
                }
            }
            // Normal literal list (no placeholders, or mixed — resolve each)
            let resolved_values: Vec<Value> = values.iter()
                .map(|v| resolve_value(v, params))
                .collect::<Result<_, _>>()?;
            Ok(PlanFilterPredicate::In { col: *col, values: resolved_values })
        }

        PlanFilterPredicate::And(l, r) => Ok(PlanFilterPredicate::And(
            Box::new(resolve_filter(l, params)?),
            Box::new(resolve_filter(r, params)?),
        )),
        PlanFilterPredicate::Or(l, r) => Ok(PlanFilterPredicate::Or(
            Box::new(resolve_filter(l, params)?),
            Box::new(resolve_filter(r, params)?),
        )),

        // Variants without Value stay unchanged
        other => Ok(other.clone()),
    }
}

pub fn resolve_value(value: &Value, params: &Params) -> Result<Value, ExecuteError> {
    match value {
        Value::Placeholder(name) => match params.get(name) {
            Some(ParamValue::Int(n)) => Ok(Value::Int(*n)),
            Some(ParamValue::Text(s)) => Ok(Value::Text(s.clone())),
            Some(ParamValue::Uuid(b)) => Ok(Value::Uuid(*b)),
            Some(ParamValue::Null) => Ok(Value::Null),
            Some(ParamValue::IntList(_) | ParamValue::TextList(_) | ParamValue::UuidList(_)) => {
                Err(ExecuteError::BindError(
                    format!("parameter :{name} is a list, but scalar expected"),
                ))
            }
            None => Err(ExecuteError::BindError(
                format!("missing parameter :{name}"),
            )),
        },
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::shared::plan::ColumnRef;
    use std::collections::HashMap;

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    fn empty_plan(filter: PlanFilterPredicate) -> PlanSelect {
        PlanSelect {
            sources: vec![],
            filter,
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        }
    }

    #[test]
    fn test_bind_scalar_int() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("id".into()),
        });
        let params = HashMap::from([("id".into(), ParamValue::Int(42))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Int(42), .. }
        ));
    }

    #[test]
    fn test_bind_scalar_text() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("name".into()),
        });
        let params = HashMap::from([("name".into(), ParamValue::Text("Alice".into()))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Text(ref s), .. } if s == "Alice"
        ));
    }

    #[test]
    fn test_bind_null() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("val".into()),
        });
        let params = HashMap::from([("val".into(), ParamValue::Null)]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Null, .. }
        ));
    }

    #[test]
    fn test_bind_int_list_for_in() {
        let plan = empty_plan(PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Placeholder("ids".into())],
        });
        let params = HashMap::from([("ids".into(), ParamValue::IntList(vec![1, 2, 3]))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        match &resolved.filter {
            PlanFilterPredicate::In { values, .. } => {
                assert_eq!(values, &[Value::Int(1), Value::Int(2), Value::Int(3)]);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn test_bind_text_list_for_in() {
        let plan = empty_plan(PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Placeholder("names".into())],
        });
        let params = HashMap::from([("names".into(), ParamValue::TextList(vec!["a".into(), "b".into()]))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        match &resolved.filter {
            PlanFilterPredicate::In { values, .. } => {
                assert_eq!(values, &[Value::Text("a".into()), Value::Text("b".into())]);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn test_bind_missing_param_error() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("missing".into()),
        });
        let params = HashMap::new();
        let err = resolve_params(&plan, &params).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(ref msg) if msg.contains("missing")));
    }

    #[test]
    fn test_bind_list_where_scalar_expected_error() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("val".into()),
        });
        let params = HashMap::from([("val".into(), ParamValue::IntList(vec![1, 2]))]);
        let err = resolve_params(&plan, &params).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(ref msg) if msg.contains("list")));
    }

    #[test]
    fn test_bind_scalar_where_list_expected_error() {
        let plan = empty_plan(PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Placeholder("val".into())],
        });
        let params = HashMap::from([("val".into(), ParamValue::Int(42))]);
        let err = resolve_params(&plan, &params).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(ref msg) if msg.contains("List")));
    }

    #[test]
    fn test_bind_limit_placeholder() {
        let mut plan = empty_plan(PlanFilterPredicate::None);
        plan.limit = Some(PlanLimit::Placeholder("n".into()));
        let params = HashMap::from([("n".into(), ParamValue::Int(10))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert_eq!(resolved.limit, Some(PlanLimit::Value(10)));
    }

    #[test]
    fn test_bind_unused_param_ignored() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("id".into()),
        });
        let params = HashMap::from([
            ("id".into(), ParamValue::Int(1)),
            ("unused".into(), ParamValue::Text("ignored".into())),
        ]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Int(1), .. }
        ));
    }

    #[test]
    fn test_bind_same_param_multiple_predicates() {
        let plan = empty_plan(PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals {
                col: c(0, 0),
                value: Value::Placeholder("val".into()),
            }),
            Box::new(PlanFilterPredicate::GreaterThan {
                col: c(0, 1),
                value: Value::Placeholder("val".into()),
            }),
        ));
        let params = HashMap::from([("val".into(), ParamValue::Int(5))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        match &resolved.filter {
            PlanFilterPredicate::And(l, r) => {
                assert!(matches!(l.as_ref(), PlanFilterPredicate::Equals { value: Value::Int(5), .. }));
                assert!(matches!(r.as_ref(), PlanFilterPredicate::GreaterThan { value: Value::Int(5), .. }));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_bind_scalar_uuid() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("id".into()),
        });
        let u = sql_parser::uuid::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let params = HashMap::from([("id".into(), ParamValue::Uuid(u))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Uuid(b), .. } if b == u
        ));
    }

    #[test]
    fn test_bind_uuid_list_for_in() {
        let plan = empty_plan(PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Placeholder("ids".into())],
        });
        let a = sql_parser::uuid::parse_uuid("00000000-0000-0000-0000-000000000001").unwrap();
        let b = sql_parser::uuid::parse_uuid("00000000-0000-0000-0000-000000000002").unwrap();
        let params = HashMap::from([("ids".into(), ParamValue::UuidList(vec![a, b]))]);
        let resolved = resolve_params(&plan, &params).unwrap();
        match &resolved.filter {
            PlanFilterPredicate::In { values, .. } => {
                assert_eq!(values, &[Value::Uuid(a), Value::Uuid(b)]);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn test_bind_uuid_list_where_scalar_expected_error() {
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("val".into()),
        });
        let a = sql_parser::uuid::parse_uuid("00000000-0000-0000-0000-000000000001").unwrap();
        let params = HashMap::from([("val".into(), ParamValue::UuidList(vec![a]))]);
        let err = resolve_params(&plan, &params).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(ref msg) if msg.contains("list")));
    }

    #[test]
    fn test_bind_mixed_param_types() {
        // Same plan, params map contains Int + Uuid + Null + UuidList — only
        // the referenced placeholder gets resolved, others are ignored.
        let plan = empty_plan(PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Placeholder("u".into()),
        });
        let u = sql_parser::uuid::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let params = HashMap::from([
            ("n".into(), ParamValue::Int(42)),
            ("u".into(), ParamValue::Uuid(u)),
            ("missing".into(), ParamValue::Null),
            ("ids".into(), ParamValue::UuidList(vec![u])),
        ]);
        let resolved = resolve_params(&plan, &params).unwrap();
        assert!(matches!(
            resolved.filter,
            PlanFilterPredicate::Equals { value: Value::Uuid(b), .. } if b == u
        ));
    }

    #[test]
    fn test_bind_limit_text_param_error() {
        let mut plan = empty_plan(PlanFilterPredicate::None);
        plan.limit = Some(PlanLimit::Placeholder("n".into()));
        let params = HashMap::from([("n".into(), ParamValue::Text("abc".into()))]);
        let err = resolve_params(&plan, &params).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(ref msg) if msg.contains("LIMIT")));
    }
}
