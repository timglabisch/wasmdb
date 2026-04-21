//! Core execution pipeline: scan -> join -> filter -> aggregate -> sort -> limit -> project.

use crate::planner::shared::plan::*;

use super::{aggregate, join, project, scan, sort};
use super::{Columns, ExecuteError, ExecutionContext, SpanOperation};

pub fn execute(
    ctx: &mut ExecutionContext,
    plan: &PlanSelect,
) -> Result<Columns, ExecuteError> {
    ctx.span(SpanOperation::Execute, |ctx| execute_inner(ctx, plan))
}

fn execute_inner(
    ctx: &mut ExecutionContext,
    plan: &PlanSelect,
) -> Result<Columns, ExecuteError> {
    // Phase 1: Scan first source -> RowSet.
    let first = &plan.sources[0];
    let mut rs = match &first.source {
        PlanSource::Table { name, .. } => {
            let first_table = ctx.db
                .get(name)
                .ok_or_else(|| ExecuteError::TableNotFound(name.clone()))?;
            scan::scan(ctx, first_table, first)
        }
        PlanSource::Requirement { row_table, .. } => {
            let first_table = ctx.db
                .get(row_table)
                .ok_or_else(|| ExecuteError::TableNotFound(row_table.clone()))?;
            scan::scan_requirement(ctx, first_table, first)?
        }
    };

    // Phase 2: Join remaining sources.
    for (source_idx, source) in plan.sources.iter().enumerate().skip(1) {
        let (table_name, is_caller) = match &source.source {
            PlanSource::Table { name, .. } => (name.clone(), false),
            PlanSource::Requirement { row_table, .. } => (row_table.clone(), true),
        };
        let table = ctx.db
            .get(&table_name)
            .ok_or_else(|| ExecuteError::TableNotFound(table_name.clone()))?;
        match source.join.as_ref() {
            Some(j) => match &j.strategy {
                PlanJoinStrategy::NestedLoop => {
                    let right = if is_caller {
                        scan::scan_requirement(ctx, table, source)?
                    } else {
                        scan::scan(ctx, table, source)
                    };
                    rs = join::nested_loop_join(
                        ctx, &rs, right.tables[0], &right.row_ids[0],
                        source_idx, &j.on, j.join_type,
                    );
                }
                PlanJoinStrategy::IndexLookup { left_col, index_columns, .. } => {
                    if is_caller {
                        return Err(ExecuteError::NotImplemented(
                            "IndexLookup join strategy onto caller source not supported".into(),
                        ));
                    }
                    rs = join::index_nested_loop_join(
                        ctx, &rs, table,
                        j.join_type, *left_col,
                        index_columns,
                        &source.pre_filter,
                    );
                }
            },
            None => {
                let right = if is_caller {
                    scan::scan_requirement(ctx, table, source)?
                } else {
                    scan::scan(ctx, table, source)
                };
                rs = join::nested_loop_join(
                    ctx, &rs, right.tables[0], &right.row_ids[0],
                    source_idx, &PlanFilterPredicate::None, sql_parser::ast::JoinType::Inner,
                );
            }
        }
    }

    // Phase 3: Post-filter.
    if !matches!(plan.filter, PlanFilterPredicate::None) {
        rs = rs.filter(ctx, &plan.filter);
    }

    // Phase 4: Aggregate.
    if !plan.group_by.is_empty() || !plan.aggregates.is_empty() {
        let aggregated = aggregate::aggregate_rowset(ctx, &rs, &plan.group_by, &plan.aggregates);
        let mut result = project::project(ctx, &aggregated, &plan.result_columns, &plan.group_by);
        if !plan.order_by.is_empty() {
            sort::sort_columns(ctx, &mut result, &plan.order_by, &plan.result_columns);
        }
        if let Some(PlanLimit::Value(limit)) = plan.limit {
            for col in &mut result { col.truncate(limit); }
        }
        return Ok(result);
    }

    // Phase 5: Sort.
    if !plan.order_by.is_empty() {
        rs.sort(ctx, &plan.order_by);
    }

    // Phase 5b: Limit.
    if let Some(PlanLimit::Value(limit)) = plan.limit {
        if rs.num_rows > limit {
            for ids in &mut rs.row_ids { ids.truncate(limit); }
            rs.num_rows = limit;
        }
    }

    // Phase 6: Project.
    Ok(project::project_rowset(ctx, &rs, &plan.result_columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::storage::{CellValue, Table};
    use sql_parser::ast::*;
    use sql_parser::schema::{ColumnDef, Schema};
    use crate::schema::{ColumnSchema, DataType, TableSchema};
    use crate::execute::value_to_cell;

    /// Pre-populate `ctx.requirements` so that `scan_requirement` treats
    /// `(caller_id, args)` as having produced `pks`. Simulates what Phase 0
    /// would have done for this invocation.
    fn fill_req(
        ctx: &mut ExecutionContext,
        caller_id: &str,
        args: &[Value],
        pks: Vec<Vec<Value>>,
    ) {
        let cells: Vec<CellValue> = args.iter().map(value_to_cell).collect();
        ctx.requirements
            .pk_sets
            .insert((caller_id.to_string(), cells), pks);
    }

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    fn make_users_table() -> Table {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
        t
    }

    fn make_orders_table() -> Table {
        let schema = TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(10), CellValue::I64(1), CellValue::I64(100)]).unwrap();
        t.insert(&[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]).unwrap();
        t.insert(&[CellValue::I64(12), CellValue::I64(2), CellValue::I64(50)]).unwrap();
        t
    }

    fn users_query_schema() -> Schema {
        Schema::new(vec![
            ColumnDef { table: Some("users".into()), name: "id".into() },
            ColumnDef { table: Some("users".into()), name: "name".into() },
            ColumnDef { table: Some("users".into()), name: "age".into() },
        ])
    }

    fn orders_query_schema() -> Schema {
        Schema::new(vec![
            ColumnDef { table: Some("orders".into()), name: "id".into() },
            ColumnDef { table: Some("orders".into()), name: "user_id".into() },
            ColumnDef { table: Some("orders".into()), name: "amount".into() },
        ])
    }

    fn make_db() -> HashMap<String, Table> {
        let mut db = HashMap::new();
        db.insert("users".into(), make_users_table());
        db.insert("orders".into(), make_orders_table());
        db
    }

    #[test]
    fn test_execute_scan_filter_project() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Table {
                    name: "users".into(), schema: users_query_schema(),
                    scan_method: PlanScanMethod::Full,
                },
                join: None, pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) },
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Column { col: c(0, 2), alias: None },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
        assert_eq!(result[1], vec![CellValue::I64(30), CellValue::I64(35)]);
        assert!(!ctx.spans.is_empty());
    }

    #[test]
    fn test_execute_join() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    source: PlanSource::Table {
                        name: "users".into(), schema: users_query_schema(),
                        scan_method: PlanScanMethod::Full,
                    },
                    join: None, pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    source: PlanSource::Table {
                        name: "orders".into(), schema: orders_query_schema(),
                        scan_method: PlanScanMethod::Full,
                    },
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) },
                        strategy: PlanJoinStrategy::NestedLoop,
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Column { col: c(1, 2), alias: None },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Alice".into()), CellValue::Str("Bob".into())]);
        assert_eq!(result[1], vec![CellValue::I64(100), CellValue::I64(200), CellValue::I64(50)]);
    }

    #[test]
    fn test_execute_aggregate() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Table {
                    name: "users".into(), schema: users_query_schema(),
                    scan_method: PlanScanMethod::Full,
                },
                join: None, pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![c(0, 1)],
            aggregates: vec![PlanAggregate { func: AggFunc::Min, col: c(0, 2) }],
            order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Aggregate { func: AggFunc::Min, col: c(0, 2), alias: Some("min_age".into()) },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[0][0], CellValue::Str("Alice".into()));
        assert_eq!(result[1][0], CellValue::I64(30));
    }

    #[test]
    fn test_execute_table_not_found() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Table {
                    name: "nonexistent".into(), schema: Schema::new(vec![]),
                    scan_method: PlanScanMethod::Full,
                },
                join: None, pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![],
        };
        let err = execute(&mut ctx, &plan).unwrap_err();
        assert!(matches!(err, ExecuteError::TableNotFound(_)));
    }

    // ── Caller-backed source (MVS for P6) ────────────────────────────────
    //
    // These tests exercise the PK-producer model: a caller returns PK
    // tuples, and scan_requirement looks up rows in the local row_table
    // via the auto-created PK index.

    fn caller_plan(caller_id: &str, row_table: &str, arg_placeholder: &str) -> PlanSelect {
        PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Requirement {
                    alias: row_table.into(),
                    row_table: row_table.into(),
                    row_schema: users_query_schema(),
                    caller_id: caller_id.into(),
                    args: vec![RequirementArg::Placeholder(arg_placeholder.into())],
                },
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 0), alias: None },
                PlanResultColumn::Column { col: c(0, 1), alias: None },
            ],
        }
    }

    #[test]
    fn test_execute_caller_source_returns_rows_by_pk() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(1));

        // Phase 0 would have fetched & upserted. Here we just pre-fill the
        // resolved PK set, since the row_table already has Alice/Bob/Carol.
        fill_req(&mut ctx, "users::by_ids", &[Value::Int(1)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
        ]);

        let plan = caller_plan("users::by_ids", "users", "__caller_0_arg_0");
        let result = execute(&mut ctx, &plan).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(result[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    #[test]
    fn test_execute_caller_missing_requirements_entry_errors() {
        // Nothing was populated by Phase 0 — scan surfaces this as a
        // CallerError with the "no result" marker.
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(1));

        let plan = caller_plan("users::unknown", "users", "__caller_0_arg_0");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::CallerError(msg) => assert!(
                msg.contains("Phase 0 produced no result"),
                "got: {msg}",
            ),
            other => panic!("expected CallerError, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_caller_missing_arg_errors() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        // No bound_values, no params — scan's arg resolver fails before it
        // ever looks in `requirements`.
        let plan = caller_plan("users::by_ids", "users", "__caller_0_arg_0");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::BindError(msg) => assert!(msg.contains("missing value"), "got: {msg}"),
            other => panic!("expected BindError, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_caller_pk_not_in_row_table_errors() {
        // Phase 0 stored PK 999, but users row_table has no such row.
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(1));
        fill_req(&mut ctx, "users::phantom", &[Value::Int(1)], vec![
            vec![Value::Int(999)],
        ]);

        let plan = caller_plan("users::phantom", "users", "__caller_0_arg_0");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::CallerError(msg) => assert!(
                msg.contains("not present in row_table"),
                "got: {msg}",
            ),
            other => panic!("expected CallerError, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_caller_user_placeholder_from_params() {
        let db = make_db();
        let params = HashMap::from([("owner".into(), super::super::ParamValue::Int(2))]);
        let mut ctx = ExecutionContext::with_params(&db, params);
        // arg resolves from params (no bound_value). Phase 0 key is Int(2).
        fill_req(&mut ctx, "users::by_owner", &[Value::Int(2)], vec![vec![Value::Int(2)]]);

        let plan = caller_plan("users::by_owner", "users", "owner");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::I64(2)]);
        assert_eq!(result[1], vec![CellValue::Str("Bob".into())]);
    }

    #[test]
    fn test_execute_caller_with_pre_filter() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::all", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]);

        let mut plan = caller_plan("users::all", "users", "__caller_0_arg_0");
        // Post-filter on age>28 — should narrow to Alice(30) and Carol(35).
        plan.sources[0].pre_filter = PlanFilterPredicate::GreaterThan {
            col: c(0, 2),
            value: Value::Int(28),
        };

        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(result[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    // ── Caller edge cases ────────────────────────────────────────────────

    #[test]
    fn test_execute_caller_empty_return_yields_no_rows() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::none", &[Value::Int(0)], vec![]);

        let plan = caller_plan("users::none", "users", "__caller_0_arg_0");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_empty());
        assert!(result[1].is_empty());
    }

    #[test]
    fn test_execute_caller_duplicate_pks_are_deduplicated() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::dup", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]);

        let plan = caller_plan("users::dup", "users", "__caller_0_arg_0");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::I64(1), CellValue::I64(2)]);
    }

    #[test]
    fn test_execute_caller_pk_arity_mismatch_errors() {
        // row_table users has single-column PK; Phase 0 stored a 2-col PK
        // tuple → scan must reject at lookup time.
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::bad_arity", &[Value::Int(0)], vec![
            vec![Value::Int(1), Value::Int(99)],
        ]);

        let plan = caller_plan("users::bad_arity", "users", "__caller_0_arg_0");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::CallerError(msg) => {
                assert!(msg.contains("PK has 2 columns"), "got: {msg}");
                assert!(msg.contains("row_table expects 1"), "got: {msg}");
            }
            other => panic!("expected CallerError, got {other:?}"),
        }
    }

    fn make_users_table_no_pk() -> Table {
        let schema = TableSchema {
            name: "users_no_pk".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]).unwrap();
        t
    }

    #[test]
    fn test_execute_caller_row_table_without_pk_errors() {
        let mut db = HashMap::new();
        db.insert("users_no_pk".into(), make_users_table_no_pk());
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users_no_pk::all", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
        ]);

        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Requirement {
                    alias: "users_no_pk".into(),
                    row_table: "users_no_pk".into(),
                    row_schema: Schema::new(vec![]),
                    caller_id: "users_no_pk::all".into(),
                    args: vec![RequirementArg::Placeholder("__caller_0_arg_0".into())],
                },
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![PlanResultColumn::Column { col: c(0, 0), alias: None }],
        };

        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::CallerError(msg) => assert!(
                msg.contains("has no primary key"),
                "got: {msg}",
            ),
            other => panic!("expected CallerError, got {other:?}"),
        }
    }

    // ── Caller param resolution ──────────────────────────────────────────

    #[test]
    fn test_execute_caller_bound_values_wins_over_params() {
        // If both bound_values and ctx.params have the placeholder, bound_values wins.
        let db = make_db();
        let params = HashMap::from([("x".into(), super::super::ParamValue::Int(99))]);
        let mut ctx = ExecutionContext::with_params(&db, params);
        ctx.bound_values.insert("x".into(), Value::Int(2));
        // Phase 0 key uses the resolved value (2, not 99).
        fill_req(&mut ctx, "users::echo", &[Value::Int(2)], vec![vec![Value::Int(2)]]);

        let plan = caller_plan("users::echo", "users", "x");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::I64(2)]);
    }

    #[test]
    fn test_execute_caller_param_list_rejected() {
        let db = make_db();
        let params = HashMap::from([(
            "ids".into(),
            super::super::ParamValue::IntList(vec![1, 2]),
        )]);
        let mut ctx = ExecutionContext::with_params(&db, params);

        let plan = caller_plan("users::any", "users", "ids");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::BindError(msg) => assert!(
                msg.contains("list, expected scalar"),
                "got: {msg}",
            ),
            other => panic!("expected BindError, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_caller_param_text_list_rejected() {
        let db = make_db();
        let params = HashMap::from([(
            "names".into(),
            super::super::ParamValue::TextList(vec!["a".into()]),
        )]);
        let mut ctx = ExecutionContext::with_params(&db, params);
        let plan = caller_plan("users::any", "users", "names");
        let err = execute(&mut ctx, &plan).unwrap_err();
        assert!(matches!(err, ExecuteError::BindError(msg) if msg.contains("list")));
    }

    #[test]
    fn test_execute_caller_param_text_resolves() {
        let db = make_db();
        let params = HashMap::from([(
            "name".into(),
            super::super::ParamValue::Text("hello".into()),
        )]);
        let mut ctx = ExecutionContext::with_params(&db, params);
        fill_req(&mut ctx, "users::by_name", &[Value::Text("hello".into())], vec![
            vec![Value::Int(1)],
        ]);
        let plan = caller_plan("users::by_name", "users", "name");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::I64(1)]);
    }

    #[test]
    fn test_execute_caller_param_null_resolves() {
        let db = make_db();
        let params = HashMap::from([("x".into(), super::super::ParamValue::Null)]);
        let mut ctx = ExecutionContext::with_params(&db, params);
        fill_req(&mut ctx, "users::null_arg", &[Value::Null], vec![
            vec![Value::Int(1)],
        ]);
        let plan = caller_plan("users::null_arg", "users", "x");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::I64(1)]);
    }

    // ── Caller multi-arg / deleted / String PK / composite PK ────────────

    #[test]
    fn test_execute_caller_multiple_args_passed_through() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(42));
        ctx.bound_values.insert("__caller_0_arg_1".into(), Value::Text("bob".into()));
        ctx.bound_values.insert("__caller_0_arg_2".into(), Value::Null);
        fill_req(
            &mut ctx,
            "users::three",
            &[Value::Int(42), Value::Text("bob".into()), Value::Null],
            vec![vec![Value::Int(2)]],
        );

        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Requirement {
                    alias: "users".into(),
                    row_table: "users".into(),
                    row_schema: users_query_schema(),
                    caller_id: "users::three".into(),
                    args: vec![
                        RequirementArg::Placeholder("__caller_0_arg_0".into()),
                        RequirementArg::Placeholder("__caller_0_arg_1".into()),
                        RequirementArg::Placeholder("__caller_0_arg_2".into()),
                    ],
                },
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![PlanResultColumn::Column { col: c(0, 1), alias: None }],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::Str("Bob".into())]);
    }

    #[test]
    fn test_execute_caller_pk_of_deleted_row_errors() {
        // Invariant: a Phase 0 PK that is no longer in the PK index
        // (either never existed, or was deleted) must surface as a CallerError.
        // Silently skipping would hide stale upstream state — the opposite of
        // what the reactive layer needs.
        let mut db = make_db();
        db.get_mut("users").unwrap().delete(1).unwrap(); // Bob gone.
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::all", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
        ]);

        let plan = caller_plan("users::all", "users", "__caller_0_arg_0");
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::CallerError(msg) => {
                assert!(msg.contains("not present in row_table"), "got: {msg}");
                assert!(msg.contains("Int(2)"), "got: {msg}");
            }
            other => panic!("expected CallerError, got {other:?}"),
        }
    }

    #[test]
    fn test_execute_caller_live_rows_only_succeed_after_delete() {
        // Happy counterpart: when Phase 0 only stored PKs for live rows
        // (correctly reflecting state), the query succeeds and returns
        // exactly those live rows.
        let mut db = make_db();
        db.get_mut("users").unwrap().delete(1).unwrap();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::live", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
        ]);

        let plan = caller_plan("users::live", "users", "__caller_0_arg_0");
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![CellValue::I64(1), CellValue::I64(3)]);
    }

    fn make_tags_table_string_pk() -> Table {
        let schema = TableSchema {
            name: "tags".into(),
            columns: vec![
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "color".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::Str("red".into()), CellValue::Str("#f00".into())]).unwrap();
        t.insert(&[CellValue::Str("green".into()), CellValue::Str("#0f0".into())]).unwrap();
        t.insert(&[CellValue::Str("blue".into()), CellValue::Str("#00f".into())]).unwrap();
        t
    }

    #[test]
    fn test_execute_caller_string_pk_lookup() {
        let mut db = HashMap::new();
        db.insert("tags".into(), make_tags_table_string_pk());
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "tags::popular", &[Value::Int(0)], vec![
            vec![Value::Text("red".into())],
            vec![Value::Text("blue".into())],
        ]);

        let tags_schema = Schema::new(vec![
            ColumnDef { table: Some("tags".into()), name: "name".into() },
            ColumnDef { table: Some("tags".into()), name: "color".into() },
        ]);
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Requirement {
                    alias: "tags".into(),
                    row_table: "tags".into(),
                    row_schema: tags_schema,
                    caller_id: "tags::popular".into(),
                    args: vec![RequirementArg::Placeholder("__caller_0_arg_0".into())],
                },
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 0), alias: None },
                PlanResultColumn::Column { col: c(0, 1), alias: None },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0].len(), 2);
        // sort_unstable on row_ids means insertion order — red first (row 0), blue third (row 2).
        assert_eq!(result[0][0], CellValue::Str("red".into()));
        assert_eq!(result[0][1], CellValue::Str("blue".into()));
    }

    fn make_memberships_composite_pk() -> Table {
        let schema = TableSchema {
            name: "memberships".into(),
            columns: vec![
                ColumnSchema { name: "org_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "role".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0, 1],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::I64(1), CellValue::Str("admin".into())]).unwrap();
        t.insert(&[CellValue::I64(1), CellValue::I64(2), CellValue::Str("member".into())]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::I64(1), CellValue::Str("owner".into())]).unwrap();
        t
    }

    #[test]
    fn test_execute_caller_composite_pk_lookup() {
        let mut db = HashMap::new();
        db.insert("memberships".into(), make_memberships_composite_pk());
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "memberships::for_admin", &[Value::Int(0)], vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
        ]);

        let m_schema = Schema::new(vec![
            ColumnDef { table: Some("memberships".into()), name: "org_id".into() },
            ColumnDef { table: Some("memberships".into()), name: "user_id".into() },
            ColumnDef { table: Some("memberships".into()), name: "role".into() },
        ]);
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                source: PlanSource::Requirement {
                    alias: "memberships".into(),
                    row_table: "memberships".into(),
                    row_schema: m_schema,
                    caller_id: "memberships::for_admin".into(),
                    args: vec![RequirementArg::Placeholder("__caller_0_arg_0".into())],
                },
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![PlanResultColumn::Column { col: c(0, 2), alias: None }],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        assert_eq!(result[0], vec![
            CellValue::Str("admin".into()),
            CellValue::Str("owner".into()),
        ]);
    }

    // ── Phase-2 joins involving caller sources ───────────────────────────

    #[test]
    fn test_execute_phase2_table_join_caller() {
        // users (table) ⨝ orders-by-user(id) (caller into orders).
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_1_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "orders::by_user", &[Value::Int(0)], vec![
            vec![Value::Int(10)],
            vec![Value::Int(11)],
        ]);

        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    source: PlanSource::Table {
                        name: "users".into(), schema: users_query_schema(),
                        scan_method: PlanScanMethod::Full,
                    },
                    join: None, pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    source: PlanSource::Requirement {
                        alias: "orders".into(),
                        row_table: "orders".into(),
                        row_schema: orders_query_schema(),
                        caller_id: "orders::by_user".into(),
                        args: vec![RequirementArg::Placeholder("__caller_1_arg_0".into())],
                    },
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) },
                        strategy: PlanJoinStrategy::NestedLoop,
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Column { col: c(1, 2), alias: None },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        // Both orders 10 & 11 belong to user_id=1 (Alice).
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Alice".into())]);
        assert_eq!(result[1], vec![CellValue::I64(100), CellValue::I64(200)]);
    }

    #[test]
    fn test_execute_phase2_caller_join_table() {
        // caller (users) first source, orders plain-table second as join partner.
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::pick", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
        ]);

        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    source: PlanSource::Requirement {
                        alias: "users".into(),
                        row_table: "users".into(),
                        row_schema: users_query_schema(),
                        caller_id: "users::pick".into(),
                        args: vec![RequirementArg::Placeholder("__caller_0_arg_0".into())],
                    },
                    join: None, pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    source: PlanSource::Table {
                        name: "orders".into(), schema: orders_query_schema(),
                        scan_method: PlanScanMethod::Full,
                    },
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) },
                        strategy: PlanJoinStrategy::NestedLoop,
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Column { col: c(1, 0), alias: None },
            ],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        // Alice has orders 10,11; Bob has order 12.
        assert_eq!(result[0], vec![
            CellValue::Str("Alice".into()),
            CellValue::Str("Alice".into()),
            CellValue::Str("Bob".into()),
        ]);
        assert_eq!(result[1], vec![CellValue::I64(10), CellValue::I64(11), CellValue::I64(12)]);
    }

    #[test]
    fn test_execute_phase2_caller_join_caller() {
        // Caller ⨝ Caller (same row_table twice through different callers).
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_0_arg_0".into(), Value::Int(0));
        ctx.bound_values.insert("__caller_1_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::first", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
        ]);
        fill_req(&mut ctx, "users::second", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
        ]);

        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    source: PlanSource::Requirement {
                        alias: "users".into(), row_table: "users".into(),
                        row_schema: users_query_schema(),
                        caller_id: "users::first".into(),
                        args: vec![RequirementArg::Placeholder("__caller_0_arg_0".into())],
                    },
                    join: None, pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    source: PlanSource::Requirement {
                        alias: "users".into(), row_table: "users".into(),
                        row_schema: users_query_schema(),
                        caller_id: "users::second".into(),
                        args: vec![RequirementArg::Placeholder("__caller_1_arg_0".into())],
                    },
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 0) },
                        strategy: PlanJoinStrategy::NestedLoop,
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![PlanResultColumn::Column { col: c(0, 0), alias: None }],
        };
        let result = execute(&mut ctx, &plan).unwrap();
        // Intersection by PK: only id=1 is in both sets.
        assert_eq!(result[0], vec![CellValue::I64(1)]);
    }

    #[test]
    fn test_execute_index_lookup_onto_caller_rejected() {
        let db = make_db();
        let mut ctx = ExecutionContext::new(&db);
        ctx.bound_values.insert("__caller_1_arg_0".into(), Value::Int(0));
        fill_req(&mut ctx, "users::pick", &[Value::Int(0)], vec![
            vec![Value::Int(1)],
        ]);

        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    source: PlanSource::Table {
                        name: "orders".into(), schema: orders_query_schema(),
                        scan_method: PlanScanMethod::Full,
                    },
                    join: None, pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    source: PlanSource::Requirement {
                        alias: "users".into(), row_table: "users".into(),
                        row_schema: users_query_schema(),
                        caller_id: "users::pick".into(),
                        args: vec![RequirementArg::Placeholder("__caller_1_arg_0".into())],
                    },
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals { left: c(0, 1), right: c(1, 0) },
                        strategy: PlanJoinStrategy::IndexLookup {
                            left_col: c(0, 1),
                            right_col: 0,
                            index_columns: vec![0],
                            is_hash: true,
                        },
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![], aggregates: vec![], order_by: vec![], limit: None,
            result_columns: vec![PlanResultColumn::Column { col: c(0, 0), alias: None }],
        };
        let err = execute(&mut ctx, &plan).unwrap_err();
        match err {
            ExecuteError::NotImplemented(msg) => assert!(
                msg.contains("IndexLookup join strategy onto caller"),
                "got: {msg}",
            ),
            other => panic!("expected NotImplemented, got {other:?}"),
        }
    }
}
