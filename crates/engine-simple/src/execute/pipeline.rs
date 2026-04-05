//! Core execution pipeline: scan -> join -> filter -> aggregate -> sort -> limit -> project.

use std::collections::HashMap;

use crate::planner::plan::*;
use crate::storage::Table;

use super::{aggregate, join, project, scan, sort};
use super::{Columns, ExecuteError};

pub fn execute(
    plan: &PlanSelect,
    db: &HashMap<String, Table>,
) -> Result<Columns, ExecuteError> {
    // Phase 1: Scan first source -> RowSet (no materialization).
    let first = &plan.sources[0];
    let first_table = db
        .get(&first.table)
        .ok_or_else(|| ExecuteError::TableNotFound(first.table.clone()))?;
    let mut rs = scan::scan(first_table, &first.pre_filter);

    // Phase 2: Join remaining sources (each extends the RowSet).
    for (source_idx, source) in plan.sources.iter().enumerate().skip(1) {
        let table = db
            .get(&source.table)
            .ok_or_else(|| ExecuteError::TableNotFound(source.table.clone()))?;
        let right = scan::scan(table, &source.pre_filter);
        match source.join.as_ref() {
            Some(j) => {
                rs = join::nested_loop_join(
                    &rs,
                    right.tables[0],
                    &right.row_ids[0],
                    source_idx,
                    &j.on,
                    j.join_type,
                );
            }
            None => {
                rs = join::nested_loop_join(
                    &rs,
                    right.tables[0],
                    &right.row_ids[0],
                    source_idx,
                    &PlanFilterPredicate::None,
                    query_engine::ast::JoinType::Inner,
                );
            }
        }
    }

    // Phase 3: Post-filter on RowSet (no materialization).
    if !matches!(plan.filter, PlanFilterPredicate::None) {
        rs = rs.filter(&plan.filter);
    }

    // Phase 4: Aggregate (RowSet -> small materialized Columns).
    if !plan.group_by.is_empty() || !plan.aggregates.is_empty() {
        let aggregated =
            aggregate::aggregate_rowset(&rs, &plan.group_by, &plan.aggregates);
        let has_aggregates = !plan.aggregates.is_empty();
        let mut result = project::project(
            &aggregated,
            &plan.result_columns,
            &plan.group_by,
            has_aggregates,
        );
        // Phase 4b: Sort aggregated results.
        if !plan.order_by.is_empty() {
            sort::sort_materialized(&mut result, &plan.order_by, &plan.result_columns);
        }
        // Phase 4c: Limit aggregated results.
        if let Some(limit) = plan.limit {
            for col in &mut result {
                col.truncate(limit);
            }
        }
        return Ok(result);
    }

    // Phase 5: Sort RowSet before projection.
    if !plan.order_by.is_empty() {
        rs.sort(&plan.order_by);
    }

    // Phase 5b: Limit RowSet before projection.
    if let Some(limit) = plan.limit {
        if rs.num_rows > limit {
            for ids in &mut rs.row_ids {
                ids.truncate(limit);
            }
            rs.num_rows = limit;
        }
    }

    // Phase 6: Project — materialize only result columns from RowSet.
    Ok(project::project_rowset(&rs, &plan.result_columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{CellValue, Table};
    use query_engine::ast::*;
    use query_engine::schema::{ColumnDef, Schema};
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

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
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                table: "users".into(),
                schema: users_query_schema(),
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::GreaterThan {
                col: c(0, 2),
                value: Value::Int(28),
            },
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Column { col: c(0, 2), alias: None },
            ],
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
        assert_eq!(result[1], vec![CellValue::I64(30), CellValue::I64(35)]);
    }

    #[test]
    fn test_execute_join() {
        let db = make_db();
        let plan = PlanSelect {
            sources: vec![
                PlanSourceEntry {
                    table: "users".into(),
                    schema: users_query_schema(),
                    join: None,
                    pre_filter: PlanFilterPredicate::None,
                },
                PlanSourceEntry {
                    table: "orders".into(),
                    schema: orders_query_schema(),
                    join: Some(PlanJoin {
                        join_type: JoinType::Inner,
                        on: PlanFilterPredicate::ColumnEquals {
                            left: c(0, 0),  // users.id
                            right: c(1, 1), // orders.user_id
                        },
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None }, // users.name
                PlanResultColumn::Column { col: c(1, 2), alias: None }, // orders.amount
            ],
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[0], vec![
            CellValue::Str("Alice".into()),
            CellValue::Str("Alice".into()),
            CellValue::Str("Bob".into()),
        ]);
        assert_eq!(result[1], vec![
            CellValue::I64(100),
            CellValue::I64(200),
            CellValue::I64(50),
        ]);
    }

    #[test]
    fn test_execute_aggregate() {
        let db = make_db();
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                table: "users".into(),
                schema: users_query_schema(),
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![c(0, 1)], // users.name
            aggregates: vec![PlanAggregate {
                func: AggFunc::Min,
                col: c(0, 2), // users.age
            }],
            order_by: vec![],
            limit: None,
            result_columns: vec![
                PlanResultColumn::Column { col: c(0, 1), alias: None },
                PlanResultColumn::Aggregate {
                    func: AggFunc::Min,
                    col: c(0, 2),
                    alias: Some("min_age".into()),
                },
            ],
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[0][0], CellValue::Str("Alice".into()));
        assert_eq!(result[1][0], CellValue::I64(30));
    }

    #[test]
    fn test_execute_table_not_found() {
        let db = HashMap::new();
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                table: "nonexistent".into(),
                schema: Schema::new(vec![]),
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
            result_columns: vec![],
        };

        let err = execute(&plan, &db).unwrap_err();
        assert!(matches!(err, ExecuteError::TableNotFound(_)));
    }
}
