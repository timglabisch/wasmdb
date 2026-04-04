pub mod aggregate;
pub mod eval;
pub mod join;
pub mod project;
pub mod scan;

use std::collections::HashMap;

use crate::planner::plan::*;
use crate::storage::{CellValue, Table};
use query_engine::ast::Value;

pub type Column = Vec<CellValue>;
pub type Columns = Vec<Column>; // columns[col_idx][row_idx]

/// Sentinel row ID for null-fill in left joins (no match on right side).
pub const NULL_ROW: usize = usize::MAX;

/// Virtual row set backed by references to underlying Tables.
/// No data is copied — cell access goes through row_id indirection.
pub struct RowSet<'a> {
    pub tables: Vec<&'a Table>,
    pub col_offsets: Vec<usize>,
    pub num_cols: usize,
    /// `row_ids[table_idx][output_row]` = physical row in that table.
    /// [`NULL_ROW`] means null fill (left join, no match).
    pub row_ids: Vec<Vec<usize>>,
    pub num_rows: usize,
}

impl<'a> RowSet<'a> {
    pub fn from_scan(table: &'a Table, row_ids: Vec<usize>) -> Self {
        let num_rows = row_ids.len();
        let num_cols = table.columns.len();
        RowSet {
            tables: vec![table],
            col_offsets: vec![0],
            num_cols,
            row_ids: vec![row_ids],
            num_rows,
        }
    }

    pub fn get(&self, row: usize, global_col: usize) -> CellValue {
        let (table_idx, local_col) = self.resolve_col(global_col);
        let row_id = self.row_ids[table_idx][row];
        if row_id == NULL_ROW {
            CellValue::Null
        } else {
            self.tables[table_idx].get(row_id, local_col)
        }
    }

    fn resolve_col(&self, global_col: usize) -> (usize, usize) {
        for i in (0..self.col_offsets.len()).rev() {
            if global_col >= self.col_offsets[i] {
                return (i, global_col - self.col_offsets[i]);
            }
        }
        unreachable!()
    }

    pub fn filter(&self, pred: &PlanFilterPredicate) -> RowSet<'a> {
        let mut new_row_ids: Vec<Vec<usize>> =
            (0..self.tables.len()).map(|_| Vec::new()).collect();
        let mut count = 0;
        for row in 0..self.num_rows {
            if eval::eval_rowset_row(pred, self, row) {
                for (ti, ids) in self.row_ids.iter().enumerate() {
                    new_row_ids[ti].push(ids[row]);
                }
                count += 1;
            }
        }
        RowSet {
            tables: self.tables.clone(),
            col_offsets: self.col_offsets.clone(),
            num_cols: self.num_cols,
            row_ids: new_row_ids,
            num_rows: count,
        }
    }
}

#[derive(Debug)]
pub enum ExecuteError {
    TableNotFound(String),
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::TableNotFound(t) => write!(f, "table not found: {t}"),
        }
    }
}

impl std::error::Error for ExecuteError {}

pub fn value_to_cell(v: &Value) -> CellValue {
    match v {
        Value::Int(n) => CellValue::I64(*n),
        Value::Text(s) => CellValue::Str(s.clone()),
        Value::Null => CellValue::Null,
        Value::Bool(b) => CellValue::I64(if *b { 1 } else { 0 }),
        Value::Float(f) => CellValue::I64(*f as i64),
    }
}

pub fn execute(
    plan: &PlanSelect,
    db: &HashMap<String, Table>,
) -> Result<Columns, ExecuteError> {
    // Phase 1: Scan first source → RowSet (no materialization).
    let first = &plan.sources[0];
    let first_table = db
        .get(&first.table)
        .ok_or_else(|| ExecuteError::TableNotFound(first.table.clone()))?;
    let mut rs = scan::scan(first_table, &first.pre_filter);

    // Phase 2: Join remaining sources (each extends the RowSet).
    for source in plan.sources.iter().skip(1) {
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
                    &j.on,
                    j.join_type,
                );
            }
            None => {
                rs = join::nested_loop_join(
                    &rs,
                    right.tables[0],
                    &right.row_ids[0],
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

    // Phase 4: Aggregate (RowSet → small materialized Columns).
    if !plan.group_by.is_empty() || !plan.aggregates.is_empty() {
        let aggregated =
            aggregate::aggregate_rowset(&rs, &plan.group_by, &plan.aggregates);
        let has_aggregates = !plan.aggregates.is_empty();
        return Ok(project::project(
            &aggregated,
            &plan.result_columns,
            &plan.group_by,
            has_aggregates,
        ));
    }

    // Phase 5: Project — materialize only result columns from RowSet.
    Ok(project::project_rowset(&rs, &plan.result_columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CellValue;
    use query_engine::ast::*;
    use query_engine::schema::{ColumnDef, Schema};
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

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
        // SELECT users.name, users.age FROM users WHERE users.age > 28
        let db = make_db();
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                table: "users".into(),
                schema: users_query_schema(),
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::GreaterThan {
                column_idx: 2,
                value: Value::Int(28),
            },
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![
                PlanResultColumn::Column { column_idx: 1, alias: None },
                PlanResultColumn::Column { column_idx: 2, alias: None },
            ],
            schema: users_query_schema(),
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result.len(), 2); // 2 output columns
        assert_eq!(result[0].len(), 2); // Alice(30), Carol(35)
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
        assert_eq!(result[1], vec![CellValue::I64(30), CellValue::I64(35)]);
    }

    #[test]
    fn test_execute_join() {
        // SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id
        let db = make_db();
        let combined = Schema::merge(&users_query_schema(), &orders_query_schema());
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
                            left_idx: 0,  // users.id
                            right_idx: 4, // orders.user_id
                        },
                    }),
                    pre_filter: PlanFilterPredicate::None,
                },
            ],
            filter: PlanFilterPredicate::None,
            group_by: vec![],
            aggregates: vec![],
            result_columns: vec![
                PlanResultColumn::Column { column_idx: 1, alias: None }, // users.name
                PlanResultColumn::Column { column_idx: 5, alias: None }, // orders.amount
            ],
            schema: combined,
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result[0].len(), 3); // Alice×2, Bob×1
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
        // SELECT users.name, MIN(users.age) FROM users GROUP BY users.name
        let db = make_db();
        let plan = PlanSelect {
            sources: vec![PlanSourceEntry {
                table: "users".into(),
                schema: users_query_schema(),
                join: None,
                pre_filter: PlanFilterPredicate::None,
            }],
            filter: PlanFilterPredicate::None,
            group_by: vec![1], // users.name
            aggregates: vec![PlanAggregate {
                func: AggFunc::Min,
                column_idx: 2, // users.age
            }],
            result_columns: vec![
                PlanResultColumn::Column { column_idx: 1, alias: None },
                PlanResultColumn::Aggregate {
                    func: AggFunc::Min,
                    column_idx: 2,
                    alias: Some("min_age".into()),
                },
            ],
            schema: users_query_schema(),
        };

        let result = execute(&plan, &db).unwrap();
        assert_eq!(result.len(), 2); // name + min_age
        assert_eq!(result[0].len(), 3); // 3 unique names
        // Each user is their own group (unique names)
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
            result_columns: vec![],
            schema: Schema::new(vec![]),
        };

        let err = execute(&plan, &db).unwrap_err();
        assert!(matches!(err, ExecuteError::TableNotFound(_)));
    }
}
