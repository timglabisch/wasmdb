use std::collections::HashMap;

use crate::planner::plan::*;
use crate::storage::{CellValue, Table};
use query_engine::ast::{AggFunc, JoinType};

use super::eval::{eval_join_condition, eval_predicate};
use super::{num_rows, Columns};

pub fn scan(table: &Table) -> Columns {
    let num_cols = table.columns.len();
    let mut columns: Columns = (0..num_cols).map(|_| Vec::new()).collect();

    for row_idx in table.row_indices() {
        for col_idx in 0..num_cols {
            columns[col_idx].push(table.get(row_idx, col_idx));
        }
    }
    columns
}

pub fn filter(cols: &Columns, pred: &PlanFilterPredicate) -> Columns {
    let mask = eval_predicate(cols, pred);
    apply_mask(cols, &mask)
}

fn apply_mask(cols: &Columns, mask: &[bool]) -> Columns {
    cols.iter()
        .map(|col| {
            col.iter()
                .zip(mask.iter())
                .filter(|(_, &keep)| keep)
                .map(|(v, _)| v.clone())
                .collect()
        })
        .collect()
}

pub fn nested_loop_join(
    left: &Columns,
    right: &Columns,
    on: &PlanFilterPredicate,
    join_type: JoinType,
) -> Columns {
    let left_rows = num_rows(left);
    let right_rows = num_rows(right);
    let total_cols = left.len() + right.len();
    let mut result: Columns = (0..total_cols).map(|_| Vec::new()).collect();

    for l in 0..left_rows {
        let mut matched = false;

        for r in 0..right_rows {
            if eval_join_condition(left, right, l, r, on) {
                matched = true;
                for (ci, col) in left.iter().enumerate() {
                    result[ci].push(col[l].clone());
                }
                for (ci, col) in right.iter().enumerate() {
                    result[left.len() + ci].push(col[r].clone());
                }
            }
        }

        if !matched && join_type == JoinType::Left {
            for (ci, col) in left.iter().enumerate() {
                result[ci].push(col[l].clone());
            }
            for ci in 0..right.len() {
                result[left.len() + ci].push(CellValue::Null);
            }
        }
    }

    result
}

pub fn aggregate(
    cols: &Columns,
    group_by: &[usize],
    aggregates: &[PlanAggregate],
) -> Columns {
    let n = num_rows(cols);
    let mut groups: HashMap<Vec<CellValue>, Vec<Accumulator>> = HashMap::new();
    let mut group_order: Vec<Vec<CellValue>> = Vec::new();

    for row in 0..n {
        let key: Vec<CellValue> = group_by.iter().map(|&ci| cols[ci][row].clone()).collect();

        let accums = groups.entry(key.clone()).or_insert_with(|| {
            group_order.push(key.clone());
            aggregates.iter().map(|agg| Accumulator::new(agg.func)).collect()
        });

        for (ai, agg) in aggregates.iter().enumerate() {
            accums[ai].feed(&cols[agg.column_idx][row]);
        }
    }

    let out_cols = group_by.len() + aggregates.len();
    let mut result: Columns = (0..out_cols).map(|_| Vec::new()).collect();

    for key in &group_order {
        for (i, val) in key.iter().enumerate() {
            result[i].push(val.clone());
        }
        let accums = &groups[key];
        for (i, acc) in accums.iter().enumerate() {
            result[group_by.len() + i].push(acc.finish());
        }
    }

    result
}

struct Accumulator {
    func: AggFunc,
    count: i64,
    sum: i64,
    min: Option<CellValue>,
    max: Option<CellValue>,
}

impl Accumulator {
    fn new(func: AggFunc) -> Self {
        Self {
            func,
            count: 0,
            sum: 0,
            min: None,
            max: None,
        }
    }

    fn feed(&mut self, val: &CellValue) {
        if matches!(val, CellValue::Null) {
            if self.func == AggFunc::Count {
                self.count += 1;
            }
            return;
        }
        match self.func {
            AggFunc::Count => self.count += 1,
            AggFunc::Sum => {
                if let CellValue::I64(n) = val {
                    self.sum += n;
                }
            }
            AggFunc::Min => {
                self.min = Some(match &self.min {
                    None => val.clone(),
                    Some(cur) => {
                        if val < cur {
                            val.clone()
                        } else {
                            cur.clone()
                        }
                    }
                });
            }
            AggFunc::Max => {
                self.max = Some(match &self.max {
                    None => val.clone(),
                    Some(cur) => {
                        if val > cur {
                            val.clone()
                        } else {
                            cur.clone()
                        }
                    }
                });
            }
        }
    }

    fn finish(&self) -> CellValue {
        match self.func {
            AggFunc::Count => CellValue::I64(self.count),
            AggFunc::Sum => CellValue::I64(self.sum),
            AggFunc::Min => self.min.clone().unwrap_or(CellValue::Null),
            AggFunc::Max => self.max.clone().unwrap_or(CellValue::Null),
        }
    }
}

pub fn project(
    cols: &Columns,
    result_columns: &[PlanResultColumn],
    group_by: &[usize],
    has_aggregates: bool,
) -> Columns {
    let mut result: Columns = Vec::with_capacity(result_columns.len());
    let mut agg_counter = 0;

    for rc in result_columns {
        match rc {
            PlanResultColumn::Column { column_idx, .. } => {
                if has_aggregates {
                    let pos = group_by
                        .iter()
                        .position(|&gb| gb == *column_idx)
                        .expect("column in aggregate query must be in group_by");
                    result.push(cols[pos].clone());
                } else {
                    result.push(cols[*column_idx].clone());
                }
            }
            PlanResultColumn::Aggregate { .. } => {
                let pos = group_by.len() + agg_counter;
                result.push(cols[pos].clone());
                agg_counter += 1;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CellValue;
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

    #[test]
    fn test_scan_reads_live_rows() {
        let mut table = make_users_table();
        table.delete(1).unwrap(); // delete Bob
        let cols = scan(&table);
        assert_eq!(cols.len(), 3); // 3 columns
        assert_eq!(cols[0].len(), 2); // 2 live rows
        assert_eq!(cols[0], vec![CellValue::I64(1), CellValue::I64(3)]);
        assert_eq!(cols[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    #[test]
    fn test_filter_reduces_rows() {
        let table = make_users_table();
        let cols = scan(&table);
        let filtered = filter(
            &cols,
            &PlanFilterPredicate::GreaterThan {
                column_idx: 2,
                value: query_engine::ast::Value::Int(28),
            },
        );
        // Alice(30) and Carol(35) pass, Bob(25) doesn't
        assert_eq!(filtered[0].len(), 2);
        assert_eq!(filtered[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }

    #[test]
    fn test_inner_join() {
        let users = make_users_table();
        let orders = make_orders_table();
        let left = scan(&users);
        let right = scan(&orders);

        // JOIN ON users.id = orders.user_id
        // users.id is col 0 in left, orders.user_id is col 1 in right → global idx 4
        let result = nested_loop_join(
            &left,
            &right,
            &PlanFilterPredicate::ColumnEquals {
                left_idx: 0,
                right_idx: 4,
            },
            JoinType::Inner,
        );

        // user 1 (Alice) matches orders 10, 11 → 2 rows
        // user 2 (Bob) matches order 12 → 1 row
        // user 3 (Carol) no match → 0 rows
        assert_eq!(result[0].len(), 3); // 3 matching rows
        assert_eq!(result.len(), 6); // 3 left + 3 right columns

        // Check user names in output
        assert_eq!(result[1], vec![
            CellValue::Str("Alice".into()),
            CellValue::Str("Alice".into()),
            CellValue::Str("Bob".into()),
        ]);
    }

    #[test]
    fn test_left_join_with_nulls() {
        let users = make_users_table();
        let orders = make_orders_table();
        let left = scan(&users);
        let right = scan(&orders);

        let result = nested_loop_join(
            &left,
            &right,
            &PlanFilterPredicate::ColumnEquals {
                left_idx: 0,
                right_idx: 4,
            },
            JoinType::Left,
        );

        // Alice → 2 matches, Bob → 1 match, Carol → 0 matches (gets NULLs)
        assert_eq!(result[0].len(), 4);
        assert_eq!(result[1][3], CellValue::Str("Carol".into()));
        // Carol's right side should be NULL
        assert_eq!(result[3], vec![
            CellValue::I64(10),
            CellValue::I64(11),
            CellValue::I64(12),
            CellValue::Null,
        ]);
    }

    #[test]
    fn test_aggregate_count_sum_min_max() {
        // Two groups: name "A" with ages 10, 30; name "B" with age 20
        let cols: Columns = vec![
            vec![CellValue::Str("A".into()), CellValue::Str("A".into()), CellValue::Str("B".into())],
            vec![CellValue::I64(10), CellValue::I64(30), CellValue::I64(20)],
        ];

        let result = aggregate(
            &cols,
            &[0], // group by column 0 (name)
            &[
                PlanAggregate { func: AggFunc::Count, column_idx: 1 },
                PlanAggregate { func: AggFunc::Sum, column_idx: 1 },
                PlanAggregate { func: AggFunc::Min, column_idx: 1 },
                PlanAggregate { func: AggFunc::Max, column_idx: 1 },
            ],
        );

        // Output: [name, count, sum, min, max]
        assert_eq!(result.len(), 5);
        // Group "A": count=2, sum=40, min=10, max=30
        assert_eq!(result[0][0], CellValue::Str("A".into()));
        assert_eq!(result[1][0], CellValue::I64(2));
        assert_eq!(result[2][0], CellValue::I64(40));
        assert_eq!(result[3][0], CellValue::I64(10));
        assert_eq!(result[4][0], CellValue::I64(30));
        // Group "B": count=1, sum=20, min=20, max=20
        assert_eq!(result[0][1], CellValue::Str("B".into()));
        assert_eq!(result[1][1], CellValue::I64(1));
        assert_eq!(result[2][1], CellValue::I64(20));
        assert_eq!(result[3][1], CellValue::I64(20));
        assert_eq!(result[4][1], CellValue::I64(20));
    }

    #[test]
    fn test_project_simple() {
        let cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(2)],
            vec![CellValue::Str("a".into()), CellValue::Str("b".into())],
            vec![CellValue::I64(10), CellValue::I64(20)],
        ];

        // Select columns 2, 0 (reversed)
        let result = project(
            &cols,
            &[
                PlanResultColumn::Column { column_idx: 2, alias: None },
                PlanResultColumn::Column { column_idx: 0, alias: None },
            ],
            &[],
            false,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::I64(10), CellValue::I64(20)]);
        assert_eq!(result[1], vec![CellValue::I64(1), CellValue::I64(2)]);
    }

    #[test]
    fn test_project_after_aggregate() {
        // Simulate post-aggregate columns: [group_by_col_0, agg_0]
        // group_by = [1] (original col 1 = name)
        // aggregates = [Min on original col 2]
        let cols: Columns = vec![
            vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())],
            vec![CellValue::I64(25), CellValue::I64(30)],
        ];

        let result = project(
            &cols,
            &[
                PlanResultColumn::Column { column_idx: 1, alias: None }, // name → group_by pos 0
                PlanResultColumn::Aggregate {
                    func: AggFunc::Min,
                    column_idx: 2,
                    alias: Some("min_age".into()),
                },
            ],
            &[1], // group_by
            true,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![CellValue::Str("Alice".into()), CellValue::Str("Bob".into())]);
        assert_eq!(result[1], vec![CellValue::I64(25), CellValue::I64(30)]);
    }
}
