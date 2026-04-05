use std::collections::HashMap;

use crate::planner::plan::PlanAggregate;
use crate::storage::CellValue;
use query_engine::ast::AggFunc;

use super::{Columns, ExecutionContext, SpanOperation};

pub fn aggregate_rowset(
    ctx: &mut ExecutionContext,
    rs: &super::RowSet,
    group_by: &[crate::planner::plan::ColumnRef],
    aggregates: &[PlanAggregate],
) -> Columns {
    ctx.span_with(|_ctx| {
        let mut groups: HashMap<Vec<CellValue>, Vec<Accumulator>> = HashMap::new();
        let mut group_order: Vec<Vec<CellValue>> = Vec::new();

        for row in 0..rs.num_rows {
            let key: Vec<CellValue> = group_by.iter().map(|&cr| rs.get(row, cr)).collect();
            let accums = groups.entry(key.clone()).or_insert_with(|| {
                group_order.push(key.clone());
                aggregates.iter().map(|agg| Accumulator::new(agg.func)).collect()
            });
            for (ai, agg) in aggregates.iter().enumerate() {
                accums[ai].feed(&rs.get(row, agg.col));
            }
        }

        let out_cols = group_by.len() + aggregates.len();
        let mut result: Columns = (0..out_cols).map(|_| Vec::new()).collect();
        for key in &group_order {
            for (i, val) in key.iter().enumerate() { result[i].push(val.clone()); }
            let accums = &groups[key];
            for (i, acc) in accums.iter().enumerate() { result[group_by.len() + i].push(acc.finish()); }
        }

        (SpanOperation::Aggregate { groups: group_order.len() }, result)
    })
}

struct Accumulator {
    func: AggFunc,
    count: i64,
    sum: Option<i64>,
    min: Option<CellValue>,
    max: Option<CellValue>,
}

impl Accumulator {
    fn new(func: AggFunc) -> Self {
        Self { func, count: 0, sum: None, min: None, max: None }
    }

    fn feed(&mut self, val: &CellValue) {
        if matches!(val, CellValue::Null) { return; }
        match self.func {
            AggFunc::Count => self.count += 1,
            AggFunc::Sum => {
                if let CellValue::I64(n) = val { *self.sum.get_or_insert(0) += n; }
            }
            AggFunc::Min => {
                self.min = Some(match &self.min {
                    None => val.clone(),
                    Some(cur) => if val < cur { val.clone() } else { cur.clone() },
                });
            }
            AggFunc::Max => {
                self.max = Some(match &self.max {
                    None => val.clone(),
                    Some(cur) => if val > cur { val.clone() } else { cur.clone() },
                });
            }
        }
    }

    fn finish(&self) -> CellValue {
        match self.func {
            AggFunc::Count => CellValue::I64(self.count),
            AggFunc::Sum => match self.sum { Some(s) => CellValue::I64(s), None => CellValue::Null },
            AggFunc::Min => self.min.clone().unwrap_or(CellValue::Null),
            AggFunc::Max => self.max.clone().unwrap_or(CellValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::RowSet;
    use crate::planner::plan::ColumnRef;
    use crate::storage::Table;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    fn make_test_table() -> Table {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "g".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "v".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![], indexes: vec![],
        };
        Table::new(schema)
    }

    #[test]
    fn test_aggregate_count_sum_min_max() {
        let mut ctx = ExecutionContext::new();
        let mut table = make_test_table();
        table.insert(&[CellValue::Str("A".into()), CellValue::I64(10)]).unwrap();
        table.insert(&[CellValue::Str("A".into()), CellValue::I64(30)]).unwrap();
        table.insert(&[CellValue::Str("B".into()), CellValue::I64(20)]).unwrap();
        let rs = RowSet::from_scan(&table, vec![0, 1, 2]);
        let result = aggregate_rowset(&mut ctx, &rs, &[c(0, 0)], &[
            PlanAggregate { func: AggFunc::Count, col: c(0, 1) },
            PlanAggregate { func: AggFunc::Sum, col: c(0, 1) },
            PlanAggregate { func: AggFunc::Min, col: c(0, 1) },
            PlanAggregate { func: AggFunc::Max, col: c(0, 1) },
        ]);
        assert_eq!(result.len(), 5);
        assert_eq!(result[1][0], CellValue::I64(2));
        assert_eq!(result[2][0], CellValue::I64(40));
        assert_eq!(result[3][0], CellValue::I64(10));
        assert_eq!(result[4][0], CellValue::I64(30));
    }

    #[test]
    fn test_aggregate_null_handling() {
        let mut ctx = ExecutionContext::new();
        let mut table = make_test_table();
        table.insert(&[CellValue::Str("A".into()), CellValue::I64(10)]).unwrap();
        table.insert(&[CellValue::Str("A".into()), CellValue::Null]).unwrap();
        table.insert(&[CellValue::Str("B".into()), CellValue::Null]).unwrap();
        let rs = RowSet::from_scan(&table, vec![0, 1, 2]);
        let result = aggregate_rowset(&mut ctx, &rs, &[c(0, 0)], &[
            PlanAggregate { func: AggFunc::Count, col: c(0, 1) },
            PlanAggregate { func: AggFunc::Sum, col: c(0, 1) },
        ]);
        assert_eq!(result[1][0], CellValue::I64(1));
        assert_eq!(result[2][0], CellValue::I64(10));
        assert_eq!(result[1][1], CellValue::I64(0));
        assert_eq!(result[2][1], CellValue::Null);
    }
}
