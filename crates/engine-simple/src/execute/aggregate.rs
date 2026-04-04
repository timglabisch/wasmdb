use std::collections::HashMap;

use crate::planner::plan::PlanAggregate;
use crate::storage::CellValue;
use query_engine::ast::AggFunc;

use super::{num_rows, Columns};

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

/// Aggregate directly from a RowSet without materializing intermediate columns.
pub fn aggregate_rowset(
    rs: &super::RowSet,
    group_by: &[usize],
    aggregates: &[PlanAggregate],
) -> Columns {
    let mut groups: HashMap<Vec<CellValue>, Vec<Accumulator>> = HashMap::new();
    let mut group_order: Vec<Vec<CellValue>> = Vec::new();

    for row in 0..rs.num_rows {
        let key: Vec<CellValue> = group_by.iter().map(|&ci| rs.get(row, ci)).collect();

        let accums = groups.entry(key.clone()).or_insert_with(|| {
            group_order.push(key.clone());
            aggregates.iter().map(|agg| Accumulator::new(agg.func)).collect()
        });

        for (ai, agg) in aggregates.iter().enumerate() {
            accums[ai].feed(&rs.get(row, agg.column_idx));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_count_sum_min_max() {
        let cols: Columns = vec![
            vec![CellValue::Str("A".into()), CellValue::Str("A".into()), CellValue::Str("B".into())],
            vec![CellValue::I64(10), CellValue::I64(30), CellValue::I64(20)],
        ];

        let result = aggregate(
            &cols,
            &[0],
            &[
                PlanAggregate { func: AggFunc::Count, column_idx: 1 },
                PlanAggregate { func: AggFunc::Sum, column_idx: 1 },
                PlanAggregate { func: AggFunc::Min, column_idx: 1 },
                PlanAggregate { func: AggFunc::Max, column_idx: 1 },
            ],
        );

        assert_eq!(result.len(), 5);
        assert_eq!(result[0][0], CellValue::Str("A".into()));
        assert_eq!(result[1][0], CellValue::I64(2));
        assert_eq!(result[2][0], CellValue::I64(40));
        assert_eq!(result[3][0], CellValue::I64(10));
        assert_eq!(result[4][0], CellValue::I64(30));
        assert_eq!(result[0][1], CellValue::Str("B".into()));
        assert_eq!(result[1][1], CellValue::I64(1));
        assert_eq!(result[2][1], CellValue::I64(20));
        assert_eq!(result[3][1], CellValue::I64(20));
        assert_eq!(result[4][1], CellValue::I64(20));
    }
}
