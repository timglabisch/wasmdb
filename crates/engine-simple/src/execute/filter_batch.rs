//! Columnar batch predicate evaluation (single-table fast path).
//!
//! Operates directly on typed column arrays ([`TypedColumn`]), avoiding per-row
//! [`CellValue`] allocation. Returns filtered row IDs as `Vec<usize>`.
//!
//! Only works for single-source predicates (after pushdown to `pre_filter`).
//! For the general row-wise path that works across tables, see [`super::filter_row`].

use std::collections::HashSet;

use crate::planner::plan::PlanFilterPredicate;
use crate::storage::{CellValue, Table, TypedColumn};
use query_engine::ast::Value;

use super::value_to_cell;

/// Normalized value for typed comparison — avoids repeating Bool/Float→I64 conversion.
enum NormalizedValue<'a> {
    I64(i64),
    Str(&'a str),
    Null,
}

fn normalize_value<'a>(v: &'a Value) -> NormalizedValue<'a> {
    match v {
        Value::Int(n) => NormalizedValue::I64(*n),
        Value::Text(s) => NormalizedValue::Str(s),
        Value::Null => NormalizedValue::Null,
        Value::Bool(b) => NormalizedValue::I64(if *b { 1 } else { 0 }),
        Value::Float(f) => NormalizedValue::I64(*f as i64),
    }
}

impl PlanFilterPredicate {
    /// Filter row IDs by evaluating this predicate against a single table's columns.
    ///
    /// This is the fast path: it reads typed column arrays directly, without
    /// boxing each cell into [`CellValue`]. Only usable for predicates that
    /// reference a single table (i.e. after pushdown to `pre_filter`).
    ///
    /// For the general row-wise path, see [`super::filter_row::filter_row`].
    pub fn filter_batch(&self, table: &Table, row_ids: &[usize]) -> Vec<usize> {
        match self {
            PlanFilterPredicate::None => row_ids.to_vec(),

            PlanFilterPredicate::And(l, r) => {
                let left_ids = l.filter_batch(table, row_ids);
                r.filter_batch(table, &left_ids)
            }
            PlanFilterPredicate::Or(l, r) => {
                let left_ids = l.filter_batch(table, row_ids);
                let right_ids = r.filter_batch(table, row_ids);
                sorted_union(&left_ids, &right_ids)
            }

            _ => self.filter_batch_leaf(table, row_ids),
        }
    }

    fn filter_batch_leaf(&self, table: &Table, row_ids: &[usize]) -> Vec<usize> {
        match self {
            PlanFilterPredicate::Equals { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Eq)
            }
            PlanFilterPredicate::NotEquals { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Neq)
            }
            PlanFilterPredicate::GreaterThan { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Gt)
            }
            PlanFilterPredicate::GreaterThanOrEqual { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Gte)
            }
            PlanFilterPredicate::LessThan { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Lt)
            }
            PlanFilterPredicate::LessThanOrEqual { col, value } => {
                filter_typed_cmp(&table.columns[col.col], row_ids, value, CmpOp::Lte)
            }
            PlanFilterPredicate::ColumnEquals { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Eq)
            }
            PlanFilterPredicate::ColumnNotEquals { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Neq)
            }
            PlanFilterPredicate::ColumnGreaterThan { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Gt)
            }
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Gte)
            }
            PlanFilterPredicate::ColumnLessThan { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Lt)
            }
            PlanFilterPredicate::ColumnLessThanOrEqual { left, right } => {
                filter_typed_col_col(&table.columns[left.col], &table.columns[right.col], row_ids, CmpOp::Lte)
            }
            PlanFilterPredicate::IsNull { col } => {
                filter_typed_is_null(&table.columns[col.col], row_ids, true)
            }
            PlanFilterPredicate::IsNotNull { col } => {
                filter_typed_is_null(&table.columns[col.col], row_ids, false)
            }
            PlanFilterPredicate::In { col, values } => {
                filter_typed_in(&table.columns[col.col], row_ids, values)
            }
            PlanFilterPredicate::InMaterialized { .. }
            | PlanFilterPredicate::CompareMaterialized { .. } => {
                unreachable!("must be resolved before execution")
            }
            _ => unreachable!(),
        }
    }
}

// ── Typed column helpers ────────────────────────────────────────────────────

use super::filter_row::CmpOp;

fn filter_typed_cmp(
    col: &TypedColumn,
    row_ids: &[usize],
    value: &Value,
    op: CmpOp,
) -> Vec<usize> {
    match normalize_value(value) {
        NormalizedValue::Null => Vec::new(),
        NormalizedValue::I64(n) => match col {
            TypedColumn::I64(data) => {
                row_ids.iter().filter(|&&i| cmp_i64(data[i], n, op)).copied().collect()
            }
            TypedColumn::NullableI64 { values, nulls } => {
                row_ids.iter().filter(|&&i| !nulls.get(i) && cmp_i64(values[i], n, op)).copied().collect()
            }
            _ => Vec::new(),
        },
        NormalizedValue::Str(s) => match col {
            TypedColumn::Str(data) => {
                row_ids.iter().filter(|&&i| cmp_str(&data[i], s, op)).copied().collect()
            }
            TypedColumn::NullableStr { values, nulls } => {
                row_ids.iter().filter(|&&i| !nulls.get(i) && cmp_str(&values[i], s, op)).copied().collect()
            }
            _ => Vec::new(),
        },
    }
}

fn filter_typed_col_col(
    left: &TypedColumn,
    right: &TypedColumn,
    row_ids: &[usize],
    op: CmpOp,
) -> Vec<usize> {
    match (left, right) {
        (TypedColumn::I64(l), TypedColumn::I64(r)) => {
            row_ids.iter().filter(|&&i| cmp_i64(l[i], r[i], op)).copied().collect()
        }
        (TypedColumn::Str(l), TypedColumn::Str(r)) => {
            row_ids.iter().filter(|&&i| cmp_str(&l[i], &r[i], op)).copied().collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && !rn.get(i) && cmp_i64(lv[i], rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && !rn.get(i) && cmp_str(&lv[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::I64(l), TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !rn.get(i) && cmp_i64(l[i], rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::I64(r)) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && cmp_i64(lv[i], r[i], op)).copied().collect()
        }
        (TypedColumn::Str(l), TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !rn.get(i) && cmp_str(&l[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::Str(r)) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && cmp_str(&lv[i], &r[i], op)).copied().collect()
        }
        _ => Vec::new(),
    }
}

fn filter_typed_in(col: &TypedColumn, row_ids: &[usize], values: &[Value]) -> Vec<usize> {
    let cell_values: HashSet<CellValue> = values.iter().map(|v| value_to_cell(v)).collect();
    row_ids.iter().filter(|&&i| {
        let cell = col.get(i);
        !matches!(cell, CellValue::Null) && cell_values.contains(&cell)
    }).copied().collect()
}

fn filter_typed_is_null(col: &TypedColumn, row_ids: &[usize], want_null: bool) -> Vec<usize> {
    match col {
        TypedColumn::I64(_) | TypedColumn::Str(_) => {
            if want_null { Vec::new() } else { row_ids.to_vec() }
        }
        TypedColumn::NullableI64 { nulls, .. } => {
            row_ids.iter().filter(|&&i| nulls.get(i) == want_null).copied().collect()
        }
        TypedColumn::NullableStr { nulls, .. } => {
            row_ids.iter().filter(|&&i| nulls.get(i) == want_null).copied().collect()
        }
    }
}

#[inline]
fn cmp_i64(left: i64, right: i64, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Neq => left != right,
        CmpOp::Lt => left < right,
        CmpOp::Lte => left <= right,
        CmpOp::Gt => left > right,
        CmpOp::Gte => left >= right,
    }
}

#[inline]
fn cmp_str(left: &str, right: &str, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Neq => left != right,
        CmpOp::Lt => left < right,
        CmpOp::Lte => left <= right,
        CmpOp::Gt => left > right,
        CmpOp::Gte => left >= right,
    }
}

fn sorted_union(a: &[usize], b: &[usize]) -> Vec<usize> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => { result.push(a[i]); i += 1; }
            std::cmp::Ordering::Greater => { result.push(b[j]); j += 1; }
            std::cmp::Ordering::Equal => { result.push(a[i]); i += 1; j += 1; }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::ColumnRef;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    fn make_i64_table(name: &str, col_name: &str, values: &[i64]) -> Table {
        let schema = TableSchema {
            name: name.into(),
            columns: vec![
                ColumnSchema { name: col_name.into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        for &v in values {
            t.insert(&[CellValue::I64(v)]).unwrap();
        }
        t
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
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::Null]).unwrap();
        t
    }

    fn c(source: usize, col: usize) -> ColumnRef {
        ColumnRef { source, col }
    }

    #[test]
    fn test_filter_batch_equals_i64() {
        let table = make_i64_table("t", "x", &[1, 2, 3, 2, 5]);
        let row_ids: Vec<usize> = (0..5).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_filter_batch_greater_than() {
        let table = make_i64_table("t", "x", &[1, 5, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(2) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![1, 2]);
    }

    #[test]
    fn test_filter_batch_string_equals() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Text("Bob".into()) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_nullable_skips_null() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(20) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![0, 1]);
    }

    #[test]
    fn test_filter_batch_is_null() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNull { col: c(0, 2) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![2]);
    }

    #[test]
    fn test_filter_batch_is_not_null_on_non_nullable() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNotNull { col: c(0, 0) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![0, 1, 2]);
    }

    #[test]
    fn test_filter_batch_column_equals() {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[CellValue::I64(1), CellValue::I64(1)]).unwrap();
        table.insert(&[CellValue::I64(2), CellValue::I64(9)]).unwrap();
        table.insert(&[CellValue::I64(3), CellValue::I64(3)]).unwrap();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(0, 1) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_and() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(1) }),
            Box::new(PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Text("Bob".into()) }),
        );
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_subset_row_ids() {
        let table = make_i64_table("t", "x", &[10, 20, 30, 40, 50]);
        let row_ids = vec![1, 3];
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(25) };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![3]);
    }

    #[test]
    fn test_filter_batch_null_value_always_false() {
        let table = make_i64_table("t", "x", &[1, 2, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Null };
        assert_eq!(pred.filter_batch(&table, &row_ids), Vec::<usize>::new());
    }

    #[test]
    fn test_filter_batch_in_i64() {
        let table = make_i64_table("t", "x", &[1, 2, 3, 4, 5]);
        let row_ids: Vec<usize> = (0..5).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Int(2), Value::Int(4)],
        };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_filter_batch_in_string() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 1),
            values: vec![Value::Text("Alice".into()), Value::Text("Carol".into())],
        };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_in_null_skipped() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 2),
            values: vec![Value::Int(25), Value::Int(30)],
        };
        assert_eq!(pred.filter_batch(&table, &row_ids), vec![0, 1]);
    }

    #[test]
    fn test_filter_batch_in_empty() {
        let table = make_i64_table("t", "x", &[1, 2, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![],
        };
        assert_eq!(pred.filter_batch(&table, &row_ids), Vec::<usize>::new());
    }
}
