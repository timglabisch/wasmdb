//! Columnar batch predicate evaluation (single-table fast path).
//!
//! Operates directly on typed column arrays ([`TypedColumn`]), avoiding per-row
//! [`CellValue`] allocation. Returns filtered row IDs as `Vec<usize>`.
//!
//! Only works for single-source predicates (after pushdown to `pre_filter`).
//! For the general row-wise path that works across tables, see [`super::filter_row`].

use std::collections::HashSet;

use crate::planner::shared::plan::PlanFilterPredicate;
use crate::storage::{CellValue, Table, TypedColumn};
use sql_parser::ast::Value;

use super::value_to_cell;
use super::ExecutionContext;

/// Normalized value for typed comparison — avoids repeating Bool/Float->I64 conversion.
enum NormalizedValue<'a> {
    I64(i64),
    Str(&'a str),
    Uuid([u8; 16]),
    Null,
}

fn normalize_value<'a>(v: &'a Value) -> NormalizedValue<'a> {
    match v {
        Value::Int(n) => NormalizedValue::I64(*n),
        Value::Text(s) => NormalizedValue::Str(s),
        Value::Uuid(b) => NormalizedValue::Uuid(*b),
        Value::Null => NormalizedValue::Null,
        Value::Bool(b) => NormalizedValue::I64(if *b { 1 } else { 0 }),
        Value::Float(f) => NormalizedValue::I64(*f as i64),
        Value::Placeholder(name) => panic!("unresolved placeholder :{name} — must bind before execution"),
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
    pub fn filter_batch(&self, ctx: &mut ExecutionContext, table: &Table, row_ids: &[usize]) -> Vec<usize> {
        match self {
            PlanFilterPredicate::None => row_ids.to_vec(),

            PlanFilterPredicate::And(l, r) => {
                let left_ids = l.filter_batch(ctx, table, row_ids);
                r.filter_batch(ctx, table, &left_ids)
            }
            PlanFilterPredicate::Or(l, r) => {
                let left_ids = l.filter_batch(ctx, table, row_ids);
                let right_ids = r.filter_batch(ctx, table, row_ids);
                sorted_union(&left_ids, &right_ids)
            }

            _ => self.filter_batch_leaf(ctx, table, row_ids),
        }
    }

    fn filter_batch_leaf(&self, _ctx: &mut ExecutionContext, table: &Table, row_ids: &[usize]) -> Vec<usize> {
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
                row_ids.iter().filter(|&&i| cmp_ord(&data[i], &n, op)).copied().collect()
            }
            TypedColumn::NullableI64 { values, nulls } => {
                row_ids.iter().filter(|&&i| !nulls.get(i) && cmp_ord(&values[i], &n, op)).copied().collect()
            }
            _ => Vec::new(),
        },
        NormalizedValue::Str(s) => match col {
            TypedColumn::Str(data) => {
                row_ids.iter().filter(|&&i| cmp_ord(data[i].as_str(), s, op)).copied().collect()
            }
            TypedColumn::NullableStr { values, nulls } => {
                row_ids.iter().filter(|&&i| !nulls.get(i) && cmp_ord(values[i].as_str(), s, op)).copied().collect()
            }
            _ => Vec::new(),
        },
        NormalizedValue::Uuid(needle) => match col {
            TypedColumn::Uuid(data) => {
                row_ids.iter().filter(|&&i| cmp_ord(&data[i], &needle, op)).copied().collect()
            }
            TypedColumn::NullableUuid { values, nulls } => {
                row_ids.iter().filter(|&&i| !nulls.get(i) && cmp_ord(&values[i], &needle, op)).copied().collect()
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
            row_ids.iter().filter(|&&i| cmp_ord(&l[i], &r[i], op)).copied().collect()
        }
        (TypedColumn::Str(l), TypedColumn::Str(r)) => {
            row_ids.iter().filter(|&&i| cmp_ord(l[i].as_str(), r[i].as_str(), op)).copied().collect()
        }
        (TypedColumn::Uuid(l), TypedColumn::Uuid(r)) => {
            row_ids.iter().filter(|&&i| cmp_ord(&l[i], &r[i], op)).copied().collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && !rn.get(i) && cmp_ord(&lv[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && !rn.get(i) && cmp_ord(lv[i].as_str(), rv[i].as_str(), op)).copied().collect()
        }
        (TypedColumn::NullableUuid { values: lv, nulls: ln }, TypedColumn::NullableUuid { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && !rn.get(i) && cmp_ord(&lv[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::I64(l), TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !rn.get(i) && cmp_ord(&l[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::I64(r)) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && cmp_ord(&lv[i], &r[i], op)).copied().collect()
        }
        (TypedColumn::Str(l), TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !rn.get(i) && cmp_ord(l[i].as_str(), rv[i].as_str(), op)).copied().collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::Str(r)) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && cmp_ord(lv[i].as_str(), r[i].as_str(), op)).copied().collect()
        }
        (TypedColumn::Uuid(l), TypedColumn::NullableUuid { values: rv, nulls: rn }) => {
            row_ids.iter().filter(|&&i| !rn.get(i) && cmp_ord(&l[i], &rv[i], op)).copied().collect()
        }
        (TypedColumn::NullableUuid { values: lv, nulls: ln }, TypedColumn::Uuid(r)) => {
            row_ids.iter().filter(|&&i| !ln.get(i) && cmp_ord(&lv[i], &r[i], op)).copied().collect()
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
        TypedColumn::I64(_) | TypedColumn::Str(_) | TypedColumn::Uuid(_) => {
            if want_null { Vec::new() } else { row_ids.to_vec() }
        }
        TypedColumn::NullableI64 { nulls, .. } => {
            row_ids.iter().filter(|&&i| nulls.get(i) == want_null).copied().collect()
        }
        TypedColumn::NullableStr { nulls, .. } => {
            row_ids.iter().filter(|&&i| nulls.get(i) == want_null).copied().collect()
        }
        TypedColumn::NullableUuid { nulls, .. } => {
            row_ids.iter().filter(|&&i| nulls.get(i) == want_null).copied().collect()
        }
    }
}

#[inline]
fn cmp_ord<T: ?Sized + Ord>(left: &T, right: &T, op: CmpOp) -> bool {
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
    use std::collections::HashMap;
    use crate::planner::shared::plan::ColumnRef;
    use crate::schema::{ColumnSchema, DataType, TableSchema};

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
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[1, 2, 3, 2, 5]);
        let row_ids: Vec<usize> = (0..5).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_filter_batch_greater_than() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[1, 5, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(2) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1, 2]);
    }

    #[test]
    fn test_filter_batch_string_equals() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Text("Bob".into()) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_nullable_skips_null() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(20) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 1]);
    }

    #[test]
    fn test_filter_batch_is_null() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNull { col: c(0, 2) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![2]);
    }

    #[test]
    fn test_filter_batch_is_not_null_on_non_nullable() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNotNull { col: c(0, 0) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 1, 2]);
    }

    #[test]
    fn test_filter_batch_column_equals() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
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
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_and() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(1) }),
            Box::new(PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Text("Bob".into()) }),
        );
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_subset_row_ids() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[10, 20, 30, 40, 50]);
        let row_ids = vec![1, 3];
        let pred = PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Int(25) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![3]);
    }

    #[test]
    fn test_filter_batch_null_value_always_false() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[1, 2, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Null };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), Vec::<usize>::new());
    }

    #[test]
    fn test_filter_batch_in_i64() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[1, 2, 3, 4, 5]);
        let row_ids: Vec<usize> = (0..5).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Int(2), Value::Int(4)],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_filter_batch_in_string() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 1),
            values: vec![Value::Text("Alice".into()), Value::Text("Carol".into())],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_in_null_skipped() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 2),
            values: vec![Value::Int(25), Value::Int(30)],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 1]);
    }

    fn make_uuid_table(values: &[[u8; 16]]) -> Table {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::Uuid, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        for v in values {
            t.insert(&[CellValue::Uuid(*v)]).unwrap();
        }
        t
    }

    fn make_nullable_uuid_table(values: &[Option<[u8; 16]>]) -> Table {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "external".into(), data_type: DataType::Uuid, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = Table::new(schema);
        for (i, v) in values.iter().enumerate() {
            let cell = match v {
                Some(b) => CellValue::Uuid(*b),
                None => CellValue::Null,
            };
            t.insert(&[CellValue::I64(i as i64), cell]).unwrap();
        }
        t
    }

    fn uuid_n(n: u8) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[15] = n;
        b
    }

    #[test]
    fn test_filter_batch_uuid_eq() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(2), uuid_n(3)]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Uuid(uuid_n(2)) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_uuid_neq() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(2), uuid_n(3)]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::NotEquals { col: c(0, 0), value: Value::Uuid(uuid_n(2)) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_lt_lex_order() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(5), uuid_n(0xa)]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::LessThan { col: c(0, 0), value: Value::Uuid(uuid_n(5)) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0]);
    }

    #[test]
    fn test_filter_batch_uuid_gte() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(5), uuid_n(0xa)]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThanOrEqual {
            col: c(0, 0),
            value: Value::Uuid(uuid_n(5)),
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_in_list() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(2), uuid_n(3), uuid_n(4)]);
        let row_ids: Vec<usize> = (0..4).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![Value::Uuid(uuid_n(2)), Value::Uuid(uuid_n(4))],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_filter_batch_uuid_in_skips_null() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_nullable_uuid_table(&[Some(uuid_n(1)), None, Some(uuid_n(2))]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 1),
            values: vec![Value::Uuid(uuid_n(1)), Value::Uuid(uuid_n(2))],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_is_null() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_nullable_uuid_table(&[Some(uuid_n(1)), None, Some(uuid_n(2))]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNull { col: c(0, 1) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_uuid_is_not_null() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_nullable_uuid_table(&[Some(uuid_n(1)), None, Some(uuid_n(2))]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNotNull { col: c(0, 1) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_column_equals() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::Uuid, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::Uuid, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[CellValue::Uuid(uuid_n(1)), CellValue::Uuid(uuid_n(1))]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(2)), CellValue::Uuid(uuid_n(9))]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(3)), CellValue::Uuid(uuid_n(3))]).unwrap();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(0, 1) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_column_equals_with_nullable() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::Uuid, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::Uuid, nullable: true },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[CellValue::Uuid(uuid_n(1)), CellValue::Uuid(uuid_n(1))]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(2)), CellValue::Null]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(3)), CellValue::Uuid(uuid_n(3))]).unwrap();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(0, 1) };
        // Row 1 must be excluded — NULL never compares equal under SQL semantics.
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_filter_batch_uuid_vs_str_column_returns_empty() {
        // Cross-type column comparison returns no rows (not an error).
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnSchema { name: "a".into(), data_type: DataType::Uuid, nullable: false },
                ColumnSchema { name: "b".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[CellValue::Uuid(uuid_n(1)), CellValue::Str("foo".into())]).unwrap();
        let row_ids: Vec<usize> = (0..1).collect();
        let pred = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(0, 1) };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), Vec::<usize>::new());
    }

    #[test]
    fn test_filter_batch_uuid_vs_str_literal_returns_empty() {
        // A `Value::Text` against a UUID column finds nothing — coercion is
        // explicitly NOT performed; callers must use `UUID 'xxx'`.
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1)]);
        let row_ids: Vec<usize> = (0..1).collect();
        let pred = PlanFilterPredicate::Equals {
            col: c(0, 0),
            value: Value::Text("00000000-0000-0000-0000-000000000001".into()),
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), Vec::<usize>::new());
    }

    #[test]
    fn test_filter_batch_uuid_null_value_always_false() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_uuid_table(&[uuid_n(1), uuid_n(2)]);
        let row_ids: Vec<usize> = (0..2).collect();
        let pred = PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Null };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), Vec::<usize>::new());
    }

    #[test]
    fn test_filter_batch_uuid_and_str_combined() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let schema = TableSchema {
            name: "customers".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::Uuid, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut table = Table::new(schema);
        table.insert(&[CellValue::Uuid(uuid_n(1)), CellValue::Str("Alice".into())]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(2)), CellValue::Str("Alice".into())]).unwrap();
        table.insert(&[CellValue::Uuid(uuid_n(3)), CellValue::Str("Bob".into())]).unwrap();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Text("Alice".into()) }),
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 0), value: Value::Uuid(uuid_n(1)) }),
        );
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), vec![1]);
    }

    #[test]
    fn test_filter_batch_in_empty() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_i64_table("t", "x", &[1, 2, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::In {
            col: c(0, 0),
            values: vec![],
        };
        assert_eq!(pred.filter_batch(&mut ctx, &table, &row_ids), Vec::<usize>::new());
    }
}
