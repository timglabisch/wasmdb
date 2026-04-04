use crate::planner::plan::PlanFilterPredicate;
use crate::storage::{CellValue, Table, TypedColumn};
use query_engine::ast::Value;

use super::value_to_cell;

#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

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
    /// Evaluate predicate on Table storage, returning matching row IDs.
    /// For And: short-circuit — right side only evaluates survivors of left.
    pub fn eval_table(&self, table: &Table, row_ids: &[usize]) -> Vec<usize> {
        match self {
            PlanFilterPredicate::None => row_ids.to_vec(),

            PlanFilterPredicate::And(l, r) => {
                let left_ids = l.eval_table(table, row_ids);
                r.eval_table(table, &left_ids)
            }
            PlanFilterPredicate::Or(l, r) => {
                let left_ids = l.eval_table(table, row_ids);
                let right_ids = r.eval_table(table, row_ids);
                sorted_union(&left_ids, &right_ids)
            }

            // Leaf predicates: directly produce matching row_ids (no Vec<bool> intermediate)
            _ => self.eval_leaf_filtered(table, row_ids),
        }
    }


    /// Directly produce matching row_ids for leaf predicates (gather path, no Vec<bool> intermediate).
    fn eval_leaf_filtered(&self, table: &Table, row_ids: &[usize]) -> Vec<usize> {
        match self {
            PlanFilterPredicate::Equals { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Eq)
            }
            PlanFilterPredicate::NotEquals { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Neq)
            }
            PlanFilterPredicate::GreaterThan { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Gt)
            }
            PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Gte)
            }
            PlanFilterPredicate::LessThan { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Lt)
            }
            PlanFilterPredicate::LessThanOrEqual { column_idx, value } => {
                filter_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Lte)
            }
            PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Eq)
            }
            PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Neq)
            }
            PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Gt)
            }
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Gte)
            }
            PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Lt)
            }
            PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => {
                filter_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Lte)
            }
            PlanFilterPredicate::IsNull { column_idx } => {
                filter_typed_is_null(&table.columns[*column_idx], row_ids, true)
            }
            PlanFilterPredicate::IsNotNull { column_idx } => {
                filter_typed_is_null(&table.columns[*column_idx], row_ids, false)
            }
            _ => unreachable!(),
        }
    }


}

// --- Typed helpers for eval_table: directly produce Vec<usize> (no Vec<bool> intermediate) ---

/// Filter row_ids by comparing a TypedColumn against a literal Value.
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

/// Filter row_ids by comparing two TypedColumns.
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

/// Filter row_ids by IS NULL / IS NOT NULL.
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


/// Merge two sorted, deduplicated slices into a sorted, deduplicated Vec.
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

// --- Generic predicate evaluation on a cell accessor (no materialization) ---

/// Evaluate a predicate using a closure that resolves column index → CellValue.
/// Used by both RowSet filter and join evaluation.
pub fn eval_pred_row<F: Fn(usize) -> CellValue>(pred: &PlanFilterPredicate, get: &F) -> bool {
    match pred {
        PlanFilterPredicate::None => true,
        PlanFilterPredicate::Equals { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Eq),
        PlanFilterPredicate::NotEquals { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Neq),
        PlanFilterPredicate::GreaterThan { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Gt),
        PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Gte),
        PlanFilterPredicate::LessThan { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Lt),
        PlanFilterPredicate::LessThanOrEqual { column_idx, value } => cmp_cell(&get(*column_idx), &value_to_cell(value), CmpOp::Lte),
        PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Eq),
        PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Neq),
        PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Gt),
        PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Gte),
        PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Lt),
        PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => cmp_cell(&get(*left_idx), &get(*right_idx), CmpOp::Lte),
        PlanFilterPredicate::IsNull { column_idx } => matches!(get(*column_idx), CellValue::Null),
        PlanFilterPredicate::IsNotNull { column_idx } => !matches!(get(*column_idx), CellValue::Null),
        PlanFilterPredicate::And(a, b) => eval_pred_row(a, get) && eval_pred_row(b, get),
        PlanFilterPredicate::Or(a, b) => eval_pred_row(a, get) || eval_pred_row(b, get),
    }
}

/// Evaluate predicate on a single RowSet row.
pub fn eval_rowset_row(pred: &PlanFilterPredicate, rs: &super::RowSet, row: usize) -> bool {
    eval_pred_row(pred, &|col| rs.get(row, col))
}

/// Evaluate join predicate: left columns from RowSet, right columns from Table.
pub fn eval_join_row(
    pred: &PlanFilterPredicate,
    left: &super::RowSet,
    right_table: &Table,
    right_col_offset: usize,
    l: usize,
    r: usize,
) -> bool {
    eval_pred_row(pred, &|col| {
        if col < right_col_offset {
            left.get(l, col)
        } else {
            right_table.get(r, col - right_col_offset)
        }
    })
}

// --- Shared utility ---

/// SQL comparison semantics: any comparison involving NULL returns false.
pub fn cmp_cell(left: &CellValue, right: &CellValue, op: CmpOp) -> bool {
    if matches!(left, CellValue::Null) || matches!(right, CellValue::Null) {
        return false;
    }
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Neq => left != right,
        CmpOp::Lt => left < right,
        CmpOp::Lte => left <= right,
        CmpOp::Gt => left > right,
        CmpOp::Gte => left >= right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // --- eval_table tests (now returns Vec<usize> of matching row IDs) ---

    #[test]
    fn test_eval_table_equals_i64() {
        let table = make_i64_table("t", "x", &[1, 2, 3, 2, 5]);
        let row_ids: Vec<usize> = (0..5).collect();
        let pred = PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(2) };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![1, 3]);
    }

    #[test]
    fn test_eval_table_greater_than() {
        let table = make_i64_table("t", "x", &[1, 5, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(2) };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![1, 2]);
    }

    #[test]
    fn test_eval_table_string_equals() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { column_idx: 1, value: Value::Text("Bob".into()) };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![1]);
    }

    #[test]
    fn test_eval_table_nullable_skips_null() {
        let table = make_users_table(); // age: 30, 25, NULL
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::GreaterThan { column_idx: 2, value: Value::Int(20) };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![0, 1]);
    }

    #[test]
    fn test_eval_table_is_null() {
        let table = make_users_table(); // age: 30, 25, NULL
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::IsNull { column_idx: 2 };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![2]);
    }

    #[test]
    fn test_eval_table_is_not_null_on_non_nullable() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        // id column is non-nullable → IS NOT NULL always true
        let pred = PlanFilterPredicate::IsNotNull { column_idx: 0 };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![0, 1, 2]);
    }

    #[test]
    fn test_eval_table_column_equals() {
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
        let pred = PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 1 };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![0, 2]);
    }

    #[test]
    fn test_eval_table_and() {
        let table = make_users_table();
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(1) }),
            Box::new(PlanFilterPredicate::Equals { column_idx: 1, value: Value::Text("Bob".into()) }),
        );
        assert_eq!(pred.eval_table(&table, &row_ids), vec![1]);
    }

    #[test]
    fn test_eval_table_subset_row_ids() {
        let table = make_i64_table("t", "x", &[10, 20, 30, 40, 50]);
        // Only evaluate rows 1 and 3
        let row_ids = vec![1, 3];
        let pred = PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(25) };
        assert_eq!(pred.eval_table(&table, &row_ids), vec![3]); // 20>25=F, 40>25=T
    }

    #[test]
    fn test_eval_table_null_value_always_false() {
        let table = make_i64_table("t", "x", &[1, 2, 3]);
        let row_ids: Vec<usize> = (0..3).collect();
        let pred = PlanFilterPredicate::Equals { column_idx: 0, value: Value::Null };
        assert_eq!(pred.eval_table(&table, &row_ids), Vec::<usize>::new());
    }

}
