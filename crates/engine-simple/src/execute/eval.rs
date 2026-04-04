use crate::planner::plan::PlanFilterPredicate;
use crate::storage::{CellValue, Table, TypedColumn};
use query_engine::ast::Value;

use super::{num_rows, value_to_cell, Columns};

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

            // Leaf predicates: produce bool mask, then select matching row_ids
            _ => {
                let mask = self.eval_leaf_mask(table, row_ids);
                row_ids.iter().zip(mask).filter(|(_, keep)| *keep).map(|(&id, _)| id).collect()
            }
        }
    }

    /// Full-scan entry point: evaluate predicate on the entire contiguous table
    /// storage (SIMD-friendly), applying the deleted bitmap as a post-filter.
    pub fn eval_full_scan(&self, table: &Table) -> Vec<usize> {
        let n = table.physical_len();
        let deleted = table.deleted_bitmap();
        match self {
            PlanFilterPredicate::None => {
                deleted.iter_zeros().filter(|&i| i < n).collect()
            }

            PlanFilterPredicate::And(l, r) => {
                let left_ids = l.eval_full_scan(table);
                r.eval_table(table, &left_ids)
            }
            PlanFilterPredicate::Or(l, r) => {
                let left_ids = l.eval_full_scan(table);
                let right_ids = r.eval_full_scan(table);
                sorted_union(&left_ids, &right_ids)
            }

            // Leaf predicates: full-scan contiguous slice, apply deleted mask
            _ => {
                let mask = self.eval_full_scan_mask(table);
                (0..n).filter(|&i| mask[i] && !deleted.get(i)).collect()
            }
        }
    }

    /// Produce a bool mask for leaf predicates using row_ids indirection (gather path).
    fn eval_leaf_mask(&self, table: &Table, row_ids: &[usize]) -> Vec<bool> {
        match self {
            PlanFilterPredicate::Equals { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Eq)
            }
            PlanFilterPredicate::NotEquals { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Neq)
            }
            PlanFilterPredicate::GreaterThan { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Gt)
            }
            PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Gte)
            }
            PlanFilterPredicate::LessThan { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Lt)
            }
            PlanFilterPredicate::LessThanOrEqual { column_idx, value } => {
                eval_typed_cmp(&table.columns[*column_idx], row_ids, value, CmpOp::Lte)
            }
            PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Eq)
            }
            PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Neq)
            }
            PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Gt)
            }
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Gte)
            }
            PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Lt)
            }
            PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => {
                eval_typed_col_col(&table.columns[*left_idx], &table.columns[*right_idx], row_ids, CmpOp::Lte)
            }
            PlanFilterPredicate::IsNull { column_idx } => {
                eval_typed_is_null(&table.columns[*column_idx], row_ids, true)
            }
            PlanFilterPredicate::IsNotNull { column_idx } => {
                eval_typed_is_null(&table.columns[*column_idx], row_ids, false)
            }
            // And/Or/None handled in eval_table, not here
            _ => unreachable!(),
        }
    }

    /// Produce a bool mask for all physical rows (contiguous scan, SIMD-friendly).
    /// Does NOT apply the deleted bitmap — caller must do that.
    fn eval_full_scan_mask(&self, table: &Table) -> Vec<bool> {
        let n = table.physical_len();
        match self {
            PlanFilterPredicate::Equals { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Eq)
            }
            PlanFilterPredicate::NotEquals { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Neq)
            }
            PlanFilterPredicate::GreaterThan { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Gt)
            }
            PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Gte)
            }
            PlanFilterPredicate::LessThan { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Lt)
            }
            PlanFilterPredicate::LessThanOrEqual { column_idx, value } => {
                eval_full_scan_cmp(&table.columns[*column_idx], n, value, CmpOp::Lte)
            }
            PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Eq)
            }
            PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Neq)
            }
            PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Gt)
            }
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Gte)
            }
            PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Lt)
            }
            PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => {
                eval_full_scan_col_col(&table.columns[*left_idx], &table.columns[*right_idx], n, CmpOp::Lte)
            }
            PlanFilterPredicate::IsNull { column_idx } => {
                eval_full_scan_is_null(&table.columns[*column_idx], n, true)
            }
            PlanFilterPredicate::IsNotNull { column_idx } => {
                eval_full_scan_is_null(&table.columns[*column_idx], n, false)
            }
            // And/Or/None handled in eval_full_scan, not here
            _ => unreachable!(),
        }
    }

    /// Evaluate this predicate against materialized columnar data (post-join).
    pub fn eval_batch(&self, cols: &Columns) -> Vec<bool> {
        let n = num_rows(cols);
        match self {
            PlanFilterPredicate::None => vec![true; n],

            PlanFilterPredicate::Equals { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Eq)
            }
            PlanFilterPredicate::NotEquals { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Neq)
            }
            PlanFilterPredicate::GreaterThan { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Gt)
            }
            PlanFilterPredicate::GreaterThanOrEqual { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Gte)
            }
            PlanFilterPredicate::LessThan { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Lt)
            }
            PlanFilterPredicate::LessThanOrEqual { column_idx, value } => {
                eval_column_value_cmp(cols, *column_idx, value, CmpOp::Lte)
            }

            PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Eq)
            }
            PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Neq)
            }
            PlanFilterPredicate::ColumnGreaterThan { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Gt)
            }
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Gte)
            }
            PlanFilterPredicate::ColumnLessThan { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Lt)
            }
            PlanFilterPredicate::ColumnLessThanOrEqual { left_idx, right_idx } => {
                eval_column_column_cmp(cols, *left_idx, *right_idx, CmpOp::Lte)
            }

            PlanFilterPredicate::IsNull { column_idx } => eval_is_null(cols, *column_idx, true),
            PlanFilterPredicate::IsNotNull { column_idx } => eval_is_null(cols, *column_idx, false),

            PlanFilterPredicate::And(l, r) => {
                let lm = l.eval_batch(cols);
                let rm = r.eval_batch(cols);
                lm.iter().zip(rm.iter()).map(|(a, b)| *a && *b).collect()
            }
            PlanFilterPredicate::Or(l, r) => {
                let lm = l.eval_batch(cols);
                let rm = r.eval_batch(cols);
                lm.iter().zip(rm.iter()).map(|(a, b)| *a || *b).collect()
            }
        }
    }

    /// Evaluate this predicate for a single (left_row, right_row) pair
    /// in a join without materializing temporary columns.
    pub fn matches_join_row(
        &self,
        left: &Columns,
        right: &Columns,
        l: usize,
        r: usize,
    ) -> bool {
        match self {
            PlanFilterPredicate::None => true,
            PlanFilterPredicate::ColumnEquals { left_idx, right_idx } => {
                get_combined(left, right, l, r, *left_idx)
                    == get_combined(left, right, l, r, *right_idx)
            }
            PlanFilterPredicate::ColumnNotEquals { left_idx, right_idx } => {
                get_combined(left, right, l, r, *left_idx)
                    != get_combined(left, right, l, r, *right_idx)
            }
            PlanFilterPredicate::Equals { column_idx, value } => {
                let cell = get_combined(left, right, l, r, *column_idx);
                cell == value_to_cell(value)
            }
            PlanFilterPredicate::And(a, b) => {
                a.matches_join_row(left, right, l, r)
                    && b.matches_join_row(left, right, l, r)
            }
            PlanFilterPredicate::Or(a, b) => {
                a.matches_join_row(left, right, l, r)
                    || b.matches_join_row(left, right, l, r)
            }
            _other => {
                let combined = build_single_row(left, right, l, r);
                let mask = self.eval_batch(&combined);
                mask[0]
            }
        }
    }
}

// --- Typed helpers for eval_table (zero-copy, SIMD-ready) ---

/// Compare a TypedColumn against a literal Value for all given row IDs.
fn eval_typed_cmp(
    col: &TypedColumn,
    row_ids: &[usize],
    value: &Value,
    op: CmpOp,
) -> Vec<bool> {
    match normalize_value(value) {
        NormalizedValue::Null => vec![false; row_ids.len()],
        NormalizedValue::I64(n) => match col {
            TypedColumn::I64(data) => {
                row_ids.iter().map(|&i| cmp_i64(data[i], n, op)).collect()
            }
            TypedColumn::NullableI64 { values, nulls } => {
                row_ids.iter().map(|&i| {
                    if nulls.get(i) { false } else { cmp_i64(values[i], n, op) }
                }).collect()
            }
            _ => vec![false; row_ids.len()],
        },
        NormalizedValue::Str(s) => match col {
            TypedColumn::Str(data) => {
                row_ids.iter().map(|&i| cmp_str(&data[i], s, op)).collect()
            }
            TypedColumn::NullableStr { values, nulls } => {
                row_ids.iter().map(|&i| {
                    if nulls.get(i) { false } else { cmp_str(&values[i], s, op) }
                }).collect()
            }
            _ => vec![false; row_ids.len()],
        },
    }
}

/// Compare two TypedColumns for all given row IDs.
fn eval_typed_col_col(
    left: &TypedColumn,
    right: &TypedColumn,
    row_ids: &[usize],
    op: CmpOp,
) -> Vec<bool> {
    match (left, right) {
        (TypedColumn::I64(l), TypedColumn::I64(r)) => {
            row_ids.iter().map(|&i| cmp_i64(l[i], r[i], op)).collect()
        }
        (TypedColumn::Str(l), TypedColumn::Str(r)) => {
            row_ids.iter().map(|&i| cmp_str(&l[i], &r[i], op)).collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().map(|&i| {
                if ln.get(i) || rn.get(i) { false } else { cmp_i64(lv[i], rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().map(|&i| {
                if ln.get(i) || rn.get(i) { false } else { cmp_str(&lv[i], &rv[i], op) }
            }).collect()
        }
        (TypedColumn::I64(l), TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            row_ids.iter().map(|&i| {
                if rn.get(i) { false } else { cmp_i64(l[i], rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::I64(r)) => {
            row_ids.iter().map(|&i| {
                if ln.get(i) { false } else { cmp_i64(lv[i], r[i], op) }
            }).collect()
        }
        (TypedColumn::Str(l), TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            row_ids.iter().map(|&i| {
                if rn.get(i) { false } else { cmp_str(&l[i], &rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::Str(r)) => {
            row_ids.iter().map(|&i| {
                if ln.get(i) { false } else { cmp_str(&lv[i], &r[i], op) }
            }).collect()
        }
        // Cross-type (I64 vs Str) → always false
        _ => vec![false; row_ids.len()],
    }
}

/// IS NULL / IS NOT NULL directly on TypedColumn.
fn eval_typed_is_null(col: &TypedColumn, row_ids: &[usize], want_null: bool) -> Vec<bool> {
    match col {
        // Non-nullable columns are never null
        TypedColumn::I64(_) | TypedColumn::Str(_) => vec![!want_null; row_ids.len()],
        TypedColumn::NullableI64 { nulls, .. } => {
            row_ids.iter().map(|&i| nulls.get(i) == want_null).collect()
        }
        TypedColumn::NullableStr { nulls, .. } => {
            row_ids.iter().map(|&i| nulls.get(i) == want_null).collect()
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

// --- Full-scan helpers (contiguous slice access, SIMD-friendly) ---

/// Compare a TypedColumn against a literal for ALL physical rows (contiguous).
/// The returned Vec<bool> has one entry per physical row. Does NOT filter deleted rows.
fn eval_full_scan_cmp(col: &TypedColumn, n: usize, value: &Value, op: CmpOp) -> Vec<bool> {
    match normalize_value(value) {
        NormalizedValue::Null => vec![false; n],
        NormalizedValue::I64(scalar) => match col {
            // SIMD-friendly: contiguous &[i64] vs scalar
            TypedColumn::I64(data) => {
                data[..n].iter().map(|&v| cmp_i64(v, scalar, op)).collect()
            }
            TypedColumn::NullableI64 { values, nulls } => {
                values[..n].iter().enumerate().map(|(i, &v)| {
                    if nulls.get(i) { false } else { cmp_i64(v, scalar, op) }
                }).collect()
            }
            _ => vec![false; n],
        },
        NormalizedValue::Str(s) => match col {
            TypedColumn::Str(data) => {
                data[..n].iter().map(|v| cmp_str(v, s, op)).collect()
            }
            TypedColumn::NullableStr { values, nulls } => {
                values[..n].iter().enumerate().map(|(i, v)| {
                    if nulls.get(i) { false } else { cmp_str(v, s, op) }
                }).collect()
            }
            _ => vec![false; n],
        },
    }
}

/// Compare two TypedColumns for ALL physical rows (contiguous).
fn eval_full_scan_col_col(
    left: &TypedColumn,
    right: &TypedColumn,
    n: usize,
    op: CmpOp,
) -> Vec<bool> {
    match (left, right) {
        (TypedColumn::I64(l), TypedColumn::I64(r)) => {
            l[..n].iter().zip(r[..n].iter()).map(|(&a, &b)| cmp_i64(a, b, op)).collect()
        }
        (TypedColumn::Str(l), TypedColumn::Str(r)) => {
            l[..n].iter().zip(r[..n].iter()).map(|(a, b)| cmp_str(a, b, op)).collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            (0..n).map(|i| {
                if ln.get(i) || rn.get(i) { false } else { cmp_i64(lv[i], rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            (0..n).map(|i| {
                if ln.get(i) || rn.get(i) { false } else { cmp_str(&lv[i], &rv[i], op) }
            }).collect()
        }
        (TypedColumn::I64(l), TypedColumn::NullableI64 { values: rv, nulls: rn }) => {
            (0..n).map(|i| {
                if rn.get(i) { false } else { cmp_i64(l[i], rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableI64 { values: lv, nulls: ln }, TypedColumn::I64(r)) => {
            (0..n).map(|i| {
                if ln.get(i) { false } else { cmp_i64(lv[i], r[i], op) }
            }).collect()
        }
        (TypedColumn::Str(l), TypedColumn::NullableStr { values: rv, nulls: rn }) => {
            (0..n).map(|i| {
                if rn.get(i) { false } else { cmp_str(&l[i], &rv[i], op) }
            }).collect()
        }
        (TypedColumn::NullableStr { values: lv, nulls: ln }, TypedColumn::Str(r)) => {
            (0..n).map(|i| {
                if ln.get(i) { false } else { cmp_str(&lv[i], &r[i], op) }
            }).collect()
        }
        _ => vec![false; n],
    }
}

/// IS NULL / IS NOT NULL for ALL physical rows.
fn eval_full_scan_is_null(col: &TypedColumn, n: usize, want_null: bool) -> Vec<bool> {
    match col {
        TypedColumn::I64(_) | TypedColumn::Str(_) => vec![!want_null; n],
        TypedColumn::NullableI64 { nulls, .. } => {
            (0..n).map(|i| nulls.get(i) == want_null).collect()
        }
        TypedColumn::NullableStr { nulls, .. } => {
            (0..n).map(|i| nulls.get(i) == want_null).collect()
        }
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

// --- Helpers for eval_batch (on materialized Columns, post-join) ---

fn eval_column_value_cmp(
    cols: &Columns,
    col_idx: usize,
    value: &Value,
    op: CmpOp,
) -> Vec<bool> {
    let col = &cols[col_idx];
    let cell_val = value_to_cell(value);
    col.iter().map(|cell| cmp_cell(cell, &cell_val, op)).collect()
}

fn eval_column_column_cmp(
    cols: &Columns,
    left_idx: usize,
    right_idx: usize,
    op: CmpOp,
) -> Vec<bool> {
    let left = &cols[left_idx];
    let right = &cols[right_idx];
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| cmp_cell(l, r, op))
        .collect()
}

fn eval_is_null(cols: &Columns, col_idx: usize, want_null: bool) -> Vec<bool> {
    cols[col_idx]
        .iter()
        .map(|v| matches!(v, CellValue::Null) == want_null)
        .collect()
}

// --- Helpers for matches_join_row ---

fn get_combined(
    left: &Columns,
    right: &Columns,
    l: usize,
    r: usize,
    global_idx: usize,
) -> CellValue {
    if global_idx < left.len() {
        left[global_idx][l].clone()
    } else {
        right[global_idx - left.len()][r].clone()
    }
}

fn build_single_row(left: &Columns, right: &Columns, l: usize, r: usize) -> Columns {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    for col in left {
        combined.push(vec![col[l].clone()]);
    }
    for col in right {
        combined.push(vec![col[r].clone()]);
    }
    combined
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

    // --- eval_batch tests (on materialized Columns) ---

    #[test]
    fn test_eval_batch_equals_i64() {
        let cols: Columns = vec![vec![
            CellValue::I64(1), CellValue::I64(2), CellValue::I64(3),
            CellValue::I64(2), CellValue::I64(5),
        ]];
        let pred = PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(2) };
        assert_eq!(pred.eval_batch(&cols), vec![false, true, false, true, false]);
    }

    #[test]
    fn test_eval_batch_greater_than() {
        let cols: Columns = vec![vec![CellValue::I64(1), CellValue::I64(5), CellValue::I64(3)]];
        let pred = PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(2) };
        assert_eq!(pred.eval_batch(&cols), vec![false, true, true]);
    }

    #[test]
    fn test_eval_batch_is_null() {
        let cols: Columns = vec![vec![
            CellValue::I64(1), CellValue::Null, CellValue::I64(3), CellValue::Null,
        ]];
        let pred = PlanFilterPredicate::IsNull { column_idx: 0 };
        assert_eq!(pred.eval_batch(&cols), vec![false, true, false, true]);
        let pred = PlanFilterPredicate::IsNotNull { column_idx: 0 };
        assert_eq!(pred.eval_batch(&cols), vec![true, false, true, false]);
    }

    #[test]
    fn test_eval_batch_and() {
        let cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(5), CellValue::I64(3)],
            vec![CellValue::Str("a".into()), CellValue::Str("b".into()), CellValue::Str("a".into())],
        ];
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(2) }),
            Box::new(PlanFilterPredicate::Equals { column_idx: 1, value: Value::Text("a".into()) }),
        );
        assert_eq!(pred.eval_batch(&cols), vec![false, false, true]);
    }

    #[test]
    fn test_eval_batch_or() {
        let cols: Columns = vec![vec![CellValue::I64(1), CellValue::I64(5), CellValue::I64(3)]];
        let pred = PlanFilterPredicate::Or(
            Box::new(PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(1) }),
            Box::new(PlanFilterPredicate::Equals { column_idx: 0, value: Value::Int(5) }),
        );
        assert_eq!(pred.eval_batch(&cols), vec![true, true, false]);
    }

    #[test]
    fn test_eval_batch_column_equals() {
        let cols: Columns = vec![
            vec![CellValue::I64(1), CellValue::I64(2), CellValue::I64(3)],
            vec![CellValue::I64(1), CellValue::I64(99), CellValue::I64(3)],
        ];
        let pred = PlanFilterPredicate::ColumnEquals { left_idx: 0, right_idx: 1 };
        assert_eq!(pred.eval_batch(&cols), vec![true, false, true]);
    }

    #[test]
    fn test_eval_batch_null_comparison_returns_false() {
        let cols: Columns = vec![vec![CellValue::I64(10), CellValue::Null, CellValue::I64(30)]];
        let pred = PlanFilterPredicate::GreaterThan { column_idx: 0, value: Value::Int(18) };
        assert_eq!(pred.eval_batch(&cols), vec![false, false, true]);
        let pred = PlanFilterPredicate::Equals { column_idx: 0, value: Value::Null };
        assert_eq!(pred.eval_batch(&cols), vec![false, false, false]);
    }

    #[test]
    fn test_eval_batch_none_accepts_all() {
        let cols: Columns = vec![vec![CellValue::I64(1), CellValue::I64(2)]];
        assert_eq!(PlanFilterPredicate::None.eval_batch(&cols), vec![true, true]);
    }
}
