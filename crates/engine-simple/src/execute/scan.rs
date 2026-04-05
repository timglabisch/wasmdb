use crate::planner::plan::PlanFilterPredicate;
use crate::storage::{CellValue, RangeOp, Table};
use super::value_to_cell;
use super::{ExecutionContext, ScanMethod, SpanOperation};

use super::RowSet;

/// Scan + pre-filter -> RowSet (no materialization).
/// Tries index lookup first; falls back to full scan + filter.
pub fn scan<'a>(ctx: &mut ExecutionContext, table: &'a Table, pre_filter: &PlanFilterPredicate) -> RowSet<'a> {
    let table_name = table.schema.name.clone();
    let (row_ids, method) = ctx.span_with(|ctx| {
        let (ids, method) = if matches!(pre_filter, PlanFilterPredicate::None) {
            (scan_row_ids(ctx, table), ScanMethod::Full)
        } else {
            match try_index_scan(ctx, table, pre_filter) {
                Some((ids, m)) => (ids, m),
                None => (scan_filtered(ctx, table, pre_filter), ScanMethod::Full),
            }
        };
        let op = SpanOperation::Scan { table: table_name, method: method.clone(), rows: ids.len() };
        (op, (ids, method))
    });
    let _ = method; // used in span above
    RowSet::from_scan(table, row_ids)
}

pub fn scan_row_ids(_ctx: &mut ExecutionContext, table: &Table) -> Vec<usize> {
    table.row_ids().collect()
}

pub fn scan_filtered(ctx: &mut ExecutionContext, table: &Table, pred: &PlanFilterPredicate) -> Vec<usize> {
    let row_ids = scan_row_ids(ctx, table);
    pred.filter_batch(ctx, table, &row_ids)
}

// ── Index scan helpers ────────────────────────────────────────────────────

fn flatten_ands(pred: &PlanFilterPredicate) -> Vec<&PlanFilterPredicate> {
    match pred {
        PlanFilterPredicate::And(l, r) => {
            let mut leaves = flatten_ands(l);
            leaves.extend(flatten_ands(r));
            leaves
        }
        other => vec![other],
    }
}

enum PredClass<'a> {
    Eq(&'a query_engine::ast::Value),
    Range(RangeOp, &'a query_engine::ast::Value),
    In(&'a [query_engine::ast::Value]),
    Other,
}

fn classify_pred(pred: &PlanFilterPredicate) -> PredClass<'_> {
    match pred {
        PlanFilterPredicate::Equals { value, .. } => PredClass::Eq(value),
        PlanFilterPredicate::GreaterThan { value, .. } => PredClass::Range(RangeOp::Gt, value),
        PlanFilterPredicate::GreaterThanOrEqual { value, .. } => PredClass::Range(RangeOp::Gte, value),
        PlanFilterPredicate::LessThan { value, .. } => PredClass::Range(RangeOp::Lt, value),
        PlanFilterPredicate::LessThanOrEqual { value, .. } => PredClass::Range(RangeOp::Lte, value),
        PlanFilterPredicate::In { values, .. } => PredClass::In(values),
        PlanFilterPredicate::InMaterialized { .. }
        | PlanFilterPredicate::CompareMaterialized { .. } => PredClass::Other,
        _ => PredClass::Other,
    }
}

fn leaf_column(pred: &PlanFilterPredicate) -> Option<usize> {
    match pred {
        PlanFilterPredicate::Equals { col, .. }
        | PlanFilterPredicate::GreaterThan { col, .. }
        | PlanFilterPredicate::GreaterThanOrEqual { col, .. }
        | PlanFilterPredicate::LessThan { col, .. }
        | PlanFilterPredicate::LessThanOrEqual { col, .. }
        | PlanFilterPredicate::In { col, .. }
        | PlanFilterPredicate::InMaterialized { col, .. }
        | PlanFilterPredicate::CompareMaterialized { col, .. } => Some(col.col),
        _ => None,
    }
}

fn try_index_scan(ctx: &mut ExecutionContext, table: &Table, pred: &PlanFilterPredicate) -> Option<(Vec<usize>, ScanMethod)> {
    let leaves = flatten_ands(pred);

    let mut seen_cols = Vec::new();
    let mut indexable: Vec<(usize, usize, &PlanFilterPredicate)> = Vec::new();
    for (li, leaf) in leaves.iter().enumerate() {
        if let Some(col) = leaf_column(leaf) {
            if !seen_cols.contains(&col) {
                seen_cols.push(col);
                indexable.push((li, col, leaf));
            }
        }
    }

    if indexable.is_empty() {
        return None;
    }

    let mut best_ids: Option<Vec<usize>> = None;
    let mut best_score: (usize, u8) = (0, 0);
    let mut best_used: Vec<usize> = Vec::new();
    let mut best_index_columns: Vec<usize> = Vec::new();
    let mut best_prefix_len: usize = 0;
    let mut best_is_hash: bool = false;

    for idx in table.indexes() {
        let idx_cols = idx.columns();
        let mut prefix_eq_values: Vec<CellValue> = Vec::new();
        let mut range_on_last: Option<(RangeOp, CellValue)> = None;
        let mut used_leaves: Vec<usize> = Vec::new();
        let mut in_on_last: Option<Vec<CellValue>> = None;

        for &col in idx_cols {
            if let Some(&(li, _, pred)) = indexable.iter().find(|(_, c, _)| *c == col) {
                match classify_pred(pred) {
                    PredClass::Eq(value) => {
                        prefix_eq_values.push(value_to_cell(value));
                        used_leaves.push(li);
                    }
                    PredClass::Range(op, value) => {
                        range_on_last = Some((op, value_to_cell(value)));
                        used_leaves.push(li);
                        break;
                    }
                    PredClass::In(values) => {
                        in_on_last = Some(values.iter().map(|v| value_to_cell(v)).collect());
                        used_leaves.push(li);
                        break;
                    }
                    PredClass::Other => break,
                }
            } else {
                break;
            }
        }

        let prefix_len = used_leaves.len();
        if prefix_len == 0 { continue; }

        let is_full_key_eq = range_on_last.is_none() && in_on_last.is_none() && prefix_eq_values.len() == idx_cols.len();
        let tie_break = if is_full_key_eq && idx.is_hash() { 2 } else if is_full_key_eq { 1 } else { 0 };
        let score = (prefix_len, tie_break);
        if score <= best_score { continue; }

        let ids = if let Some(ref in_values) = in_on_last {
            let mut combined: Vec<usize> = Vec::new();
            for v in in_values {
                let mut key = prefix_eq_values.clone();
                key.push(v.clone());
                if let Some(hits) = idx.lookup_eq(&key).map(|s| s.to_vec()) {
                    combined.extend(hits);
                }
            }
            combined.sort_unstable();
            combined.dedup();
            Some(combined)
        } else if let Some((op, ref value)) = range_on_last {
            idx.lookup_prefix_range(&prefix_eq_values, op, value)
        } else if is_full_key_eq {
            idx.lookup_eq(&prefix_eq_values).map(|s| s.to_vec())
        } else {
            idx.lookup_prefix_eq(&prefix_eq_values)
        };

        if let Some(ids) = ids {
            best_ids = Some(ids);
            best_score = score;
            best_used = used_leaves;
            best_index_columns = idx_cols.to_vec();
            best_prefix_len = prefix_len;
            best_is_hash = idx.is_hash();
        }
    }

    let mut ids = best_ids?;
    ids.retain(|&r| !table.is_deleted(r));

    for (li, leaf) in leaves.iter().enumerate() {
        if !best_used.contains(&li) {
            ids = leaf.filter_batch(ctx, table, &ids);
        }
    }

    let method = ScanMethod::Index { columns: best_index_columns, prefix_len: best_prefix_len, is_hash: best_is_hash };
    Some((ids, method))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::ColumnRef;
    use crate::storage::CellValue;
    use query_engine::ast::Value;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    fn make_users_table() -> Table {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0], indexes: vec![],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
        t
    }

    #[test]
    fn test_scan_row_ids_skips_deleted() {
        let mut ctx = ExecutionContext::new();
        let mut table = make_users_table();
        table.delete(1).unwrap();
        assert_eq!(scan_row_ids(&mut ctx, &table), vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_equals() {
        let mut ctx = ExecutionContext::new();
        let table = make_users_table();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) }), vec![1]);
    }

    #[test]
    fn test_scan_filtered_greater_than() {
        let mut ctx = ExecutionContext::new();
        let table = make_users_table();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) }), vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_skips_deleted() {
        let mut ctx = ExecutionContext::new();
        let mut table = make_users_table();
        table.delete(0).unwrap();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) }), vec![2]);
    }

    #[test]
    fn test_scan_filtered_and() {
        let mut ctx = ExecutionContext::new();
        let table = make_users_table();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(24) }),
            Box::new(PlanFilterPredicate::LessThan { col: c(0, 2), value: Value::Int(32) }),
        );
        assert_eq!(scan_filtered(&mut ctx, &table, &pred), vec![0, 1]);
    }

    #[test]
    fn test_scan_returns_rowset() {
        let mut ctx = ExecutionContext::new();
        let table = make_users_table();
        let rs = scan(&mut ctx, &table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) });
        assert_eq!(rs.num_rows, 2);
        assert_eq!(rs.get(0, c(0, 1)), CellValue::Str("Alice".into()));
        assert_eq!(rs.get(1, c(0, 1)), CellValue::Str("Carol".into()));
    }

    #[test]
    fn test_flatten_ands() {
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::And(
                Box::new(PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(1) }),
                Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 1), value: Value::Int(5) }),
            )),
            Box::new(PlanFilterPredicate::LessThan { col: c(0, 2), value: Value::Int(10) }),
        );
        assert_eq!(flatten_ands(&pred).len(), 3);
    }

    fn make_composite_indexed_table() -> Table {
        use schema_engine::schema::IndexSchema;
        let schema = TableSchema {
            name: "events".into(),
            columns: vec![
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "category".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "score".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![],
            indexes: vec![IndexSchema {
                name: Some("idx_user_cat".into()), columns: vec![0, 1],
                index_type: schema_engine::schema::IndexType::BTree,
            }],
        };
        let mut t = Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::I64(10), CellValue::I64(100)]).unwrap();
        t.insert(&[CellValue::I64(1), CellValue::I64(20), CellValue::I64(200)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::I64(10), CellValue::I64(300)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::I64(20), CellValue::I64(400)]).unwrap();
        t
    }

    #[test]
    fn test_scan_composite_index_full_eq() {
        let mut ctx = ExecutionContext::new();
        let table = make_composite_indexed_table();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) }),
            Box::new(PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Int(20) }),
        );
        let rs = scan(&mut ctx, &table, &pred);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }

    #[test]
    fn test_scan_composite_index_prefix_eq() {
        let mut ctx = ExecutionContext::new();
        let table = make_composite_indexed_table();
        let rs = scan(&mut ctx, &table, &PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(1) });
        assert_eq!(rs.num_rows, 2);
    }

    #[test]
    fn test_scan_composite_index_prefix_range() {
        let mut ctx = ExecutionContext::new();
        let table = make_composite_indexed_table();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) }),
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 1), value: Value::Int(10) }),
        );
        let rs = scan(&mut ctx, &table, &pred);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }

    #[test]
    fn test_scan_composite_index_with_remaining_filter() {
        let mut ctx = ExecutionContext::new();
        let table = make_composite_indexed_table();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::And(
                Box::new(PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) }),
                Box::new(PlanFilterPredicate::GreaterThanOrEqual { col: c(0, 1), value: Value::Int(10) }),
            )),
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(350) }),
        );
        let rs = scan(&mut ctx, &table, &pred);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }
}
