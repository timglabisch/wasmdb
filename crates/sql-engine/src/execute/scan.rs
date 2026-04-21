use crate::planner::shared::plan::{PlanFilterPredicate, PlanIndexLookup, PlanScanMethod, PlanSource, PlanSourceEntry};
use crate::storage::{CellValue, RangeOp, Table};
use super::value_to_cell;
use super::{ExecutionContext, ScanMethod, SpanOperation};

use super::RowSet;

/// Scan a source according to the plan's scan_method.
///
/// Requirement sources are rejected at the pipeline level before reaching
/// here, so this function assumes the source is a `Table`.
pub fn scan<'a>(ctx: &mut ExecutionContext, table: &'a Table, source: &PlanSourceEntry) -> RowSet<'a> {
    let scan_method = match &source.source {
        PlanSource::Table { scan_method, .. } => scan_method,
        PlanSource::Requirement { .. } => {
            unreachable!("Requirement sources are filtered out in the pipeline before scan()");
        }
    };
    let table_name = table.schema.name.clone();
    let row_ids = ctx.span_with(|ctx| {
        let (ids, method) = match scan_method {
            PlanScanMethod::Full => {
                if matches!(source.pre_filter, PlanFilterPredicate::None) {
                    (scan_row_ids(ctx, table), ScanMethod::Full)
                } else {
                    (scan_filtered(ctx, table, &source.pre_filter), ScanMethod::Full)
                }
            }
            PlanScanMethod::Index { index_columns, prefix_len, is_hash, index_predicates, lookup } => {
                let mut ids = execute_index_lookup(table, index_columns, index_predicates, *lookup);
                ids.retain(|&r| !table.is_deleted(r));
                if !matches!(source.pre_filter, PlanFilterPredicate::None) {
                    ids = source.pre_filter.filter_batch(ctx, table, &ids);
                }
                let method = ScanMethod::Index {
                    columns: index_columns.clone(),
                    prefix_len: *prefix_len,
                    is_hash: *is_hash,
                };
                (ids, method)
            }
        };
        let op = SpanOperation::Scan { table: table_name, method, rows: ids.len() };
        (op, ids)
    });
    RowSet::from_scan(table, row_ids)
}

pub fn scan_row_ids(_ctx: &mut ExecutionContext, table: &Table) -> Vec<usize> {
    table.row_ids().collect()
}

pub fn scan_filtered(ctx: &mut ExecutionContext, table: &Table, pred: &PlanFilterPredicate) -> Vec<usize> {
    let row_ids = scan_row_ids(ctx, table);
    pred.filter_batch(ctx, table, &row_ids)
}

// ── Index execution (the plan already decided WHICH index) ───────────────

fn execute_index_lookup(
    table: &Table,
    planned_columns: &[usize],
    index_predicates: &[PlanFilterPredicate],
    lookup: PlanIndexLookup,
) -> Vec<usize> {
    let idx = table.indexes().iter()
        .find(|idx| idx.columns() == planned_columns)
        .expect("planned index must exist at runtime");

    // Extract lookup values from the index_predicates.
    // Predicates are in index-column order: Eq..., then optionally one Range or In at the end.
    let mut prefix_eq_values: Vec<CellValue> = Vec::new();
    let mut range_on_last: Option<(RangeOp, CellValue)> = None;
    let mut in_on_last: Option<Vec<CellValue>> = None;

    for pred in index_predicates {
        match pred {
            PlanFilterPredicate::Equals { value, .. } => {
                prefix_eq_values.push(value_to_cell(value));
            }
            PlanFilterPredicate::GreaterThan { value, .. } => {
                range_on_last = Some((RangeOp::Gt, value_to_cell(value)));
            }
            PlanFilterPredicate::GreaterThanOrEqual { value, .. } => {
                range_on_last = Some((RangeOp::Gte, value_to_cell(value)));
            }
            PlanFilterPredicate::LessThan { value, .. } => {
                range_on_last = Some((RangeOp::Lt, value_to_cell(value)));
            }
            PlanFilterPredicate::LessThanOrEqual { value, .. } => {
                range_on_last = Some((RangeOp::Lte, value_to_cell(value)));
            }
            PlanFilterPredicate::In { values, .. } => {
                in_on_last = Some(values.iter().map(|v| value_to_cell(v)).collect());
            }
            _ => {}
        }
    }

    match lookup {
        PlanIndexLookup::InMultiLookup => {
            let in_values = in_on_last.expect("InMultiLookup requires IN predicate");
            let mut combined: Vec<usize> = Vec::new();
            for v in &in_values {
                let mut key = prefix_eq_values.clone();
                key.push(v.clone());
                if let Some(hits) = idx.lookup_eq(&key).map(|s| s.to_vec()) {
                    combined.extend(hits);
                }
            }
            combined.sort_unstable();
            combined.dedup();
            combined
        }
        PlanIndexLookup::PrefixRange => {
            let (op, value) = range_on_last.expect("PrefixRange requires range predicate");
            idx.lookup_prefix_range(&prefix_eq_values, op, &value)
                .unwrap_or_default()
        }
        PlanIndexLookup::FullKeyEq => {
            idx.lookup_eq(&prefix_eq_values).map(|s| s.to_vec()).unwrap_or_default()
        }
        PlanIndexLookup::PrefixEq => {
            idx.lookup_prefix_eq(&prefix_eq_values).unwrap_or_default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::planner::shared::plan::{ColumnRef, PlanScanMethod};
    use crate::storage::CellValue;
    use sql_parser::ast::Value;
    use sql_parser::schema::Schema;
    use crate::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    fn make_source(table: &str, pre_filter: PlanFilterPredicate, scan_method: PlanScanMethod) -> PlanSourceEntry {
        // Minimal PlanSourceEntry for scan tests — schema is not used by scan().
        PlanSourceEntry {
            source: PlanSource::Table {
                name: table.into(),
                schema: Schema::new(vec![]),
                scan_method,
            },
            join: None,
            pre_filter,
        }
    }

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
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let mut table = make_users_table();
        table.delete(1).unwrap();
        assert_eq!(scan_row_ids(&mut ctx, &table), vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_equals() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) }), vec![1]);
    }

    #[test]
    fn test_scan_filtered_greater_than() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) }), vec![0, 2]);
    }

    #[test]
    fn test_scan_filtered_skips_deleted() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let mut table = make_users_table();
        table.delete(0).unwrap();
        assert_eq!(scan_filtered(&mut ctx, &table, &PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) }), vec![2]);
    }

    #[test]
    fn test_scan_filtered_and() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let pred = PlanFilterPredicate::And(
            Box::new(PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(24) }),
            Box::new(PlanFilterPredicate::LessThan { col: c(0, 2), value: Value::Int(32) }),
        );
        assert_eq!(scan_filtered(&mut ctx, &table, &pred), vec![0, 1]);
    }

    #[test]
    fn test_scan_returns_rowset() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_users_table();
        let pre_filter = PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(28) };
        let source = make_source("users", pre_filter, PlanScanMethod::Full);
        let rs = scan(&mut ctx, &table, &source);
        assert_eq!(rs.num_rows, 2);
        assert_eq!(rs.get(0, c(0, 1)), CellValue::Str("Alice".into()));
        assert_eq!(rs.get(1, c(0, 1)), CellValue::Str("Carol".into()));
    }

    fn make_composite_indexed_table() -> Table {
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
                index_type: IndexType::BTree,
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
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_composite_indexed_table();
        // pre_filter = None (index covers all predicates)
        let source = make_source("events", PlanFilterPredicate::None, PlanScanMethod::Index {
            index_columns: vec![0, 1],
            prefix_len: 2,
            is_hash: false,
            index_predicates: vec![
                PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) },
                PlanFilterPredicate::Equals { col: c(0, 1), value: Value::Int(20) },
            ],
            lookup: PlanIndexLookup::FullKeyEq,
        });
        let rs = scan(&mut ctx, &table, &source);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }

    #[test]
    fn test_scan_composite_index_prefix_eq() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_composite_indexed_table();
        // pre_filter = None (index covers the single predicate)
        let source = make_source("events", PlanFilterPredicate::None, PlanScanMethod::Index {
            index_columns: vec![0, 1],
            prefix_len: 1,
            is_hash: false,
            index_predicates: vec![
                PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(1) },
            ],
            lookup: PlanIndexLookup::PrefixEq,
        });
        let rs = scan(&mut ctx, &table, &source);
        assert_eq!(rs.num_rows, 2);
    }

    #[test]
    fn test_scan_composite_index_prefix_range() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_composite_indexed_table();
        // pre_filter = None (index covers both predicates)
        let source = make_source("events", PlanFilterPredicate::None, PlanScanMethod::Index {
            index_columns: vec![0, 1],
            prefix_len: 2,
            is_hash: false,
            index_predicates: vec![
                PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) },
                PlanFilterPredicate::GreaterThan { col: c(0, 1), value: Value::Int(10) },
            ],
            lookup: PlanIndexLookup::PrefixRange,
        });
        let rs = scan(&mut ctx, &table, &source);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }

    #[test]
    fn test_scan_composite_index_with_remaining_filter() {
        let db = HashMap::new();
        let mut ctx = ExecutionContext::new(&db);
        let table = make_composite_indexed_table();
        // pre_filter = post_filter (score > 350 not covered by index)
        let source = make_source("events",
            PlanFilterPredicate::GreaterThan { col: c(0, 2), value: Value::Int(350) },
            PlanScanMethod::Index {
                index_columns: vec![0, 1],
                prefix_len: 2,
                is_hash: false,
                index_predicates: vec![
                    PlanFilterPredicate::Equals { col: c(0, 0), value: Value::Int(2) },
                    PlanFilterPredicate::GreaterThanOrEqual { col: c(0, 1), value: Value::Int(10) },
                ],
                lookup: PlanIndexLookup::PrefixRange,
            },
        );
        let rs = scan(&mut ctx, &table, &source);
        assert_eq!(rs.num_rows, 1);
        assert_eq!(rs.get(0, c(0, 2)), CellValue::I64(400));
    }
}
