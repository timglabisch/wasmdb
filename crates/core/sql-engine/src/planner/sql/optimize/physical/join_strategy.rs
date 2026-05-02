//! Choose join strategy based on available indexes on the right table.
//!
//! If the join condition is `ColumnEquals` and the right table has an index
//! on the join column, an index-nested-loop join is chosen. Otherwise,
//! a full-scan nested loop join is used.

use crate::schema::{self, IndexType, TableSchema};

use crate::planner::shared::plan::*;

pub fn choose(
    on: &PlanFilterPredicate,
    right_source: usize,
    ts: &TableSchema,
) -> PlanJoinStrategy {
    let PlanFilterPredicate::ColumnEquals { left, right } = on else {
        return PlanJoinStrategy::NestedLoop;
    };

    let (left_col, right_col) = if right.source == right_source {
        (*left, right.col)
    } else if left.source == right_source {
        (*right, left.col)
    } else {
        return PlanJoinStrategy::NestedLoop;
    };

    let indexes = schema::effective_indexes(ts);
    for idx in &indexes {
        if idx.columns.first() == Some(&right_col) {
            return PlanJoinStrategy::IndexLookup {
                left_col,
                right_col,
                index_columns: idx.columns.clone(),
                is_hash: idx.index_type == IndexType::Hash,
            };
        }
    }

    PlanJoinStrategy::NestedLoop
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, DataType, IndexSchema};

    fn c(source: usize, col: usize) -> ColumnRef { ColumnRef { source, col } }

    fn ts_with_index_on(col: usize, hash: bool) -> TableSchema {
        TableSchema {
            name: "right".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "fk".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![
                IndexSchema {
                    name: None,
                    columns: vec![col],
                    index_type: if hash { IndexType::Hash } else { IndexType::BTree },
                },
            ],
        }
    }

    fn ts_no_index_on_fk() -> TableSchema {
        TableSchema {
            name: "right".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "fk".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    #[test]
    fn index_lookup_when_right_col_has_index() {
        let on = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) };
        let strategy = choose(&on, 1, &ts_with_index_on(1, true));
        assert!(matches!(strategy, PlanJoinStrategy::IndexLookup { right_col: 1, is_hash: true, .. }));
    }

    #[test]
    fn nested_loop_when_no_index_on_join_col() {
        let on = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) };
        let strategy = choose(&on, 1, &ts_no_index_on_fk());
        assert!(matches!(strategy, PlanJoinStrategy::NestedLoop));
    }

    #[test]
    fn nested_loop_for_non_equality_join() {
        let on = PlanFilterPredicate::ColumnGreaterThan { left: c(0, 0), right: c(1, 1) };
        let strategy = choose(&on, 1, &ts_with_index_on(1, true));
        assert!(matches!(strategy, PlanJoinStrategy::NestedLoop));
    }

    #[test]
    fn index_lookup_on_pk() {
        // Join on PK (col 0) — auto PK Hash index should be found
        let on = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 0) };
        let strategy = choose(&on, 1, &ts_no_index_on_fk());
        // PK auto Hash index on col 0
        assert!(matches!(strategy, PlanJoinStrategy::IndexLookup { right_col: 0, is_hash: true, .. }));
    }

    #[test]
    fn left_col_resolved_correctly_when_flipped() {
        // right.source == 1 matches right_source, so left_col should be c(0,0)
        let on = PlanFilterPredicate::ColumnEquals { left: c(0, 0), right: c(1, 1) };
        let strategy = choose(&on, 1, &ts_with_index_on(1, false));
        match strategy {
            PlanJoinStrategy::IndexLookup { left_col, right_col, is_hash, .. } => {
                assert_eq!(left_col, c(0, 0));
                assert_eq!(right_col, 1);
                assert!(!is_hash);
            }
            _ => panic!("expected IndexLookup"),
        }
    }
}
