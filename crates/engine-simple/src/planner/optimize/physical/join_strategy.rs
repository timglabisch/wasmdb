//! Choose join strategy based on available indexes on the right table.
//!
//! If the join condition is `ColumnEquals` and the right table has an index
//! on the join column, an index-nested-loop join is chosen. Otherwise,
//! a full-scan nested loop join is used.

use schema_engine::schema::{IndexSchema, IndexType};

use crate::planner::plan::*;

pub fn choose(
    on: &PlanFilterPredicate,
    right_source: usize,
    right_indexes: &[IndexSchema],
) -> PlanJoinStrategy {
    if let PlanFilterPredicate::ColumnEquals { left, right } = on {
        let (left_col, right_col) = if right.source == right_source {
            (*left, right.col)
        } else if left.source == right_source {
            (*right, left.col)
        } else {
            return PlanJoinStrategy::NestedLoop;
        };

        for idx in right_indexes {
            if idx.columns.first() == Some(&right_col) {
                return PlanJoinStrategy::IndexLookup {
                    left_col,
                    right_col,
                    index_columns: idx.columns.clone(),
                    is_hash: idx.index_type == IndexType::Hash,
                };
            }
        }
    }
    PlanJoinStrategy::NestedLoop
}
