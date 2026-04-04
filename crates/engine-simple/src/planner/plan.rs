use query_engine::ast::{AggFunc, JoinType, Value};
use query_engine::schema::Schema;

#[derive(Debug, Clone)]
pub enum PlanFilterPredicate {
    /// column == value
    Equals { column_idx: usize, value: Value },
    /// column != value
    NotEquals { column_idx: usize, value: Value },
    /// column > value
    GreaterThan { column_idx: usize, value: Value },
    /// column >= value
    GreaterThanOrEqual { column_idx: usize, value: Value },
    /// column < value
    LessThan { column_idx: usize, value: Value },
    /// column <= value
    LessThanOrEqual { column_idx: usize, value: Value },

    /// left_column == right_column
    ColumnEquals { left_idx: usize, right_idx: usize },
    /// left_column != right_column
    ColumnNotEquals { left_idx: usize, right_idx: usize },
    /// left_column > right_column
    ColumnGreaterThan { left_idx: usize, right_idx: usize },
    /// left_column >= right_column
    ColumnGreaterThanOrEqual { left_idx: usize, right_idx: usize },
    /// left_column < right_column
    ColumnLessThan { left_idx: usize, right_idx: usize },
    /// left_column <= right_column
    ColumnLessThanOrEqual { left_idx: usize, right_idx: usize },

    IsNull { column_idx: usize },
    IsNotNull { column_idx: usize },

    And(Box<PlanFilterPredicate>, Box<PlanFilterPredicate>),
    Or(Box<PlanFilterPredicate>, Box<PlanFilterPredicate>),

    /// Accept all rows
    None,
}

#[derive(Debug, Clone)]
pub struct PlanSelect {
    pub sources: Vec<PlanSourceEntry>,
    pub filter: PlanFilterPredicate,
    pub group_by: Vec<usize>,
    pub aggregates: Vec<PlanAggregate>,
    pub result_columns: Vec<PlanResultColumn>,
    /// Combined schema of all sources.
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct PlanSourceEntry {
    pub table: String,
    pub schema: Schema,
    pub join: Option<PlanJoin>,
    pub pre_filter: PlanFilterPredicate,
}

#[derive(Debug, Clone)]
pub struct PlanJoin {
    pub join_type: JoinType,
    pub on: PlanFilterPredicate,
}

#[derive(Debug, Clone)]
pub struct PlanAggregate {
    pub func: AggFunc,
    pub column_idx: usize,
}

#[derive(Debug, Clone)]
pub enum PlanResultColumn {
    Column {
        column_idx: usize,
        alias: Option<String>,
    },
    Aggregate {
        func: AggFunc,
        column_idx: usize,
        alias: Option<String>,
    },
}
