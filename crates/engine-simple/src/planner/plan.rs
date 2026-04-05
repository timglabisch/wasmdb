use query_engine::ast::{AggFunc, JoinType, OrderDirection, Value};
use query_engine::schema::Schema;

/// Reference to a column: (source table index, column index within that table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub source: usize,
    pub col: usize,
}

#[derive(Debug, Clone)]
pub enum PlanFilterPredicate {
    Equals { col: ColumnRef, value: Value },
    NotEquals { col: ColumnRef, value: Value },
    GreaterThan { col: ColumnRef, value: Value },
    GreaterThanOrEqual { col: ColumnRef, value: Value },
    LessThan { col: ColumnRef, value: Value },
    LessThanOrEqual { col: ColumnRef, value: Value },

    ColumnEquals { left: ColumnRef, right: ColumnRef },
    ColumnNotEquals { left: ColumnRef, right: ColumnRef },
    ColumnGreaterThan { left: ColumnRef, right: ColumnRef },
    ColumnGreaterThanOrEqual { left: ColumnRef, right: ColumnRef },
    ColumnLessThan { left: ColumnRef, right: ColumnRef },
    ColumnLessThanOrEqual { left: ColumnRef, right: ColumnRef },

    IsNull { col: ColumnRef },
    IsNotNull { col: ColumnRef },

    And(Box<PlanFilterPredicate>, Box<PlanFilterPredicate>),
    Or(Box<PlanFilterPredicate>, Box<PlanFilterPredicate>),

    /// Accept all rows
    None,
}

#[derive(Debug, Clone)]
pub struct PlanSelect {
    pub sources: Vec<PlanSourceEntry>,
    pub filter: PlanFilterPredicate,
    pub group_by: Vec<ColumnRef>,
    pub aggregates: Vec<PlanAggregate>,
    pub order_by: Vec<PlanOrderSpec>,
    pub limit: Option<usize>,
    pub result_columns: Vec<PlanResultColumn>,
}

#[derive(Debug, Clone)]
pub struct PlanOrderSpec {
    pub col: ColumnRef,
    pub direction: OrderDirection,
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
    pub col: ColumnRef,
}

#[derive(Debug, Clone)]
pub enum PlanResultColumn {
    Column {
        col: ColumnRef,
        alias: Option<String>,
    },
    Aggregate {
        func: AggFunc,
        col: ColumnRef,
        alias: Option<String>,
    },
}
