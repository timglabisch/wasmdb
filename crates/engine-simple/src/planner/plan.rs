use query_engine::ast::{AggFunc, JoinType, Operator, OrderDirection, Value};
use query_engine::schema::Schema;

/// Reference to a column: (source table position, column position within that table).
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

    In { col: ColumnRef, values: Vec<Value> },

    /// IN from materialized subquery. Resolved to In{} before execution.
    InMaterialized { col: ColumnRef, mat_id: usize },

    /// Column comparison against materialized scalar. Resolved before execution.
    CompareMaterialized { col: ColumnRef, op: Operator, mat_id: usize },

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

/// How to scan a single table — decided by the planner.
#[derive(Debug, Clone)]
pub enum PlanScanMethod {
    /// Full table scan, apply pre_filter as post-filter.
    Full,
    /// Use an index. The executor executes the lookup, then applies
    /// `source.pre_filter` as post-filter (which access_path has narrowed
    /// to only the residual predicates not covered by the index).
    Index {
        /// Index column positions (matches a TableIndex.columns()).
        index_columns: Vec<usize>,
        /// How many leading columns the index uses.
        prefix_len: usize,
        /// Hash or BTree.
        is_hash: bool,
        /// Which leaf predicates the index handles (in index-column order).
        /// Executor uses these to build the lookup key.
        index_predicates: Vec<PlanFilterPredicate>,
    },
}

/// How to execute a join — decided by the planner.
#[derive(Debug, Clone)]
pub enum PlanJoinStrategy {
    /// Full scan of right table, then nested-loop with predicate evaluation.
    NestedLoop,
    /// Per left row: index lookup on right table.
    IndexLookup {
        /// Column in the LEFT table that provides the lookup value.
        left_col: ColumnRef,
        /// Column in the RIGHT table that has the index.
        right_col: usize,
        /// Index metadata.
        index_columns: Vec<usize>,
        is_hash: bool,
    },
}

#[derive(Debug, Clone)]
pub struct PlanSourceEntry {
    pub table: String,
    pub schema: Schema,
    pub join: Option<PlanJoin>,
    pub pre_filter: PlanFilterPredicate,
    pub scan_method: PlanScanMethod,
}

#[derive(Debug, Clone)]
pub struct PlanJoin {
    pub join_type: JoinType,
    pub on: PlanFilterPredicate,
    pub strategy: PlanJoinStrategy,
}

#[derive(Debug, Clone)]
pub struct PlanAggregate {
    pub func: AggFunc,
    pub col: ColumnRef,
}

/// Top-level execution plan: materialization steps + main query.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Materialization steps in bottom-up order (inner-most subquery first).
    pub materializations: Vec<MaterializeStep>,
    /// Main query — may contain InMaterialized/CompareMaterialized predicates.
    pub main: PlanSelect,
}

#[derive(Debug, Clone)]
pub struct MaterializeStep {
    pub plan: PlanSelect,
    pub kind: MaterializeKind,
}

#[derive(Debug, Clone, Copy)]
pub enum MaterializeKind {
    /// 1 column, 1 row — scalar value for comparison.
    Scalar,
    /// 1 column, N rows — value list for IN.
    List,
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
