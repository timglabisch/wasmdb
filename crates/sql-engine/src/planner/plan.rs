use sql_parser::ast::{AggFunc, JoinType, Operator, OrderDirection, Value};
use sql_parser::schema::Schema;

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

impl PlanFilterPredicate {
    /// Combine an iterator of predicates with AND. Returns `None` for empty input.
    pub fn combine_and(preds: impl IntoIterator<Item = PlanFilterPredicate>) -> PlanFilterPredicate {
        preds.into_iter()
            .reduce(|a, b| PlanFilterPredicate::And(Box::new(a), Box::new(b)))
            .unwrap_or(PlanFilterPredicate::None)
    }

    /// Extract all column references from this predicate.
    pub fn column_refs(&self) -> Vec<ColumnRef> {
        match self {
            PlanFilterPredicate::Equals { col, .. }
            | PlanFilterPredicate::NotEquals { col, .. }
            | PlanFilterPredicate::GreaterThan { col, .. }
            | PlanFilterPredicate::GreaterThanOrEqual { col, .. }
            | PlanFilterPredicate::LessThan { col, .. }
            | PlanFilterPredicate::LessThanOrEqual { col, .. }
            | PlanFilterPredicate::IsNull { col }
            | PlanFilterPredicate::IsNotNull { col } => vec![*col],

            PlanFilterPredicate::ColumnEquals { left, right }
            | PlanFilterPredicate::ColumnNotEquals { left, right }
            | PlanFilterPredicate::ColumnGreaterThan { left, right }
            | PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right }
            | PlanFilterPredicate::ColumnLessThan { left, right }
            | PlanFilterPredicate::ColumnLessThanOrEqual { left, right } => {
                vec![*left, *right]
            }

            PlanFilterPredicate::In { col, .. }
            | PlanFilterPredicate::InMaterialized { col, .. }
            | PlanFilterPredicate::CompareMaterialized { col, .. } => vec![*col],

            PlanFilterPredicate::And(l, r) | PlanFilterPredicate::Or(l, r) => {
                let mut v = l.column_refs();
                v.extend(r.column_refs());
                v
            }
            PlanFilterPredicate::None => vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanSelect {
    pub sources: Vec<PlanSourceEntry>,
    pub filter: PlanFilterPredicate,
    pub group_by: Vec<ColumnRef>,
    pub aggregates: Vec<PlanAggregate>,
    pub order_by: Vec<PlanOrderSpec>,
    pub limit: Option<PlanLimit>,
    pub result_columns: Vec<PlanResultColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanLimit {
    Value(usize),
    Placeholder(String),
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
    /// `source.pre_filter` as post-filter (which the planner has narrowed
    /// to only the predicates not covered by the index).
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
        /// Which lookup method the executor should use.
        lookup: PlanIndexLookup,
    },
}

/// How to query the index — decided by the planner based on predicate shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanIndexLookup {
    /// All index columns matched with Eq → `lookup_eq` (exact match).
    FullKeyEq,
    /// Only leading columns matched with Eq → `lookup_prefix_eq` (prefix scan).
    PrefixEq,
    /// Leading Eq columns + one Range predicate → `lookup_prefix_range`.
    PrefixRange,
    /// Leading Eq columns + one IN predicate → multiple `lookup_eq` calls.
    InMultiLookup,
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

pub use super::reactive::*;

/// Top-level execution plan: materialization steps + main query.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Materialization steps in bottom-up order (inner-most subquery first).
    pub materializations: Vec<MaterializeStep>,
    /// Main query — may contain InMaterialized/CompareMaterialized predicates.
    pub main: PlanSelect,
    /// Present when the query contains INVALIDATE_ON expressions.
    pub reactive: Option<ReactiveMetadata>,
}

// ── Pretty printer ───────────────────────────────────────────────────────

impl ExecutionPlan {
    pub fn pretty_print(&self) -> String {
        let mut out = String::new();
        for (i, mat) in self.materializations.iter().enumerate() {
            let kind = match mat.kind {
                MaterializeKind::Scalar => "Scalar",
                MaterializeKind::List => "List",
            };
            out.push_str(&format!("Materialize step={i} kind={kind}\n"));
            mat.plan.pretty_print_to(&mut out, 1);
        }
        out.push_str("Select\n");
        self.main.pretty_print_to(&mut out, 1);
        if let Some(ref reactive) = self.reactive {
            let strat = match reactive.strategy {
                InvalidationStrategy::ReExecute => "ReExecute",
                InvalidationStrategy::Invalidate => "Invalidate",
            };
            out.push_str(&format!("Reactive strategy={strat}\n"));
            for (i, cond) in reactive.conditions.iter().enumerate() {
                out.push_str(&format!("  invalidation[{i}] table={}\n", cond.table));
                for key in &cond.index_keys {
                    out.push_str(&format!("    key: {}.col{} = {}\n", cond.table, key.col, val(&key.value)));
                }
                if !matches!(cond.verify_filter, PlanFilterPredicate::None) {
                    out.push_str("    verify: ");
                    // Use source list from main plan for column name resolution
                    cond.verify_filter.pretty_print_to(&mut out, &self.main.sources);
                    out.push('\n');
                }
            }
        }
        out
    }
}

impl PlanSelect {
    fn pretty_print_to(&self, out: &mut String, depth: usize) {
        let indent = "  ".repeat(depth);

        // Sources
        for (i, source) in self.sources.iter().enumerate() {
            if i == 0 {
                out.push_str(&format!("{indent}Scan table={}", source.table));
            } else if let Some(join) = &source.join {
                let jt = match join.join_type {
                    JoinType::Inner => "Inner",
                    JoinType::Left => "Left",
                };
                let strategy = match &join.strategy {
                    PlanJoinStrategy::NestedLoop => "NestedLoop".to_string(),
                    PlanJoinStrategy::IndexLookup { index_columns, is_hash, .. } => {
                        let kind = if *is_hash { "Hash" } else { "BTree" };
                        format!("IndexLookup({kind}{index_columns:?})")
                    }
                };
                out.push_str(&format!("{indent}Join type={jt} strategy={strategy} table={}", source.table));
            } else {
                out.push_str(&format!("{indent}CrossJoin table={}", source.table));
            }

            // Scan method
            match &source.scan_method {
                PlanScanMethod::Full => out.push_str(" scan=Full"),
                PlanScanMethod::Index { index_columns, prefix_len, is_hash, lookup, .. } => {
                    let kind = if *is_hash { "Hash" } else { "BTree" };
                    let lk = match lookup {
                        PlanIndexLookup::FullKeyEq => "FullKeyEq",
                        PlanIndexLookup::PrefixEq => "PrefixEq",
                        PlanIndexLookup::PrefixRange => "PrefixRange",
                        PlanIndexLookup::InMultiLookup => "InMultiLookup",
                    };
                    out.push_str(&format!(" scan={kind}({index_columns:?} prefix={prefix_len} lookup={lk})"));
                }
            }
            out.push('\n');

            // Pre-filter
            if !matches!(source.pre_filter, PlanFilterPredicate::None) {
                out.push_str(&format!("{}  pre_filter: ", indent));
                source.pre_filter.pretty_print_to(out, &self.sources);
                out.push('\n');
            }

            // Join ON predicate
            if let Some(join) = &source.join {
                if !matches!(join.on, PlanFilterPredicate::None) {
                    out.push_str(&format!("{}  on: ", indent));
                    join.on.pretty_print_to(out, &self.sources);
                    out.push('\n');
                }
            }

            // Index predicates
            if let PlanScanMethod::Index { index_predicates, .. } = &source.scan_method {
                if !index_predicates.is_empty() {
                    out.push_str(&format!("{}  index_preds: [", indent));
                    for (j, pred) in index_predicates.iter().enumerate() {
                        if j > 0 { out.push_str(", "); }
                        pred.pretty_print_to(out, &self.sources);
                    }
                    out.push_str("]\n");
                }
            }
        }

        // Post-filter
        if !matches!(self.filter, PlanFilterPredicate::None) {
            out.push_str(&format!("{indent}Filter: "));
            self.filter.pretty_print_to(out, &self.sources);
            out.push('\n');
        }

        // Group by
        if !self.group_by.is_empty() {
            out.push_str(&format!("{indent}GroupBy ["));
            for (i, col) in self.group_by.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                out.push_str(&col_name(col, &self.sources));
            }
            out.push_str("]\n");
        }

        // Aggregates
        for agg in &self.aggregates {
            let func = agg_name(agg.func);
            out.push_str(&format!("{indent}Aggregate {func}({})\n", col_name(&agg.col, &self.sources)));
        }

        // Order by
        if !self.order_by.is_empty() {
            out.push_str(&format!("{indent}OrderBy ["));
            for (i, spec) in self.order_by.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                let dir = match spec.direction {
                    OrderDirection::Asc => "ASC",
                    OrderDirection::Desc => "DESC",
                };
                out.push_str(&format!("{} {dir}", col_name(&spec.col, &self.sources)));
            }
            out.push_str("]\n");
        }

        // Limit
        match &self.limit {
            Some(PlanLimit::Value(n)) => out.push_str(&format!("{indent}Limit {n}\n")),
            Some(PlanLimit::Placeholder(name)) => out.push_str(&format!("{indent}Limit :{name}\n")),
            None => {}
        }

        // Result columns
        out.push_str(&format!("{indent}Output ["));
        for (i, rc) in self.result_columns.iter().enumerate() {
            if i > 0 { out.push_str(", "); }
            match rc {
                PlanResultColumn::Column { col, alias } => {
                    out.push_str(&col_name(col, &self.sources));
                    if let Some(a) = alias { out.push_str(&format!(" AS {a}")); }
                }
                PlanResultColumn::Aggregate { func, col, alias } => {
                    let f = agg_name(*func);
                    out.push_str(&format!("{f}({})", col_name(col, &self.sources)));
                    if let Some(a) = alias { out.push_str(&format!(" AS {a}")); }
                }
                PlanResultColumn::InvalidateOn { condition_idx, alias } => {
                    out.push_str(&format!("INVALIDATE_ON[{condition_idx}]"));
                    if let Some(a) = alias { out.push_str(&format!(" AS {a}")); }
                }
            }
        }
        out.push_str("]\n");
    }
}

impl PlanFilterPredicate {
    fn pretty_print_to(&self, out: &mut String, sources: &[PlanSourceEntry]) {
        match self {
            PlanFilterPredicate::Equals { col, value } =>
                out.push_str(&format!("{} = {}", col_name(col, sources), val(value))),
            PlanFilterPredicate::NotEquals { col, value } =>
                out.push_str(&format!("{} != {}", col_name(col, sources), val(value))),
            PlanFilterPredicate::GreaterThan { col, value } =>
                out.push_str(&format!("{} > {}", col_name(col, sources), val(value))),
            PlanFilterPredicate::GreaterThanOrEqual { col, value } =>
                out.push_str(&format!("{} >= {}", col_name(col, sources), val(value))),
            PlanFilterPredicate::LessThan { col, value } =>
                out.push_str(&format!("{} < {}", col_name(col, sources), val(value))),
            PlanFilterPredicate::LessThanOrEqual { col, value } =>
                out.push_str(&format!("{} <= {}", col_name(col, sources), val(value))),

            PlanFilterPredicate::ColumnEquals { left, right } =>
                out.push_str(&format!("{} = {}", col_name(left, sources), col_name(right, sources))),
            PlanFilterPredicate::ColumnNotEquals { left, right } =>
                out.push_str(&format!("{} != {}", col_name(left, sources), col_name(right, sources))),
            PlanFilterPredicate::ColumnGreaterThan { left, right } =>
                out.push_str(&format!("{} > {}", col_name(left, sources), col_name(right, sources))),
            PlanFilterPredicate::ColumnGreaterThanOrEqual { left, right } =>
                out.push_str(&format!("{} >= {}", col_name(left, sources), col_name(right, sources))),
            PlanFilterPredicate::ColumnLessThan { left, right } =>
                out.push_str(&format!("{} < {}", col_name(left, sources), col_name(right, sources))),
            PlanFilterPredicate::ColumnLessThanOrEqual { left, right } =>
                out.push_str(&format!("{} <= {}", col_name(left, sources), col_name(right, sources))),

            PlanFilterPredicate::IsNull { col } =>
                out.push_str(&format!("{} IS NULL", col_name(col, sources))),
            PlanFilterPredicate::IsNotNull { col } =>
                out.push_str(&format!("{} IS NOT NULL", col_name(col, sources))),

            PlanFilterPredicate::And(l, r) => {
                out.push('(');
                l.pretty_print_to(out, sources);
                out.push_str(" AND ");
                r.pretty_print_to(out, sources);
                out.push(')');
            }
            PlanFilterPredicate::Or(l, r) => {
                out.push('(');
                l.pretty_print_to(out, sources);
                out.push_str(" OR ");
                r.pretty_print_to(out, sources);
                out.push(')');
            }

            PlanFilterPredicate::In { col, values } => {
                out.push_str(&format!("{} IN (", col_name(col, sources)));
                for (i, v) in values.iter().enumerate() {
                    if i > 0 { out.push_str(", "); }
                    out.push_str(&val(v));
                }
                out.push(')');
            }

            PlanFilterPredicate::InMaterialized { col, mat_id } =>
                out.push_str(&format!("{} IN $mat{mat_id}", col_name(col, sources))),
            PlanFilterPredicate::CompareMaterialized { col, op, mat_id } => {
                let op_str = match op {
                    Operator::Eq => "=", Operator::Neq => "!=",
                    Operator::Lt => "<", Operator::Gt => ">",
                    Operator::Lte => "<=", Operator::Gte => ">=",
                    Operator::And => "AND", Operator::Or => "OR",
                };
                out.push_str(&format!("{} {op_str} $mat{mat_id}", col_name(col, sources)));
            }

            PlanFilterPredicate::None => out.push_str("TRUE"),
        }
    }
}

fn col_name(col: &ColumnRef, sources: &[PlanSourceEntry]) -> String {
    if let Some(source) = sources.get(col.source) {
        if let Some(cdef) = source.schema.columns.get(col.col) {
            return format!("{}.{}", source.table, cdef.name);
        }
    }
    format!("#{}.{}", col.source, col.col)
}

fn val(v: &Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => format!("'{s}'"),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Placeholder(name) => format!(":{name}"),
    }
}

fn agg_name(func: AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
    }
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
    InvalidateOn {
        condition_idx: usize,
        alias: Option<String>,
    },
}
