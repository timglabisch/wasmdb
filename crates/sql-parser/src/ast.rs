#[derive(Debug, Clone)]
pub enum Statement {
    Select(AstSelect),
    Insert(AstInsert),
    Delete(AstDelete),
    Update(AstUpdate),
    CreateTable(AstCreateTable),
}

#[derive(Debug, Clone)]
pub struct AstDelete {
    pub table: String,
    pub filter: Option<AstExpr>,
}

#[derive(Debug, Clone)]
pub struct AstUpdate {
    pub table: String,
    pub assignments: Vec<(String, AstExpr)>,
    pub filter: Option<AstExpr>,
}

#[derive(Debug, Clone)]
pub struct AstCreateTable {
    pub name: String,
    pub columns: Vec<AstColumnDef>,
    pub constraints: Vec<AstTableConstraint>,
}

#[derive(Debug, Clone)]
pub struct AstColumnDef {
    pub name: String,
    pub data_type: AstDataType,
    pub not_null: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AstDataType {
    I64,
    String,
    Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AstIndexType {
    BTree,
    Hash,
}

#[derive(Debug, Clone)]
pub enum AstTableConstraint {
    PrimaryKey { columns: Vec<String> },
    Index { name: Option<String>, columns: Vec<String>, index_type: AstIndexType },
}

#[derive(Debug, Clone)]
pub struct AstInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<AstExpr>>,
}

#[derive(Debug, Clone)]
pub struct AstSelect {
    pub sources: Vec<AstSourceEntry>,
    pub filter: Vec<AstExpr>,
    pub group_by: Vec<AstExpr>,
    pub order_by: Vec<AstOrderSpec>,
    pub limit: Option<AstLimit>,
    pub result_columns: Vec<AstResultColumn>,
}

#[derive(Debug, Clone)]
pub struct AstOrderSpec {
    pub expr: AstExpr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct AstSourceEntry {
    pub source: AstSource,
    /// Optional `AS alias` after the source expression.
    pub alias: Option<String>,
    /// None for the first table, Some for joined tables.
    pub join: Option<AstJoinClause>,
}

/// What sits in the FROM-clause slot. Today only a plain table name —
/// requirement-style function-call sources (`schema.function(args)`) were
/// removed when first-class Requirements moved into the dedicated
/// `requirements` crate. The variant is kept as a single-variant enum so
/// future source kinds (subqueries, derived requirements) can slot in
/// without churn.
#[derive(Debug, Clone)]
pub enum AstSource {
    Table(String),
}

impl AstSource {
    /// Name shown in error messages and used for column-resolution lookups
    /// against the plain-table registry.
    pub fn name(&self) -> &str {
        let AstSource::Table(t) = self;
        t
    }
}

#[derive(Debug, Clone)]
pub struct AstJoinClause {
    pub join_type: JoinType,
    pub on: Vec<AstExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub enum AstExpr {
    Column(AstColumnRef),
    Literal(Value),
    Binary {
        left: Box<AstExpr>,
        op: Operator,
        right: Box<AstExpr>,
    },
    Aggregate {
        func: AggFunc,
        arg: Box<AstExpr>,
    },
    InList {
        expr: Box<AstExpr>,
        values: Vec<AstExpr>,
    },
    InSubquery {
        expr: Box<AstExpr>,
        subquery: Box<AstSelect>,
    },
    Subquery(Box<AstSelect>),
    Reactive(Box<AstExpr>),
}

#[derive(Debug, Clone)]
pub struct AstColumnRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
    /// Native UUID literal (16 raw bytes). Parsed from `UUID 'xxxx-...'` syntax;
    /// the wire format is the canonical hyphenated 8-4-4-4-12 hex form.
    Uuid([u8; 16]),
    Placeholder(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstLimit {
    Value(u64),
    Placeholder(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct AstResultColumn {
    pub expr: AstExpr,
    pub alias: Option<String>,
}
