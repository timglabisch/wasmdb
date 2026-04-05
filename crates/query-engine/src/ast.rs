#[derive(Debug, Clone)]
pub struct AstSelect {
    pub sources: Vec<AstSourceEntry>,
    pub filter: Vec<AstExpr>,
    pub group_by: Vec<AstExpr>,
    pub order_by: Vec<AstOrderSpec>,
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
    pub table: String,
    /// None for the first table, Some for joined tables.
    pub join: Option<AstJoinClause>,
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
