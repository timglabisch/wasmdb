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
    String,
    U32,
    I32,
    U64,
    I64,
}

#[derive(Debug, Clone)]
pub enum AstTableConstraint {
    PrimaryKey { columns: Vec<String> },
    Index { name: Option<String>, columns: Vec<String> },
}
