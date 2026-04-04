#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub table: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    /// Resolve a column reference to an index.
    /// If `table` is None, matches the first column with the given name.
    pub fn resolve(&self, table: Option<&str>, column: &str) -> Option<usize> {
        self.columns.iter().position(|c| {
            c.name == column
                && match (table, &c.table) {
                    (Some(t), Some(ct)) => t == ct,
                    (None, _) => true,
                    (Some(_), None) => false,
                }
        })
    }

    /// Merge two schemas (e.g. for a join).
    pub fn merge(left: &Schema, right: &Schema) -> Schema {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().cloned());
        Schema { columns }
    }
}
