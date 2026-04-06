use std::collections::HashMap;

pub struct TableDef {
    pub name: String,
    pub field_ids: Vec<u16>,
}

pub struct Schema {
    pub tables: Vec<TableDef>,
    pub field_names: Vec<String>,
    pub field_ids: HashMap<String, u16>,
    pub table_name_to_id: HashMap<String, u16>,
}

impl Schema {
    pub fn new() -> Self {
        let mut s = Self {
            tables: Vec::new(),
            field_names: Vec::new(),
            field_ids: HashMap::new(),
            table_name_to_id: HashMap::new(),
        };
        s.intern_field("_table"); // FIELD_TABLE = 0
        s.intern_field("_id");    // FIELD_ID = 1
        s
    }

    pub fn intern_field(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.field_ids.get(name) {
            return id;
        }
        let id = self.field_names.len() as u16;
        self.field_names.push(name.to_string());
        self.field_ids.insert(name.to_string(), id);
        id
    }

    pub fn register_table(&mut self, name: String, fields: Vec<String>) -> u16 {
        let field_ids: Vec<u16> = fields.iter().map(|f| self.intern_field(f)).collect();
        let id = self.tables.len() as u16;
        self.table_name_to_id.insert(name.clone(), id);
        self.tables.push(TableDef { name, field_ids });
        id
    }
}
