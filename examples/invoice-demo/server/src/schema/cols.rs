use sql_engine::schema::{ColumnSchema, DataType};

pub fn col(name: &str, ty: DataType) -> ColumnSchema {
    ColumnSchema { name: name.into(), data_type: ty, nullable: false }
}

pub fn nullable_col(name: &str, ty: DataType) -> ColumnSchema {
    ColumnSchema { name: name.into(), data_type: ty, nullable: true }
}

pub fn str_col(name: &str) -> ColumnSchema { col(name, DataType::String) }
pub fn i64_col(name: &str) -> ColumnSchema { col(name, DataType::I64) }
pub fn uuid_col(name: &str) -> ColumnSchema { col(name, DataType::Uuid) }
pub fn nullable_uuid_col(name: &str) -> ColumnSchema { nullable_col(name, DataType::Uuid) }
