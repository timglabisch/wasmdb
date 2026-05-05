use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "counters")]
#[export(name = "", groups = ["all"])]
pub struct Counter {
    #[pk]
    pub id: Uuid,
    pub label: String,
    pub value: i64,
}
