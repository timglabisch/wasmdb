use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "users")]
#[export(name = "", groups = ["all"])]
pub struct User {
    #[pk]
    pub id: Uuid,
    pub name: String,
    pub status: String,
}
