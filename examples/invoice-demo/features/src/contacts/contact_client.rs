use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "contacts")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Contact {
    #[pk]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub role: String,
    pub is_primary: i64,
}
