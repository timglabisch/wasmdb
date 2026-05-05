use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "rooms")]
#[export(name = "", groups = ["all"])]
pub struct Room {
    #[pk]
    pub id: Uuid,
    pub name: String,
    pub owner_user_id: Uuid,
}
