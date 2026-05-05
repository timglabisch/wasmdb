use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "messages")]
#[export(name = "", groups = ["all"])]
pub struct Message {
    #[pk]
    pub id: Uuid,
    pub room_id: Uuid,
    pub author_user_id: Uuid,
    pub body: String,
    pub created_at: String,
}
