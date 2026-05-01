use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "activity_log")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct ActivityLogEntry {
    #[pk]
    pub id: Uuid,
    pub timestamp: String,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub action: String,
    pub actor: String,
    pub detail: String,
}
