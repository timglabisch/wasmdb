use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "payments")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Payment {
    #[pk]
    pub id: Uuid,
    pub invoice_id: Uuid,
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}
