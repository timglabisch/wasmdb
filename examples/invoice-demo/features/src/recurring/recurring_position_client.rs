use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "recurring_positions")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct RecurringPosition {
    #[pk]
    pub id: Uuid,
    pub recurring_id: Uuid,
    pub position_nr: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub unit: String,
    pub item_number: String,
    pub discount_pct: i64,
}
