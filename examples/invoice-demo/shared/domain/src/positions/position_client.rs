use sql_engine::storage::Uuid;
use sqlbuilder::FromRow;
use tables_storage::row;

#[row(table = "positions")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
#[derive(FromRow)]
pub struct Position {
    #[pk]
    pub id: Uuid,
    pub invoice_id: Uuid,
    pub position_nr: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub product_id: Option<Uuid>,
    pub item_number: String,
    pub unit: String,
    pub discount_pct: i64,
    pub cost_price: i64,
    pub position_type: String,
}
