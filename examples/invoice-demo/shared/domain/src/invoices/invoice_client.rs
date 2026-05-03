use sql_engine::storage::Uuid;
use sqlbuilder::FromRow;
use tables_storage::row;

#[row(table = "invoices")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
#[derive(FromRow)]
pub struct Invoice {
    #[pk]
    pub id: Uuid,
    pub customer_id: Option<Uuid>,
    pub number: String,
    pub status: String,
    pub date_issued: String,
    pub date_due: String,
    pub notes: String,
    pub doc_type: String,
    pub parent_id: Option<Uuid>,
    pub service_date: String,
    pub cash_allowance_pct: i64,
    pub cash_allowance_days: i64,
    pub discount_pct: i64,
    pub payment_method: String,
    pub sepa_mandate_id: Option<Uuid>,
    pub currency: String,
    pub language: String,
    pub project_ref: String,
    pub external_id: String,
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,
}
