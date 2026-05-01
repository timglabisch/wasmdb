use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "recurring_invoices")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct RecurringInvoice {
    #[pk]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub template_name: String,
    pub interval_unit: String,
    pub interval_value: i64,
    pub next_run: String,
    pub last_run: String,
    pub enabled: i64,
    pub status_template: String,
    pub notes_template: String,
}
