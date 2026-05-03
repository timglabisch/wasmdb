use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "sepa_mandates")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct SepaMandate {
    #[pk]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub mandate_ref: String,
    pub iban: String,
    pub bic: String,
    pub holder_name: String,
    pub signed_at: String,
    pub status: String,
}
