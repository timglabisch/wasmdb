//! Client-side `Customer` DTO. Wire format + TypeScript codegen source.
//! Has only the fields the client should see — no `tenant_id`, no audit
//! columns. Server-side schema lives in `customer_server.rs` and is
//! intentionally decoupled.

use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "customers")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Customer {
    #[pk]
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub created_at: String,
    pub company_type: String,
    pub tax_id: String,
    pub vat_id: String,
    pub payment_terms_days: i64,
    pub default_discount_pct: i64,
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,
    pub default_iban: String,
    pub default_bic: String,
    pub notes: String,
}
