//! Client-side `Product` DTO. Wire format + TypeScript codegen source.
//! Has only the fields the client should see — no `tenant_id`, no audit
//! columns. Server-side schema lives in `product_server.rs` and is
//! intentionally decoupled.

use sql_engine::storage::Uuid;
use tables_storage::row;

#[row(table = "products")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Product {
    #[pk]
    pub id: Uuid,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub cost_price: i64,
    pub active: i64,
}
