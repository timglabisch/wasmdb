//! Row + queries with the native UUID type — UUID PK, optional UUID column,
//! mixed-type query arguments. Mirrors the `customers` shape but exercises
//! the `Uuid` codegen path end-to-end.

use sql_engine::storage::Uuid;
use tables_storage::{query, row};

use crate::AppCtx;

#[row]
pub struct Contact {
    #[pk]
    pub id: Uuid,
    pub name: String,
    /// Optional cross-system identifier (also a UUID).
    pub external_id: Option<Uuid>,
}

#[query]
async fn by_id(id: Uuid, ctx: &AppCtx) -> Result<Vec<Contact>, String> {
    Ok(ctx
        .contacts
        .iter()
        .filter(|c| c.id == id)
        .cloned()
        .collect())
}

#[query]
async fn by_external_id(external_id: Uuid, ctx: &AppCtx) -> Result<Vec<Contact>, String> {
    Ok(ctx
        .contacts
        .iter()
        .filter(|c| c.external_id == Some(external_id))
        .cloned()
        .collect())
}

#[query]
async fn by_name_and_id(
    name: String,
    id: Uuid,
    ctx: &AppCtx,
) -> Result<Vec<Contact>, String> {
    Ok(ctx
        .contacts
        .iter()
        .filter(|c| c.name == name && c.id == id)
        .cloned()
        .collect())
}
