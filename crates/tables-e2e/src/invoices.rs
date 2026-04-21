//! Invoice row + queries. `note: Option<String>` exercises the nullable-
//! string column path; queries cover the join-partner role against Customer.
//!
//! Named `Invoice` (not `Order`) because the SQL lexer reserves `ORDER`
//! as a keyword, and the generated table name uses the snake_case of the
//! struct ident.

use tables_storage::{query, row};

use crate::AppCtx;

#[row]
pub struct Invoice {
    #[pk]
    pub id: i64,
    pub customer_id: i64,
    pub amount: i64,
    pub note: Option<String>,
}

#[query]
async fn by_customer(customer_id: i64, ctx: &AppCtx) -> Result<Vec<Invoice>, String> {
    Ok(ctx
        .invoices
        .iter()
        .filter(|o| o.customer_id == customer_id)
        .cloned()
        .collect())
}

#[query]
async fn with_note_containing(needle: String, ctx: &AppCtx) -> Result<Vec<Invoice>, String> {
    Ok(ctx
        .invoices
        .iter()
        .filter(|o| o.note.as_deref().is_some_and(|n| n.contains(&needle)))
        .cloned()
        .collect())
}

#[query]
async fn with_optional_note(
    note: Option<String>,
    ctx: &AppCtx,
) -> Result<Vec<Invoice>, String> {
    Ok(ctx
        .invoices
        .iter()
        .filter(|o| o.note == note)
        .cloned()
        .collect())
}

#[query]
async fn min_amount(min: i64, ctx: &AppCtx) -> Result<Vec<Invoice>, String> {
    Ok(ctx
        .invoices
        .iter()
        .filter(|o| o.amount >= min)
        .cloned()
        .collect())
}
