use tables_storage::row;

/// The derived read model: one row per account, maintained entirely by
/// `BalanceFold`. No command writes it directly — it is the projection's
/// output. `#[export]` emits the matching TypeScript type into the
/// generated package so the UI reads it typed.
#[row]
#[export(name = "", groups = ["all"])]
pub struct Balance {
    #[pk]
    pub account: String,
    /// Running balance in cents (signed: deposits add, withdrawals subtract).
    pub balance_cents: i64,
    /// Number of ledger entries folded into this balance.
    pub entries: i64,
}
