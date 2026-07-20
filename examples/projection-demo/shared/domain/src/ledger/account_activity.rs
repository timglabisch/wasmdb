use tables_storage::row;

/// The demand-materialized read model (design §12): one row per ACTIVATED
/// account. Unlike `balance` (data presence: every account with log rows
/// is materialized), this table only holds accounts whose `activity`
/// instance is currently activated — the 10k-entities scenario in
/// miniature. Owned by `ActivityFold`; no command writes it.
#[row]
#[export(name = "", groups = ["all"])]
pub struct AccountActivity {
    #[pk]
    pub account: String,
    /// Number of non-negative entries folded in.
    pub deposits: i64,
    /// Number of negative entries folded in.
    pub withdrawals: i64,
    /// The signed amount with the largest absolute value seen.
    pub largest_cents: i64,
}
