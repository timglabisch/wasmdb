//! Customer row + fetchers. The `#[row]` macro derives Borsh/Serde and
//! `impl Row`; `#[fetcher]` binds a params struct to that row + a wire id.

use tables_client::{fetcher, row};

#[row]
pub struct Customer {
    #[pk] pub id: i64,
    pub name: String,
}

#[fetcher(row = Customer)]
pub struct ByOwner {
    pub owner_id: i64,
}
