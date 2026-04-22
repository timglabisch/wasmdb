//! MySQL/TiDB adapter for the sync server: replay a client-emitted `ZSet`
//! into a sqlx transaction.
//!
//! The heavy lifting sits in [`apply_zset`]: it walks the entries and
//! emits one `INSERT` per `+1` row and one `DELETE … WHERE <pk> = ?` per
//! `-1` row, using column metadata from the supplied `TableSchema` map.
//!
//! Commands that are happy with a verbatim replay get that behaviour for
//! free via the blanket [`ServerCommand`] impl. Commands that need
//! authoritative server-side logic (uniqueness checks, balance lookups,
//! …) override `execute_server` and drop down to raw sqlx inside it.

pub mod apply;
pub mod server_command;

pub use apply::apply_zset;
pub use server_command::ServerCommand;
