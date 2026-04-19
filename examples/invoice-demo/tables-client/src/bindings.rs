//! JS entry points — generated via `tables_client::wasm_fetch!`.
//! One macro invocation per fetcher; serde/borsh framing is handled inside.

use crate::{ByOwner, TABLE_FETCH_URL};

tables_client::wasm_fetch!(fetch_customers_by_owner, ByOwner, TABLE_FETCH_URL);
