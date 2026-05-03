//! Cross-feature constants and helpers.

/// Hardcoded tenant for the demo. Server-side TiDB queries bind this on
/// every INSERT/UPDATE/DELETE/SELECT. Client-side `Database` does not know
/// about tenants — the column lives in TiDB only.
pub const DEMO_TENANT_ID: i64 = 0;
