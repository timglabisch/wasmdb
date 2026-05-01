//! Server-only helpers: BINARY(16) → `Uuid` conversion for sqlx rows.
//! Used by `*_server.rs` files when they fall back to raw sqlx (the
//! SeaORM path lives directly in those files and does not need this).

#![cfg(feature = "server")]

use sqlx::Row;

/// Convert a sqlx-fetched `BINARY(16)` column into a `Uuid`. Failure to
/// produce 16 bytes means the column is malformed.
pub fn try_uuid(
    row: &sqlx::mysql::MySqlRow,
    col: &str,
) -> Result<sql_engine::storage::Uuid, sqlx::Error> {
    let bytes: Vec<u8> = row.try_get(col)?;
    let arr: [u8; 16] = bytes.try_into().map_err(|v: Vec<u8>| {
        sqlx::Error::Decode(
            format!("column {col}: expected 16 bytes, got {}", v.len()).into(),
        )
    })?;
    Ok(sql_engine::storage::Uuid(arr))
}

/// Same as `try_uuid` but tolerates SQL NULL by returning `None`.
pub fn try_uuid_opt(
    row: &sqlx::mysql::MySqlRow,
    col: &str,
) -> Result<Option<sql_engine::storage::Uuid>, sqlx::Error> {
    let Some(bytes): Option<Vec<u8>> = row.try_get(col)? else {
        return Ok(None);
    };
    let arr: [u8; 16] = bytes.try_into().map_err(|v: Vec<u8>| {
        sqlx::Error::Decode(
            format!("column {col}: expected 16 bytes, got {}", v.len()).into(),
        )
    })?;
    Ok(Some(sql_engine::storage::Uuid(arr)))
}
