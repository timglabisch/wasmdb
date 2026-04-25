pub mod cols;
pub mod customers;
pub mod invoices;
pub mod products;
pub mod recurring;
pub mod sepa_mandates;
pub mod activity_log;

use std::collections::HashMap;

use sql_engine::schema::{DataType, TableSchema};

pub fn build_table_schemas() -> HashMap<String, TableSchema> {
    let mut s = HashMap::new();
    customers::register(&mut s);
    invoices::register(&mut s);
    products::register(&mut s);
    recurring::register(&mut s);
    sepa_mandates::register(&mut s);
    activity_log::register(&mut s);
    s
}

/// Validate that the live TiDB database has the column layout that
/// `TableSchema` expects. Fails loud at boot so schema drift cannot silently
/// corrupt ZSets (column-order has to match 1:1 for the columns the client
/// knows about).
///
/// MySQL columns not present in `TableSchema` are skipped. This lets us host
/// server-only columns (e.g. `tenant_id`) in TiDB without forcing them into
/// the client's row layout. The order-strict comparison runs over the
/// filtered, schema-known column subset only.
pub async fn assert_mysql_matches(
    pool: &sqlx::MySqlPool,
    schemas: &HashMap<String, TableSchema>,
) -> Result<(), String> {
    for (table, schema) in schemas {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT COLUMN_NAME, DATA_TYPE
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
             ORDER BY ORDINAL_POSITION",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("TiDB query for table `{table}`: {e}"))?;

        if rows.is_empty() {
            return Err(format!(
                "table `{table}` missing in TiDB — run examples/invoice-demo/sql/001_init.sql"
            ));
        }

        let known: std::collections::HashSet<&str> =
            schema.columns.iter().map(|c| c.name.as_str()).collect();
        let filtered: Vec<&(String, String)> = rows
            .iter()
            .filter(|(name, _)| known.contains(name.as_str()))
            .collect();

        if filtered.len() != schema.columns.len() {
            return Err(format!(
                "table `{table}`: expected {} client-visible columns, TiDB has {} matching",
                schema.columns.len(),
                filtered.len(),
            ));
        }
        for (i, col) in schema.columns.iter().enumerate() {
            let (name, dtype) = filtered[i];
            if name != &col.name {
                return Err(format!(
                    "table `{table}` column #{i}: expected `{}`, TiDB has `{}` (after filtering server-only columns)",
                    col.name, name,
                ));
            }
            let ok = match col.data_type {
                DataType::I64 => matches!(dtype.as_str(), "bigint" | "int"),
                DataType::String => {
                    matches!(dtype.as_str(), "varchar" | "char" | "text" | "longtext")
                }
                DataType::Uuid => {
                    matches!(dtype.as_str(), "binary" | "char" | "varchar")
                }
            };
            if !ok {
                return Err(format!(
                    "table `{table}` column `{}`: expected `{:?}`, TiDB has `{}`",
                    col.name, col.data_type, dtype,
                ));
            }
        }
    }
    Ok(())
}
