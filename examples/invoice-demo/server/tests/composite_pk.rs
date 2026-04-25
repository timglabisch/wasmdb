//! Integration tests for the (tenant_id, id) composite-PK invariants and the
//! ON-DUPLICATE-KEY idempotency guarantees added in Slice 3.
//!
//! Marked `#[ignore]` because they require a running TiDB on
//! `mysql://root:@127.0.0.1:4000/invoice_demo` with the schema from
//! `examples/invoice-demo/sql/001_init.sql` applied.
//!
//! Run locally:  cargo test -p invoice-demo-server -- --ignored

use sqlx::MySqlPool;

const CUST_ID: [u8; 16] = [0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88,
                           0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00];

async fn pool() -> MySqlPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:@127.0.0.1:4000/invoice_demo".into());
    MySqlPool::connect(&url).await.expect("connect TiDB")
}

async fn cleanup_tenants(pool: &MySqlPool, tenants: &[i64]) {
    for &tenant in tenants {
        sqlx::query("DELETE FROM positions WHERE tenant_id = ?")
            .bind(tenant).execute(pool).await.ok();
        sqlx::query("DELETE FROM invoices WHERE tenant_id = ?")
            .bind(tenant).execute(pool).await.ok();
        sqlx::query("DELETE FROM customers WHERE tenant_id = ?")
            .bind(tenant).execute(pool).await.ok();
    }
}

async fn insert_customer(pool: &MySqlPool, tenant: i64, id: &[u8; 16], name: &str) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO customers (tenant_id, id, name, email, created_at, company_type, tax_id, vat_id, payment_terms_days, default_discount_pct, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country, default_iban, default_bic, notes) \
         VALUES (?, ?, ?, '', '', '', '', '', 0, 0, '', '', '', '', '', '', '', '', '', '', '') \
         ON DUPLICATE KEY UPDATE id = id",
    )
    .bind(tenant)
    .bind(&id[..])
    .bind(name)
    .execute(pool)
    .await
}

#[tokio::test]
#[ignore]
async fn same_uuid_two_tenants_no_conflict() {
    const T_A: i64 = 9001;
    const T_B: i64 = 9002;
    let pool = pool().await;
    cleanup_tenants(&pool, &[T_A, T_B]).await;

    insert_customer(&pool, T_A, &CUST_ID, "tenant-A").await
        .expect("insert into tenant A");
    insert_customer(&pool, T_B, &CUST_ID, "tenant-B").await
        .expect("insert same UUID into tenant B should succeed — composite PK");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM customers WHERE id = ? AND tenant_id IN (?, ?)",
    )
        .bind(&CUST_ID[..]).bind(T_A).bind(T_B)
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2, "both tenants should hold a row with this UUID");

    cleanup_tenants(&pool, &[T_A, T_B]).await;
}

#[tokio::test]
#[ignore]
async fn cross_tenant_fk_rejected() {
    const T_A: i64 = 9003;
    const T_B: i64 = 9004;
    let pool = pool().await;
    cleanup_tenants(&pool, &[T_A, T_B]).await;

    insert_customer(&pool, T_A, &CUST_ID, "tenant-A").await
        .expect("seed customer in tenant A");

    let invoice_id: [u8; 16] = [0xab; 16];
    let res = sqlx::query(
        "INSERT INTO invoices (tenant_id, id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
         VALUES (?, ?, ?, '', 'draft', '', '', '', 'invoice', ?, '', 0, 0, 0, 'transfer', ?, 'EUR', 'de', '', '', '', '', '', '', '', '', '', '')",
    )
    .bind(T_B)
    .bind(&invoice_id[..])
    .bind(&CUST_ID[..])
    .bind(&[0u8; 16][..])
    .bind(&[0u8; 16][..])
    .execute(&pool)
    .await;

    let err = res.expect_err("FK should reject cross-tenant customer reference");
    let msg = err.to_string();
    assert!(msg.contains("1452") || msg.to_lowercase().contains("foreign key"),
        "expected FK constraint violation (1452), got: {msg}");

    cleanup_tenants(&pool, &[T_A, T_B]).await;
}

#[tokio::test]
#[ignore]
async fn double_insert_idempotent() {
    const T: i64 = 9005;
    let pool = pool().await;
    cleanup_tenants(&pool, &[T]).await;

    insert_customer(&pool, T, &CUST_ID, "first").await
        .expect("first insert");
    insert_customer(&pool, T, &CUST_ID, "second-with-different-name").await
        .expect("second insert with same (tenant,id) must not error — ON DUPLICATE KEY");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM customers WHERE tenant_id = ? AND id = ?")
        .bind(T).bind(&CUST_ID[..])
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1, "exactly one row after two inserts");

    let name: String = sqlx::query_scalar("SELECT name FROM customers WHERE tenant_id = ? AND id = ?")
        .bind(T).bind(&CUST_ID[..])
        .fetch_one(&pool).await.unwrap();
    assert_eq!(name, "first", "silent-ignore policy: original row body wins");

    cleanup_tenants(&pool, &[T]).await;
}
