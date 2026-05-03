-- Invoice-demo — full schema. Column order mirrors
-- examples/invoice-demo/server/src/schema/*.rs exactly — required for
-- the MysqlRunner ZSet synthesis (rows are decoded in schema order).
--
-- `tenant_id` is server-only: it is NOT mirrored in the client TableSchema.
-- assert_mysql_matches() filters MySQL columns not present in TableSchema
-- before the order-strict comparison, so `tenant_id` lives in TiDB only.
--
-- All queries against this schema MUST set `tenant_id` explicitly — the
-- column has NO DEFAULT. The hardcoded value for the demo is `0`.
--
-- Apply:   mysql -h 127.0.0.1 -P 4000 -u root < examples/invoice-demo/server/sql/001_init.sql

CREATE DATABASE IF NOT EXISTS invoice_demo;
USE invoice_demo;

DROP TABLE IF EXISTS activity_log;
DROP TABLE IF EXISTS recurring_positions;
DROP TABLE IF EXISTS recurring_invoices;
DROP TABLE IF EXISTS sepa_mandates;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS positions;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    tenant_id            BIGINT       NOT NULL,
    id                   BINARY(16)   NOT NULL,
    name                 VARCHAR(255) NOT NULL,
    email                VARCHAR(255) NOT NULL,
    created_at           VARCHAR(64)  NOT NULL,
    company_type         VARCHAR(64)  NOT NULL,
    tax_id               VARCHAR(64)  NOT NULL,
    vat_id               VARCHAR(64)  NOT NULL,
    payment_terms_days   BIGINT       NOT NULL,
    default_discount_pct BIGINT       NOT NULL,
    billing_street       VARCHAR(255) NOT NULL,
    billing_zip          VARCHAR(32)  NOT NULL,
    billing_city         VARCHAR(128) NOT NULL,
    billing_country      VARCHAR(64)  NOT NULL,
    shipping_street      VARCHAR(255) NOT NULL,
    shipping_zip         VARCHAR(32)  NOT NULL,
    shipping_city        VARCHAR(128) NOT NULL,
    shipping_country     VARCHAR(64)  NOT NULL,
    default_iban         VARCHAR(64)  NOT NULL,
    default_bic          VARCHAR(32)  NOT NULL,
    notes                TEXT         NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE contacts (
    tenant_id   BIGINT       NOT NULL,
    id          BINARY(16)   NOT NULL,
    customer_id BINARY(16)   NOT NULL,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    phone       VARCHAR(64)  NOT NULL,
    role        VARCHAR(64)  NOT NULL,
    is_primary  BIGINT       NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_contacts_customer (tenant_id, customer_id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
);

CREATE TABLE invoices (
    tenant_id            BIGINT       NOT NULL,
    id                   BINARY(16)   NOT NULL,
    customer_id          BINARY(16)   NULL,
    number               VARCHAR(64)  NOT NULL,
    status               VARCHAR(32)  NOT NULL,
    date_issued          VARCHAR(32)  NOT NULL,
    date_due             VARCHAR(32)  NOT NULL,
    notes                TEXT         NOT NULL,
    doc_type             VARCHAR(32)  NOT NULL,
    parent_id            BINARY(16)   NULL,
    service_date         VARCHAR(32)  NOT NULL,
    cash_allowance_pct   BIGINT       NOT NULL,
    cash_allowance_days  BIGINT       NOT NULL,
    discount_pct         BIGINT       NOT NULL,
    payment_method       VARCHAR(32)  NOT NULL,
    sepa_mandate_id      BINARY(16)   NULL,
    currency             VARCHAR(8)   NOT NULL,
    language             VARCHAR(8)   NOT NULL,
    project_ref          VARCHAR(128) NOT NULL,
    external_id          VARCHAR(128) NOT NULL,
    billing_street       VARCHAR(255) NOT NULL,
    billing_zip          VARCHAR(32)  NOT NULL,
    billing_city         VARCHAR(128) NOT NULL,
    billing_country      VARCHAR(64)  NOT NULL,
    shipping_street      VARCHAR(255) NOT NULL,
    shipping_zip         VARCHAR(32)  NOT NULL,
    shipping_city        VARCHAR(128) NOT NULL,
    shipping_country     VARCHAR(64)  NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_invoices_customer (tenant_id, customer_id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
    -- customer_id, parent_id, sepa_mandate_id are nullable (unset on draft /
    -- non-credit-note / non-SEPA). MySQL skips FK checks on NULL columns by
    -- default, so the FK above only fires when customer_id is actually set.
);

CREATE TABLE positions (
    tenant_id     BIGINT       NOT NULL,
    id            BINARY(16)   NOT NULL,
    invoice_id    BINARY(16)   NOT NULL,
    position_nr   BIGINT       NOT NULL,
    description   TEXT         NOT NULL,
    quantity      BIGINT       NOT NULL,
    unit_price    BIGINT       NOT NULL,
    tax_rate      BIGINT       NOT NULL,
    product_id    BINARY(16)   NULL,
    item_number   VARCHAR(64)  NOT NULL,
    unit          VARCHAR(32)  NOT NULL,
    discount_pct  BIGINT       NOT NULL,
    cost_price    BIGINT       NOT NULL,
    position_type VARCHAR(32)  NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_positions_invoice (tenant_id, invoice_id),
    FOREIGN KEY (tenant_id, invoice_id) REFERENCES invoices (tenant_id, id)
    -- product_id is nullable (free-text positions without a product link).
);

CREATE TABLE payments (
    tenant_id  BIGINT       NOT NULL,
    id         BINARY(16)   NOT NULL,
    invoice_id BINARY(16)   NOT NULL,
    amount     BIGINT       NOT NULL,
    paid_at    VARCHAR(32)  NOT NULL,
    method     VARCHAR(32)  NOT NULL,
    reference  VARCHAR(128) NOT NULL,
    note       TEXT         NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_payments_invoice (tenant_id, invoice_id),
    FOREIGN KEY (tenant_id, invoice_id) REFERENCES invoices (tenant_id, id)
);

CREATE TABLE products (
    tenant_id   BIGINT       NOT NULL,
    id          BINARY(16)   NOT NULL,
    sku         VARCHAR(64)  NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT         NOT NULL,
    unit        VARCHAR(32)  NOT NULL,
    unit_price  BIGINT       NOT NULL,
    tax_rate    BIGINT       NOT NULL,
    cost_price  BIGINT       NOT NULL,
    active      BIGINT       NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE sepa_mandates (
    tenant_id   BIGINT       NOT NULL,
    id          BINARY(16)   NOT NULL,
    customer_id BINARY(16)   NOT NULL,
    mandate_ref VARCHAR(64)  NOT NULL,
    iban        VARCHAR(64)  NOT NULL,
    bic         VARCHAR(32)  NOT NULL,
    holder_name VARCHAR(255) NOT NULL,
    signed_at   VARCHAR(32)  NOT NULL,
    status      VARCHAR(32)  NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_sepa_customer (tenant_id, customer_id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
);

CREATE TABLE recurring_invoices (
    tenant_id       BIGINT       NOT NULL,
    id              BINARY(16)   NOT NULL,
    customer_id     BINARY(16)   NOT NULL,
    template_name   VARCHAR(255) NOT NULL,
    interval_unit   VARCHAR(16)  NOT NULL,
    interval_value  BIGINT       NOT NULL,
    next_run        VARCHAR(32)  NOT NULL,
    last_run        VARCHAR(32)  NOT NULL,
    enabled         BIGINT       NOT NULL,
    status_template VARCHAR(32)  NOT NULL,
    notes_template  TEXT         NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_recurring_customer (tenant_id, customer_id),
    FOREIGN KEY (tenant_id, customer_id) REFERENCES customers (tenant_id, id)
);

CREATE TABLE recurring_positions (
    tenant_id     BIGINT       NOT NULL,
    id            BINARY(16)   NOT NULL,
    recurring_id  BINARY(16)   NOT NULL,
    position_nr   BIGINT       NOT NULL,
    description   TEXT         NOT NULL,
    quantity      BIGINT       NOT NULL,
    unit_price    BIGINT       NOT NULL,
    tax_rate      BIGINT       NOT NULL,
    unit          VARCHAR(32)  NOT NULL,
    item_number   VARCHAR(64)  NOT NULL,
    discount_pct  BIGINT       NOT NULL,
    PRIMARY KEY (tenant_id, id),
    INDEX idx_recurring_pos_recurring (tenant_id, recurring_id),
    FOREIGN KEY (tenant_id, recurring_id) REFERENCES recurring_invoices (tenant_id, id)
);

CREATE TABLE activity_log (
    tenant_id   BIGINT       NOT NULL,
    id          BINARY(16)   NOT NULL,
    timestamp   VARCHAR(32)  NOT NULL,
    entity_type VARCHAR(64)  NOT NULL,
    entity_id   BINARY(16)   NOT NULL,
    action      VARCHAR(64)  NOT NULL,
    actor       VARCHAR(64)  NOT NULL,
    detail      TEXT         NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
