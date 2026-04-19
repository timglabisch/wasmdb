-- Invoice-demo initial schema.
-- Apply with:
--   mysql -h 127.0.0.1 -P 4000 -u root < examples/invoice-demo/sql/001_init.sql

CREATE DATABASE IF NOT EXISTS invoice_demo;
USE invoice_demo;

CREATE TABLE IF NOT EXISTS customers (
    owner_id BIGINT      NOT NULL,
    id       BIGINT      NOT NULL,
    name     VARCHAR(255) NOT NULL,
    PRIMARY KEY (owner_id, id)
);

-- Seed rows for local poking.
INSERT IGNORE INTO customers (owner_id, id, name) VALUES
    (1, 101, 'Acme GmbH'),
    (1, 102, 'Globex AG'),
    (2, 201, 'Initech Ltd');
