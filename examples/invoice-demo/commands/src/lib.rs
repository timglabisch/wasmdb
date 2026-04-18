use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

mod helpers;
pub mod customer;
pub mod contact;
pub mod invoice;
pub mod position;
pub mod payment;
pub mod product;
pub mod sepa;
pub mod recurring;
pub mod activity;

/// Wire-format enum. Variants map 1:1 to per-command executor modules.
/// The #[serde(tag = "type")] discriminator + Borsh derives + `#[ts(export)]`
/// generate the TS bindings consumed by the frontend, so variant layout is
/// API-stable — do not split this enum across multiple types.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum InvoiceCommand {
    // ── Customer ────────────────────────────────────────────────────────────
    CreateCustomer {
        id: i64,
        name: String,
        email: String,
        created_at: String,
        company_type: String,
        tax_id: String,
        vat_id: String,
        payment_terms_days: i64,
        default_discount_pct: i64,
        billing_street: String,
        billing_zip: String,
        billing_city: String,
        billing_country: String,
        shipping_street: String,
        shipping_zip: String,
        shipping_city: String,
        shipping_country: String,
        default_iban: String,
        default_bic: String,
        notes: String,
    },
    UpdateCustomer {
        id: i64,
        name: String,
        email: String,
        company_type: String,
        tax_id: String,
        vat_id: String,
        payment_terms_days: i64,
        default_discount_pct: i64,
        billing_street: String,
        billing_zip: String,
        billing_city: String,
        billing_country: String,
        shipping_street: String,
        shipping_zip: String,
        shipping_city: String,
        shipping_country: String,
        default_iban: String,
        default_bic: String,
        notes: String,
    },
    DeleteCustomer { id: i64 },
    /// Cascades through payments, recurring_positions, recurring_invoices,
    /// sepa_mandates, positions, invoices, contacts, customer — all atomic.
    DeleteCustomerCascade { id: i64 },

    // ── Contact ─────────────────────────────────────────────────────────────
    CreateContact {
        id: i64, customer_id: i64,
        name: String, email: String, phone: String, role: String,
        is_primary: i64,
    },
    UpdateContact {
        id: i64,
        name: String, email: String, phone: String, role: String,
        is_primary: i64,
    },
    DeleteContact { id: i64 },

    // ── Invoice ─────────────────────────────────────────────────────────────
    CreateInvoice {
        id: i64,
        customer_id: i64,
        number: String,
        status: String,
        date_issued: String,
        date_due: String,
        notes: String,
        doc_type: String,
        parent_id: i64,
        service_date: String,
        cash_allowance_pct: i64,
        cash_allowance_days: i64,
        discount_pct: i64,
        payment_method: String,
        sepa_mandate_id: i64,
        currency: String,
        language: String,
        project_ref: String,
        external_id: String,
        billing_street: String,
        billing_zip: String,
        billing_city: String,
        billing_country: String,
        shipping_street: String,
        shipping_zip: String,
        shipping_city: String,
        shipping_country: String,
    },
    UpdateInvoiceHeader {
        id: i64,
        number: String,
        status: String,
        date_issued: String,
        date_due: String,
        notes: String,
        doc_type: String,
        parent_id: i64,
        service_date: String,
        cash_allowance_pct: i64,
        cash_allowance_days: i64,
        discount_pct: i64,
        payment_method: String,
        sepa_mandate_id: i64,
        currency: String,
        language: String,
        project_ref: String,
        external_id: String,
        billing_street: String,
        billing_zip: String,
        billing_city: String,
        billing_country: String,
        shipping_street: String,
        shipping_zip: String,
        shipping_city: String,
        shipping_country: String,
    },
    /// Cascades positions + payments + invoice.
    DeleteInvoice { id: i64 },

    // ── Position ────────────────────────────────────────────────────────────
    AddPosition {
        id: i64, invoice_id: i64, position_nr: i64,
        description: String, quantity: i64, unit_price: i64, tax_rate: i64,
        product_id: i64, item_number: String, unit: String,
        discount_pct: i64, cost_price: i64, position_type: String,
    },
    UpdatePosition {
        id: i64,
        description: String, quantity: i64, unit_price: i64, tax_rate: i64,
        product_id: i64, item_number: String, unit: String,
        discount_pct: i64, cost_price: i64, position_type: String,
    },
    DeletePosition { id: i64 },
    MovePosition { id: i64, new_position_nr: i64 },

    // ── Payment ─────────────────────────────────────────────────────────────
    CreatePayment {
        id: i64, invoice_id: i64, amount: i64, paid_at: String,
        method: String, reference: String, note: String,
    },
    UpdatePayment {
        id: i64, amount: i64, paid_at: String,
        method: String, reference: String, note: String,
    },
    DeletePayment { id: i64 },

    // ── Product ─────────────────────────────────────────────────────────────
    CreateProduct {
        id: i64, sku: String, name: String, description: String,
        unit: String, unit_price: i64, tax_rate: i64, cost_price: i64,
        active: i64,
    },
    UpdateProduct {
        id: i64, sku: String, name: String, description: String,
        unit: String, unit_price: i64, tax_rate: i64, cost_price: i64,
        active: i64,
    },
    DeleteProduct { id: i64 },

    // ── SEPA Mandate ────────────────────────────────────────────────────────
    CreateSepaMandate {
        id: i64, customer_id: i64, mandate_ref: String,
        iban: String, bic: String, holder_name: String,
        signed_at: String,
    },
    UpdateSepaMandate {
        id: i64, mandate_ref: String,
        iban: String, bic: String, holder_name: String,
        signed_at: String, status: String,
    },
    DeleteSepaMandate { id: i64 },

    // ── Recurring Invoice ───────────────────────────────────────────────────
    CreateRecurring {
        id: i64, customer_id: i64, template_name: String,
        interval_unit: String, interval_value: i64,
        next_run: String,
        status_template: String, notes_template: String,
    },
    UpdateRecurring {
        id: i64, template_name: String,
        interval_unit: String, interval_value: i64,
        next_run: String, enabled: i64,
        status_template: String, notes_template: String,
    },
    /// Cascades recurring_positions + recurring_invoice.
    DeleteRecurring { id: i64 },
    AddRecurringPosition {
        id: i64, recurring_id: i64, position_nr: i64,
        description: String, quantity: i64, unit_price: i64, tax_rate: i64,
        unit: String, item_number: String, discount_pct: i64,
    },
    UpdateRecurringPosition {
        id: i64,
        description: String, quantity: i64, unit_price: i64, tax_rate: i64,
        unit: String, item_number: String, discount_pct: i64,
    },
    DeleteRecurringPosition { id: i64 },
    /// Creates a new invoice with positions copied from the recurring template.
    /// `position_ids` must have as many entries as the template has positions.
    /// Updates last_run + next_run on the recurring row.
    RunRecurringOnce {
        recurring_id: i64,
        new_invoice_id: i64,
        position_ids: Vec<i64>,
        new_number: String,
        issue_date: String,
        due_date: String,
        new_next_run: String,
    },

    // ── Activity Log ────────────────────────────────────────────────────────
    LogActivity {
        id: i64, timestamp: String,
        entity_type: String, entity_id: i64,
        action: String, actor: String, detail: String,
    },
}

impl Command for InvoiceCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        use InvoiceCommand::*;
        match self {
            // ── Customer ────────────────────────────────────────────────────
            CreateCustomer {
                id, name, email, created_at,
                company_type, tax_id, vat_id,
                payment_terms_days, default_discount_pct,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
                default_iban, default_bic, notes,
            } => customer::create::run(
                db, *id, name, email, created_at,
                company_type, tax_id, vat_id,
                *payment_terms_days, *default_discount_pct,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
                default_iban, default_bic, notes,
            ),
            UpdateCustomer {
                id, name, email,
                company_type, tax_id, vat_id,
                payment_terms_days, default_discount_pct,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
                default_iban, default_bic, notes,
            } => customer::update::run(
                db, *id, name, email,
                company_type, tax_id, vat_id,
                *payment_terms_days, *default_discount_pct,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
                default_iban, default_bic, notes,
            ),
            DeleteCustomer { id } => customer::delete::run(db, *id),
            DeleteCustomerCascade { id } => customer::delete_cascade::run(db, *id),

            // ── Contact ─────────────────────────────────────────────────────
            CreateContact { id, customer_id, name, email, phone, role, is_primary } =>
                contact::create::run(db, *id, *customer_id, name, email, phone, role, *is_primary),
            UpdateContact { id, name, email, phone, role, is_primary } =>
                contact::update::run(db, *id, name, email, phone, role, *is_primary),
            DeleteContact { id } => contact::delete::run(db, *id),

            // ── Invoice ─────────────────────────────────────────────────────
            CreateInvoice {
                id, customer_id, number, status, date_issued, date_due, notes,
                doc_type, parent_id, service_date,
                cash_allowance_pct, cash_allowance_days, discount_pct,
                payment_method, sepa_mandate_id, currency, language,
                project_ref, external_id,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
            } => invoice::create::run(
                db, *id, *customer_id, number, status, date_issued, date_due, notes,
                doc_type, *parent_id, service_date,
                *cash_allowance_pct, *cash_allowance_days, *discount_pct,
                payment_method, *sepa_mandate_id, currency, language,
                project_ref, external_id,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
            ),
            UpdateInvoiceHeader {
                id, number, status, date_issued, date_due, notes,
                doc_type, parent_id, service_date,
                cash_allowance_pct, cash_allowance_days, discount_pct,
                payment_method, sepa_mandate_id, currency, language,
                project_ref, external_id,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
            } => invoice::update_header::run(
                db, *id, number, status, date_issued, date_due, notes,
                doc_type, *parent_id, service_date,
                *cash_allowance_pct, *cash_allowance_days, *discount_pct,
                payment_method, *sepa_mandate_id, currency, language,
                project_ref, external_id,
                billing_street, billing_zip, billing_city, billing_country,
                shipping_street, shipping_zip, shipping_city, shipping_country,
            ),
            DeleteInvoice { id } => invoice::delete::run(db, *id),

            // ── Position ────────────────────────────────────────────────────
            AddPosition {
                id, invoice_id, position_nr,
                description, quantity, unit_price, tax_rate,
                product_id, item_number, unit, discount_pct, cost_price, position_type,
            } => position::add::run(
                db, *id, *invoice_id, *position_nr,
                description, *quantity, *unit_price, *tax_rate,
                *product_id, item_number, unit, *discount_pct, *cost_price, position_type,
            ),
            UpdatePosition {
                id, description, quantity, unit_price, tax_rate,
                product_id, item_number, unit, discount_pct, cost_price, position_type,
            } => position::update::run(
                db, *id, description, *quantity, *unit_price, *tax_rate,
                *product_id, item_number, unit, *discount_pct, *cost_price, position_type,
            ),
            DeletePosition { id } => position::delete::run(db, *id),
            MovePosition { id, new_position_nr } =>
                position::move_nr::run(db, *id, *new_position_nr),

            // ── Payment ─────────────────────────────────────────────────────
            CreatePayment { id, invoice_id, amount, paid_at, method, reference, note } =>
                payment::create::run(db, *id, *invoice_id, *amount, paid_at, method, reference, note),
            UpdatePayment { id, amount, paid_at, method, reference, note } =>
                payment::update::run(db, *id, *amount, paid_at, method, reference, note),
            DeletePayment { id } => payment::delete::run(db, *id),

            // ── Product ─────────────────────────────────────────────────────
            CreateProduct {
                id, sku, name, description, unit, unit_price, tax_rate, cost_price, active,
            } => product::create::run(
                db, *id, sku, name, description, unit, *unit_price, *tax_rate, *cost_price, *active,
            ),
            UpdateProduct {
                id, sku, name, description, unit, unit_price, tax_rate, cost_price, active,
            } => product::update::run(
                db, *id, sku, name, description, unit, *unit_price, *tax_rate, *cost_price, *active,
            ),
            DeleteProduct { id } => product::delete::run(db, *id),

            // ── SEPA Mandate ────────────────────────────────────────────────
            CreateSepaMandate {
                id, customer_id, mandate_ref, iban, bic, holder_name, signed_at,
            } => sepa::create::run(
                db, *id, *customer_id, mandate_ref, iban, bic, holder_name, signed_at,
            ),
            UpdateSepaMandate {
                id, mandate_ref, iban, bic, holder_name, signed_at, status,
            } => sepa::update::run(
                db, *id, mandate_ref, iban, bic, holder_name, signed_at, status,
            ),
            DeleteSepaMandate { id } => sepa::delete::run(db, *id),

            // ── Recurring ───────────────────────────────────────────────────
            CreateRecurring {
                id, customer_id, template_name,
                interval_unit, interval_value, next_run,
                status_template, notes_template,
            } => recurring::create::run(
                db, *id, *customer_id, template_name,
                interval_unit, *interval_value, next_run,
                status_template, notes_template,
            ),
            UpdateRecurring {
                id, template_name,
                interval_unit, interval_value, next_run, enabled,
                status_template, notes_template,
            } => recurring::update::run(
                db, *id, template_name,
                interval_unit, *interval_value, next_run, *enabled,
                status_template, notes_template,
            ),
            DeleteRecurring { id } => recurring::delete::run(db, *id),
            AddRecurringPosition {
                id, recurring_id, position_nr,
                description, quantity, unit_price, tax_rate,
                unit, item_number, discount_pct,
            } => recurring::add_position::run(
                db, *id, *recurring_id, *position_nr,
                description, *quantity, *unit_price, *tax_rate,
                unit, item_number, *discount_pct,
            ),
            UpdateRecurringPosition {
                id, description, quantity, unit_price, tax_rate,
                unit, item_number, discount_pct,
            } => recurring::update_position::run(
                db, *id, description, *quantity, *unit_price, *tax_rate,
                unit, item_number, *discount_pct,
            ),
            DeleteRecurringPosition { id } => recurring::delete_position::run(db, *id),
            RunRecurringOnce {
                recurring_id, new_invoice_id, position_ids,
                new_number, issue_date, due_date, new_next_run,
            } => recurring::run_once::run(
                db, *recurring_id, *new_invoice_id, position_ids,
                new_number, issue_date, due_date, new_next_run,
            ),

            // ── Activity ────────────────────────────────────────────────────
            LogActivity {
                id, timestamp, entity_type, entity_id, action, actor, detail,
            } => activity::log::run(
                db, *id, timestamp, entity_type, *entity_id, action, actor, detail,
            ),
        }
    }
}
