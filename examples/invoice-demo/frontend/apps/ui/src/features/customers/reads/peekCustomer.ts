import { peekQuery } from '@wasmdb/client';
import type { CustomerWithoutPk } from 'invoice-demo-generated/tables/Customer';
import type { ContactWithoutPk } from 'invoice-demo-generated/tables/Contact';
import type { SepaMandateWithoutPk } from 'invoice-demo-generated/tables/SepaMandate';

const rowToCustomer = (r: any[]): CustomerWithoutPk => ({
  name: r[0] as string, email: r[1] as string,
  created_at: r[2] as string,
  company_type: r[3] as string, tax_id: r[4] as string, vat_id: r[5] as string,
  payment_terms_days: r[6] as number, default_discount_pct: r[7] as number,
  billing_street: r[8] as string, billing_zip: r[9] as string,
  billing_city: r[10] as string, billing_country: r[11] as string,
  shipping_street: r[12] as string, shipping_zip: r[13] as string,
  shipping_city: r[14] as string, shipping_country: r[15] as string,
  default_iban: r[16] as string, default_bic: r[17] as string, notes: r[18] as string,
});

export function peekCustomer(id: string): CustomerWithoutPk | null {
  const rows = peekQuery(
    `SELECT customers.name, customers.email, customers.created_at, ` +
    `customers.company_type, customers.tax_id, customers.vat_id, ` +
    `customers.payment_terms_days, customers.default_discount_pct, ` +
    `customers.billing_street, customers.billing_zip, customers.billing_city, customers.billing_country, ` +
    `customers.shipping_street, customers.shipping_zip, customers.shipping_city, customers.shipping_country, ` +
    `customers.default_iban, customers.default_bic, customers.notes ` +
    `FROM customers WHERE customers.id = :id`,
    { id },
  );
  if (rows.length === 0) return null;
  return rowToCustomer(rows[0]);
}

const rowToContact = (r: any[]): ContactWithoutPk => ({
  customer_id: r[0] as string,
  name: r[1] as string, email: r[2] as string, phone: r[3] as string,
  role: r[4] as string, is_primary: r[5] as number,
});

export function peekContact(id: string): ContactWithoutPk | null {
  const rows = peekQuery(
    `SELECT contacts.customer_id, contacts.name, contacts.email, contacts.phone, contacts.role, contacts.is_primary ` +
    `FROM contacts WHERE contacts.id = :id`,
    { id },
  );
  if (rows.length === 0) return null;
  return rowToContact(rows[0]);
}

const rowToSepa = (r: any[]): SepaMandateWithoutPk => ({
  customer_id: r[0] as string,
  mandate_ref: r[1] as string, iban: r[2] as string, bic: r[3] as string,
  holder_name: r[4] as string, signed_at: r[5] as string, status: r[6] as string,
});

export function peekSepaMandate(id: string): SepaMandateWithoutPk | null {
  const rows = peekQuery(
    `SELECT sepa_mandates.customer_id, sepa_mandates.mandate_ref, sepa_mandates.iban, sepa_mandates.bic, ` +
    `sepa_mandates.holder_name, sepa_mandates.signed_at, sepa_mandates.status ` +
    `FROM sepa_mandates WHERE sepa_mandates.id = :id`,
    { id },
  );
  if (rows.length === 0) return null;
  return rowToSepa(rows[0]);
}
