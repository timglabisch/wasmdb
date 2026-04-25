import { peekQuery } from '@/wasm';
import type { CustomerRow, ContactRow, SepaMandateRow } from '../types';

const rowToCustomer = (r: any[]): CustomerRow => ({
  name: r[0] as string, email: r[1] as string,
  company_type: r[2] as string, tax_id: r[3] as string, vat_id: r[4] as string,
  payment_terms_days: r[5] as number, default_discount_pct: r[6] as number,
  billing_street: r[7] as string, billing_zip: r[8] as string,
  billing_city: r[9] as string, billing_country: r[10] as string,
  shipping_street: r[11] as string, shipping_zip: r[12] as string,
  shipping_city: r[13] as string, shipping_country: r[14] as string,
  default_iban: r[15] as string, default_bic: r[16] as string, notes: r[17] as string,
});

export function peekCustomer(id: string): CustomerRow | null {
  const rows = peekQuery(
    `SELECT customers.name, customers.email, customers.company_type, customers.tax_id, customers.vat_id, ` +
    `customers.payment_terms_days, customers.default_discount_pct, ` +
    `customers.billing_street, customers.billing_zip, customers.billing_city, customers.billing_country, ` +
    `customers.shipping_street, customers.shipping_zip, customers.shipping_city, customers.shipping_country, ` +
    `customers.default_iban, customers.default_bic, customers.notes ` +
    `FROM customers WHERE customers.id = UUID '${id}'`,
  );
  if (rows.length === 0) return null;
  return rowToCustomer(rows[0]);
}

const rowToContact = (r: any[]): ContactRow => ({
  name: r[0] as string, email: r[1] as string, phone: r[2] as string,
  role: r[3] as string, is_primary: r[4] as number,
});

export function peekContact(id: string): ContactRow | null {
  const rows = peekQuery(
    `SELECT contacts.name, contacts.email, contacts.phone, contacts.role, contacts.is_primary ` +
    `FROM contacts WHERE contacts.id = UUID '${id}'`,
  );
  if (rows.length === 0) return null;
  return rowToContact(rows[0]);
}

const rowToSepa = (r: any[]): SepaMandateRow => ({
  mandate_ref: r[0] as string, iban: r[1] as string, bic: r[2] as string,
  holder_name: r[3] as string, signed_at: r[4] as string, status: r[5] as string,
});

export function peekSepaMandate(id: string): SepaMandateRow | null {
  const rows = peekQuery(
    `SELECT sepa_mandates.mandate_ref, sepa_mandates.iban, sepa_mandates.bic, ` +
    `sepa_mandates.holder_name, sepa_mandates.signed_at, sepa_mandates.status ` +
    `FROM sepa_mandates WHERE sepa_mandates.id = UUID '${id}'`,
  );
  if (rows.length === 0) return null;
  return rowToSepa(rows[0]);
}
