import { peekQuery } from '@/wasm';
import { selectById } from '@/queries';
import type { CustomerRow, ContactRow, SepaMandateRow } from '../types';

const CUSTOMER_COLS =
  'name, email, company_type, tax_id, vat_id, payment_terms_days, default_discount_pct, ' +
  'billing_street, billing_zip, billing_city, billing_country, ' +
  'shipping_street, shipping_zip, shipping_city, shipping_country, ' +
  'default_iban, default_bic, notes';

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

export function peekCustomer(id: number): CustomerRow | null {
  const rows = peekQuery(selectById('customers', CUSTOMER_COLS, id));
  if (rows.length === 0) return null;
  return rowToCustomer(rows[0]);
}

const CONTACT_COLS = 'name, email, phone, role, is_primary';

const rowToContact = (r: any[]): ContactRow => ({
  name: r[0] as string, email: r[1] as string, phone: r[2] as string,
  role: r[3] as string, is_primary: r[4] as number,
});

export function peekContact(id: number): ContactRow | null {
  const rows = peekQuery(selectById('contacts', CONTACT_COLS, id));
  if (rows.length === 0) return null;
  return rowToContact(rows[0]);
}

const SEPA_COLS = 'mandate_ref, iban, bic, holder_name, signed_at, status';

const rowToSepa = (r: any[]): SepaMandateRow => ({
  mandate_ref: r[0] as string, iban: r[1] as string, bic: r[2] as string,
  holder_name: r[3] as string, signed_at: r[4] as string, status: r[5] as string,
});

export function peekSepaMandate(id: number): SepaMandateRow | null {
  const rows = peekQuery(selectById('sepa_mandates', SEPA_COLS, id));
  if (rows.length === 0) return null;
  return rowToSepa(rows[0]);
}
