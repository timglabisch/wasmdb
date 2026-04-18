import { peekQuery } from '../../../wasm.ts';
import { selectById } from '../../../queries.ts';
import type { InvoiceRow } from '../types.ts';

const COLS =
  'number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, ' +
  'cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, ' +
  'currency, language, project_ref, external_id, ' +
  'billing_street, billing_zip, billing_city, billing_country, ' +
  'shipping_street, shipping_zip, shipping_city, shipping_country, customer_id';

const rowToInvoice = (r: any[]): InvoiceRow => ({
  number: r[0] as string, status: r[1] as string,
  date_issued: r[2] as string, date_due: r[3] as string, notes: r[4] as string,
  doc_type: r[5] as string, parent_id: r[6] as number, service_date: r[7] as string,
  cash_allowance_pct: r[8] as number, cash_allowance_days: r[9] as number, discount_pct: r[10] as number,
  payment_method: r[11] as string, sepa_mandate_id: r[12] as number,
  currency: r[13] as string, language: r[14] as string,
  project_ref: r[15] as string, external_id: r[16] as string,
  billing_street: r[17] as string, billing_zip: r[18] as string, billing_city: r[19] as string, billing_country: r[20] as string,
  shipping_street: r[21] as string, shipping_zip: r[22] as string, shipping_city: r[23] as string, shipping_country: r[24] as string,
  customer_id: r[25] as number,
});

/** One-shot non-reactive full-row read. Used at write time to compose UpdateInvoiceHeader payloads. */
export function peekInvoice(invoiceId: number): InvoiceRow | null {
  const rows = peekQuery(selectById('invoices', COLS, invoiceId));
  if (rows.length === 0) return null;
  return rowToInvoice(rows[0]);
}
