/**
 * Full row shape for an `invoices` row, as consumed by UpdateInvoiceHeader
 * plus the display columns.
 *
 * Kept as a plain interface (not a generated type) because the write-path
 * command already enumerates these fields — this interface is the caller-side
 * projection that `peekInvoice()` returns and hooks like `usePatchInvoice()`
 * accept as Partial.
 */
export interface InvoiceRow {
  number: string; status: string;
  date_issued: string; date_due: string; notes: string;
  doc_type: string; parent_id: string; service_date: string;
  cash_allowance_pct: number; cash_allowance_days: number; discount_pct: number;
  payment_method: string; sepa_mandate_id: string;
  currency: string; language: string;
  project_ref: string; external_id: string;
  billing_street: string; billing_zip: string; billing_city: string; billing_country: string;
  shipping_street: string; shipping_zip: string; shipping_city: string; shipping_country: string;
  customer_id: string;
}
