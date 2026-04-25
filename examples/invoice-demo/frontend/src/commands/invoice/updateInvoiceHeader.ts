import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateInvoiceHeader' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', number: '', status: 'draft',
  date_issued: '', date_due: '', notes: '',
  doc_type: 'invoice', parent_id: '', service_date: '',
  cash_allowance_pct: 0, cash_allowance_days: 0, discount_pct: 0,
  payment_method: 'transfer', sepa_mandate_id: '',
  currency: 'EUR', language: 'de',
  project_ref: '', external_id: '',
  billing_street: '', billing_zip: '', billing_city: '', billing_country: 'DE',
  shipping_street: '', shipping_zip: '', shipping_city: '', shipping_country: 'DE',
};

export function updateInvoiceHeader(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateInvoiceHeader', ...DEFAULTS, ...args };
}
