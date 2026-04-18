import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateInvoice' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: 0, customer_id: 0, number: '', status: 'draft',
  date_issued: '', date_due: '', notes: '',
  doc_type: 'invoice', parent_id: 0, service_date: '',
  cash_allowance_pct: 0, cash_allowance_days: 0, discount_pct: 0,
  payment_method: 'transfer', sepa_mandate_id: 0,
  currency: 'EUR', language: 'de',
  project_ref: '', external_id: '',
  billing_street: '', billing_zip: '', billing_city: '', billing_country: 'DE',
  shipping_street: '', shipping_zip: '', shipping_city: '', shipping_country: 'DE',
};

export function createInvoice(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreateInvoice', ...DEFAULTS, ...args };
}
