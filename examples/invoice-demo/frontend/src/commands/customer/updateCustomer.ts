import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateCustomer' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', name: '', email: '',
  company_type: 'company', tax_id: '', vat_id: '',
  payment_terms_days: 14, default_discount_pct: 0,
  billing_street: '', billing_zip: '', billing_city: '', billing_country: 'DE',
  shipping_street: '', shipping_zip: '', shipping_city: '', shipping_country: 'DE',
  default_iban: '', default_bic: '', notes: '',
};

export function updateCustomer(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateCustomer', ...DEFAULTS, ...args };
}
