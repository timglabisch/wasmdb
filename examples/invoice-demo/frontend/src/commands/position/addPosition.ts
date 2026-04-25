import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'AddPosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', invoice_id: '', position_nr: 0,
  description: '', quantity: 1000, unit_price: 0, tax_rate: 1900,
  product_id: '', item_number: '', unit: 'Stk',
  discount_pct: 0, cost_price: 0, position_type: 'service',
};

export function addPosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'AddPosition', ...DEFAULTS, ...args };
}
