import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdatePosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '',
  description: '', quantity: 0, unit_price: 0, tax_rate: 1900,
  product_id: '', item_number: '', unit: 'Stk',
  discount_pct: 0, cost_price: 0, position_type: 'service',
};

export function updatePosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdatePosition', ...DEFAULTS, ...args };
}
