import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateProduct' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', sku: '', name: '', description: '',
  unit: 'Stk', unit_price: 0, tax_rate: 1900, cost_price: 0,
  active: 1,
};

export function updateProduct(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateProduct', ...DEFAULTS, ...args };
}
