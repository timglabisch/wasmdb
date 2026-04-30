import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateProduct' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', sku: '', name: '', description: '',
  unit: 'Stk', unit_price: 0, tax_rate: 1900, cost_price: 0,
  active: 1,
  activity_id: '', timestamp: '',
};

export function createProduct(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return {
    type: 'CreateProduct',
    ...DEFAULTS,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
    ...args,
  };
}
