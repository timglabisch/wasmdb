import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateRecurring' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', customer_id: '', template_name: '',
  interval_unit: 'month', interval_value: 1, next_run: '',
  status_template: 'draft', notes_template: '',
  activity_id: '', timestamp: '',
};

export function createRecurring(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return {
    type: 'CreateRecurring',
    ...DEFAULTS,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
    ...args,
  };
}
