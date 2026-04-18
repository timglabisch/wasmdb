import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateRecurring' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: 0, customer_id: 0, template_name: '',
  interval_unit: 'month', interval_value: 1, next_run: '',
  status_template: 'draft', notes_template: '',
};

export function createRecurring(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreateRecurring', ...DEFAULTS, ...args };
}
