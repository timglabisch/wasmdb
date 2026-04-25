import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateRecurring' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', template_name: '',
  interval_unit: 'month', interval_value: 1, next_run: '', enabled: 1,
  status_template: 'draft', notes_template: '',
};

export function updateRecurring(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateRecurring', ...DEFAULTS, ...args };
}
