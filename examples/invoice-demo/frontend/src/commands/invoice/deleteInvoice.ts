import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteInvoice' }>;

/** Cascading delete — drops the invoice plus its positions and payments.
 * The activity_log row is emitted by the command itself; callers do not
 * need a separate logActivity call. */
export function deleteInvoice(args: { id: string; number: string }): InvoiceCommand {
  const cmd: Variant = {
    type: 'DeleteInvoice',
    id: args.id,
    number: args.number,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
