import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'MarkPaid' }>;

/** Build a MarkPaid intent command. The activity row is emitted by the
 * command itself (see `commands/invoice/mark_paid.rs`) — caller does not
 * compose any LogActivity. */
export function markPaid(invoiceId: string): InvoiceCommand {
  const cmd: Variant = {
    type: 'MarkPaid',
    id: invoiceId,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
