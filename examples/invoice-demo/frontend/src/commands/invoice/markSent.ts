import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'MarkSent' }>;

/** Build a MarkSent intent command. The activity row is emitted by the
 * command itself (see `commands/invoice/mark_sent.rs`) — caller does not
 * compose any LogActivity. */
export function markSent(invoiceId: string): InvoiceCommand {
  const cmd: Variant = {
    type: 'MarkSent',
    id: invoiceId,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
