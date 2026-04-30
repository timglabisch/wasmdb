import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteRecurring' }>;

/** Cascading delete of recurring invoice + positions. The activity_log row is
 * emitted by the command itself; callers do not need a separate logActivity call. */
export function deleteRecurring(args: { id: string; label_for_detail: string }): InvoiceCommand {
  const cmd: Variant = {
    type: 'DeleteRecurring',
    id: args.id,
    label_for_detail: args.label_for_detail,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
