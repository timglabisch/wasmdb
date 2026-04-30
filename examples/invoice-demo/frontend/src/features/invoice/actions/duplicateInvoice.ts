import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import * as duplicateInvoiceCmd from '../../../commands/invoice/duplicateInvoice.ts';

/**
 * Duplicate an invoice: copy header + all positions into a new draft.
 * Returns the new id, or null if the source doesn't exist.
 */
export async function duplicateInvoice(invoiceId: string): Promise<string | null> {
  const result = duplicateInvoiceCmd.duplicateInvoice(invoiceId);
  if (!result) return null;
  const stream = createStream(1);
  executeOnStream(stream, result.cmd);
  await flushStream(stream).catch(() => {});
  return result.newInvoiceId;
}
