import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import * as createCreditNoteCmd from '../../../commands/invoice/createCreditNote.ts';

/**
 * Create a credit note for an existing invoice: fresh draft with negated
 * quantities + parent_id link back to the source invoice.
 */
export async function createCreditNote(invoiceId: string): Promise<string | null> {
  const result = createCreditNoteCmd.createCreditNote(invoiceId);
  if (!result) return null;
  if (!confirm(`Gutschrift zu "${result.srcNumber}" erzeugen?`)) return null;
  const stream = createStream(1);
  executeOnStream(stream, result.cmd);
  await flushStream(stream).catch(() => {});
  return result.newInvoiceId;
}
