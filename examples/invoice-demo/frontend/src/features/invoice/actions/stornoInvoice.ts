import { executeOnStream, createStream, flushStream, nextId } from '../../../wasm.ts';
import { storno as stornoCmd } from '../../../commands/invoice/storno.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { peekPositions } from '../reads/peekPositions.ts';
import { isoDate } from './isoDate.ts';

/**
 * Storno: mark the invoice cancelled AND emit a mirror credit note in the same
 * atomic stream. Keeps the audit trail clean for accounting.
 *
 * The optimistic apply (status update + credit note + positions + activity row)
 * happens synchronously inside the `Storno` intent command. Callers no longer
 * compose separate writes.
 */
export async function stornoInvoice(invoiceId: string): Promise<string | null> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return null;
  if (!confirm(`"${inv.number}" stornieren (status=cancelled) und Gutschrift erzeugen?`)) return null;
  const positions = peekPositions(invoiceId);
  const creditNoteId = nextId();
  const stream = createStream(4);
  executeOnStream(stream, stornoCmd({
    invoiceId,
    invoice: inv,
    creditNoteId,
    creditNoteNumber: `CN-${inv.number}`,
    dateIssued: isoDate(0),
    dateDue: isoDate(14),
    positions,
  }));
  await flushStream(stream);
  return creditNoteId;
}
