import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import { toast } from '@/components/ui/sonner';
import { convertOfferToInvoice as convertCmd } from '../../../commands/invoice/convertOfferToInvoice.ts';

/**
 * Convert an offer into a fresh draft invoice.
 *
 * Sets `doc_type = 'invoice'` and `status = 'draft'`, and emits an
 * `offer_converted` activity-log row — all inside the `ConvertOfferToInvoice`
 * intent command. Callers no longer compose separate writes.
 */
export function convertOfferToInvoice(invoiceId: string): void {
  const stream = createStream(2);
  executeOnStream(stream, convertCmd(invoiceId));
  flushStream(stream).catch((err: unknown) => {
    toast.error(`Umwandlung fehlgeschlagen: ${(err as Error).message}`);
  });
}
