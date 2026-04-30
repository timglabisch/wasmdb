import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import { toast } from '@/components/ui/sonner';
import { markSent as markSentCmd } from '../../../commands/invoice/markSent.ts';

/**
 * Set an invoice's status to `sent`.
 *
 * The optimistic apply (status update + activity-log row) happens
 * synchronously inside the `MarkSent` intent command — the local store
 * reflects both immediately. The server roundtrip (`flushStream`) is
 * fire-and-forget so slow links (3G, etc.) don't stall the click handler.
 * If the server rejects, the optimistic state rolls back and we surface
 * the reason via toast.
 */
export function markSent(invoiceId: string): void {
  const stream = createStream(2);
  executeOnStream(stream, markSentCmd(invoiceId));
  flushStream(stream).catch((err: unknown) => {
    toast.error(`Statuswechsel abgelehnt: ${(err as Error).message}`);
  });
}
