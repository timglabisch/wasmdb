import { createStream, flushStream } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { toast } from '@/components/ui/sonner';
import { markPaid as markPaidCmd } from 'invoice-demo-generated/InvoiceCommandFactories';

/**
 * Set an invoice's status to `paid`.
 *
 * The optimistic apply (status update + activity-log row) happens
 * synchronously inside the `MarkPaid` intent command — the local store
 * reflects both immediately. The server roundtrip (`flushStream`) is
 * fire-and-forget so slow links (3G, etc.) don't stall the click handler.
 * If the server rejects, the optimistic state rolls back and we surface
 * the reason via toast.
 */
export function markPaid(invoiceId: string): void {
  const stream = createStream(2);
  executeOnStream(stream, markPaidCmd({ id: invoiceId }));
  flushStream(stream).catch((err: unknown) => {
    toast.error(`Statuswechsel abgelehnt: ${(err as Error).message}`);
  });
}
