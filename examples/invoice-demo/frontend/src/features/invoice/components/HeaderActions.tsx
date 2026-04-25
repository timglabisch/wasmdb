import { memo, useCallback } from 'react';
import { useNavigate } from '@tanstack/react-router';
import {
  Send, CheckCircle2, ArrowRightCircle, Copy, FileMinus2, Ban, Trash2, MoreHorizontal,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/sonner';
import { useQuery } from '@/wasm';
import { markSent } from '@/features/invoice/actions/markSent';
import { markPaid } from '@/features/invoice/actions/markPaid';
import { convertOfferToInvoice } from '@/features/invoice/actions/convertOfferToInvoice';
import { duplicateInvoice } from '@/features/invoice/actions/duplicateInvoice';
import { createCreditNote } from '@/features/invoice/actions/createCreditNote';
import { stornoInvoice } from '@/features/invoice/actions/stornoInvoice';
import { deleteInvoiceWithConfirm } from '@/features/invoice/actions/deleteInvoiceWithConfirm';

interface HeaderBits {
  status: string;
  doc_type: string;
  number: string;
}

/**
 * Minimal subscription for the header's action bar:
 * only the columns that change availability of actions.
 */
export const HeaderActions = memo(function HeaderActions({ invoiceId }: { invoiceId: string }) {
  const navigate = useNavigate();
  const rows = useQuery<HeaderBits>(
    `SELECT invoices.status, invoices.doc_type, invoices.number ` +
    `FROM invoices WHERE invoices.id = UUID '${invoiceId}'`,
    ([status, doc_type, number]) => ({
      status: status as string,
      doc_type: doc_type as string,
      number: number as string,
    }),
  );
  const h = rows[0];

  const onSent = useCallback(async () => {
    await markSent(invoiceId);
    toast.success('Als versendet markiert');
  }, [invoiceId]);

  const onPaid = useCallback(async () => {
    await markPaid(invoiceId);
    toast.success('Als bezahlt markiert');
  }, [invoiceId]);

  const onConvert = useCallback(() => {
    convertOfferToInvoice(invoiceId);
    toast.success('Angebot in Rechnung umgewandelt');
  }, [invoiceId]);

  const onDuplicate = useCallback(async () => {
    const newId = await duplicateInvoice(invoiceId);
    if (newId !== null) {
      toast.success('Dupliziert');
      navigate({ to: '/invoices/$invoiceId', params: { invoiceId: newId } });
    }
  }, [invoiceId, navigate]);

  const onCredit = useCallback(async () => {
    const newId = await createCreditNote(invoiceId);
    if (newId !== null) {
      toast.success('Gutschrift erstellt');
      navigate({ to: '/invoices/$invoiceId', params: { invoiceId: newId } });
    }
  }, [invoiceId, navigate]);

  const onStorno = useCallback(async () => {
    const newId = await stornoInvoice(invoiceId);
    if (newId !== null) {
      toast.success('Storniert');
    }
  }, [invoiceId]);

  const onDelete = useCallback(async () => {
    if (!h) return;
    const ok = await deleteInvoiceWithConfirm(invoiceId, h.number);
    if (ok) {
      toast.success('Gelöscht');
      navigate({ to: '/invoices' });
    }
  }, [invoiceId, h, navigate]);

  if (!h) return null;

  const isOffer = h.doc_type === 'offer';
  const canMarkSent = h.status === 'draft';
  const canMarkPaid = h.status === 'sent' || h.status === 'draft';
  const canStorno = h.status !== 'cancelled' && (h.doc_type === 'invoice' || h.doc_type === 'proforma');

  return (
    <div className="flex items-center gap-2">
      {canMarkSent && (
        <Button variant="secondary" size="sm" onClick={onSent}>
          <Send className="h-4 w-4" /> Als versendet
        </Button>
      )}
      {canMarkPaid && (
        <Button variant="default" size="sm" onClick={onPaid}>
          <CheckCircle2 className="h-4 w-4" /> Als bezahlt
        </Button>
      )}
      {isOffer && (
        <Button variant="default" size="sm" onClick={onConvert}>
          <ArrowRightCircle className="h-4 w-4" /> In Rechnung umwandeln
        </Button>
      )}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm">
            <MoreHorizontal className="h-4 w-4" /> Weitere Aktionen
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          <DropdownMenuItem onSelect={onDuplicate}>
            <Copy className="h-4 w-4" /> Duplizieren
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={onCredit}>
            <FileMinus2 className="h-4 w-4" /> Gutschrift erstellen
          </DropdownMenuItem>
          {canStorno && (
            <DropdownMenuItem onSelect={onStorno}>
              <Ban className="h-4 w-4" /> Stornieren
            </DropdownMenuItem>
          )}
          <DropdownMenuSeparator />
          <DropdownMenuItem onSelect={onDelete} className="text-destructive focus:text-destructive">
            <Trash2 className="h-4 w-4" /> Löschen
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
});
