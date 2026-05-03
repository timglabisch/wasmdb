import { Badge } from '@/components/ui/badge';

export const DOC_TYPE_LABEL: Record<string, string> = {
  invoice: 'Rechnung',
  offer: 'Angebot',
  credit_note: 'Gutschrift',
  delivery_note: 'Lieferschein',
  proforma: 'Proforma',
};

export const STATUS_LABEL: Record<string, string> = {
  draft: 'Entwurf',
  sent: 'Versendet',
  paid: 'Bezahlt',
  cancelled: 'Storniert',
};

export function DocTypeBadge({ docType }: { docType: string }) {
  return <Badge variant="muted">{DOC_TYPE_LABEL[docType] ?? docType}</Badge>;
}

export function InvoiceStatusBadge({ status }: { status: string }) {
  const variant =
    status === 'paid'
      ? 'success'
      : status === 'sent'
      ? 'warning'
      : status === 'cancelled'
      ? 'destructive'
      : 'secondary';
  return <Badge variant={variant}>{STATUS_LABEL[status] ?? status}</Badge>;
}

export type PaymentStatus = 'unpaid' | 'partial' | 'paid' | 'overpaid';

export function computePaymentStatus(grossCents: number, paidCents: number): PaymentStatus {
  if (paidCents <= 0) return 'unpaid';
  if (paidCents > grossCents) return 'overpaid';
  if (paidCents >= grossCents) return 'paid';
  return 'partial';
}

const PAY_LABEL: Record<PaymentStatus, string> = {
  unpaid: 'offen',
  partial: 'teilweise',
  paid: 'bezahlt',
  overpaid: 'überzahlt',
};

export function PaymentStatusBadge({ status }: { status: PaymentStatus }) {
  const variant: React.ComponentProps<typeof Badge>['variant'] =
    status === 'paid' ? 'success' : status === 'partial' ? 'warning' : status === 'overpaid' ? 'warning' : 'muted';
  return <Badge variant={variant}>{PAY_LABEL[status]}</Badge>;
}

export function isOverdue(dateDue: string, status: string): boolean {
  if (!dateDue || status === 'paid' || status === 'cancelled') return false;
  return dateDue < new Date().toISOString().slice(0, 10);
}
