import { memo, useEffect, useMemo, useState } from 'react';
import { X } from 'lucide-react';
import {
  Card, CardContent, CardHeader, CardTitle,
} from '@/components/ui/card';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';
import { toast } from '@/components/ui/sonner';
import { execute, useQuery, nextId } from '@/wasm';
import { createPayment } from '@/commands/payment/createPayment';
import { deletePayment } from '@/commands/payment/deletePayment';
import { formatEuro, formatDateISO } from '@/shared/lib/format';
import { useInvoiceGrossCents } from '@/shared/lib/gross';
import { isoDate } from '@/features/invoice/actions/isoDate';

const METHODS = [
  { value: 'transfer', label: 'Überweisung' },
  { value: 'sepa', label: 'SEPA' },
  { value: 'cash', label: 'Bar' },
  { value: 'card', label: 'Karte' },
];

const METHOD_LABEL: Record<string, string> = Object.fromEntries(METHODS.map((m) => [m.value, m.label]));

interface PaymentRow {
  id: string;
  paid_at: string;
  amount: number;
  method: string;
  reference: string;
}

export function PaymentsCard({ invoiceId }: { invoiceId: string }) {
  const payments = useQuery<PaymentRow>(
    `SELECT payments.id, payments.paid_at, payments.amount, payments.method, payments.reference ` +
    `FROM payments WHERE payments.invoice_id = UUID '${invoiceId}' ` +
    `ORDER BY payments.paid_at DESC, payments.id`,
    ([id, paid_at, amount, method, ref]) => ({
      id: id as string,
      paid_at: paid_at as string,
      amount: amount as number,
      method: method as string,
      reference: ref as string,
    }),
  );

  const gross = useInvoiceGrossCents(invoiceId);
  const paid = useMemo(() => payments.reduce((acc, p) => acc + p.amount, 0), [payments]);
  const open = gross - paid;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
        <CardTitle className="text-sm">Zahlungen</CardTitle>
        <Badge variant={open <= 0 ? 'success' : 'destructive'}>
          Offen: {formatEuro(Math.max(0, open))}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-4 pb-5">
        <NewPaymentForm invoiceId={invoiceId} defaultAmount={Math.max(0, open)} />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-32">Datum</TableHead>
              <TableHead className="w-32 text-right">Betrag</TableHead>
              <TableHead className="w-32">Methode</TableHead>
              <TableHead>Referenz</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {payments.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="py-6 text-center text-sm text-muted-foreground">
                  Noch keine Zahlungen erfasst.
                </TableCell>
              </TableRow>
            ) : (
              payments.map((p) => <PaymentLine key={p.id} payment={p} />)
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function NewPaymentForm({
  invoiceId,
  defaultAmount,
}: {
  invoiceId: string;
  defaultAmount: number;
}) {
  const [paidAt, setPaidAt] = useState(() => isoDate(0));
  const [amount, setAmount] = useState<string>(() => String(defaultAmount));
  const [method, setMethod] = useState<string>('transfer');
  const [reference, setReference] = useState('');

  // When the remaining amount changes from below, keep the prefill in sync.
  useEffect(() => {
    setAmount(String(defaultAmount));
  }, [defaultAmount]);

  const submit = () => {
    const n = Number(amount);
    if (!Number.isFinite(n) || n <= 0) {
      toast.error('Betrag ungültig');
      return;
    }
    execute(createPayment({
      id: nextId(),
      invoice_id: invoiceId,
      amount: Math.round(n),
      paid_at: paidAt,
      method,
      reference,
      note: '',
    }));
    toast.success('Zahlung erfasst');
    setReference('');
  };

  return (
    <div className="grid grid-cols-1 gap-2 rounded-md border bg-muted/30 p-3 md:grid-cols-[140px_160px_160px_1fr_auto] md:items-center">
      <div>
        <label className="mb-1 block text-xs text-muted-foreground">Datum</label>
        <Input type="date" value={paidAt} onChange={(e) => setPaidAt(e.target.value)} className="h-8" />
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted-foreground">Betrag (Cent)</label>
        <Input
          type="number"
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          className="h-8 text-right tabular-nums"
        />
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted-foreground">Methode</label>
        <Select value={method} onValueChange={setMethod}>
          <SelectTrigger className="h-8"><SelectValue /></SelectTrigger>
          <SelectContent>
            {METHODS.map((m) => (
              <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted-foreground">Referenz</label>
        <Input
          value={reference}
          onChange={(e) => setReference(e.target.value)}
          placeholder="Verwendungszweck"
          className="h-8"
        />
      </div>
      <div className="self-end">
        <Button size="sm" onClick={submit}>Zahlung erfassen</Button>
      </div>
    </div>
  );
}

const PaymentLine = memo(function PaymentLine({ payment }: { payment: PaymentRow }) {
  const onDelete = () => {
    if (!confirm('Zahlung löschen?')) return;
    execute(deletePayment({ id: payment.id }));
    toast.success('Zahlung gelöscht');
  };
  return (
    <TableRow>
      <TableCell className="whitespace-nowrap text-muted-foreground">
        {formatDateISO(payment.paid_at)}
      </TableCell>
      <TableCell className="text-right tabular-nums">{formatEuro(payment.amount)}</TableCell>
      <TableCell>{METHOD_LABEL[payment.method] ?? payment.method}</TableCell>
      <TableCell className="truncate text-muted-foreground">{payment.reference || '—'}</TableCell>
      <TableCell>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 text-muted-foreground hover:text-destructive"
          onClick={onDelete}
        >
          <X className="h-4 w-4" />
        </Button>
      </TableCell>
    </TableRow>
  );
});
