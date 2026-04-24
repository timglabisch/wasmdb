import * as React from 'react';
import { useNavigate } from '@tanstack/react-router';
import {
  FilePlus, MoreHorizontal, Search, Trash2, Users,
} from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Input } from '@/components/ui/input';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { PageBody, PageHeader } from '@/shared/layout/AppShell';
import { useQuery, useAsyncQuery, createStream, executeOnStream, flushStream } from '@/wasm';
import { deleteCustomerCascade } from '@/commands/customer/deleteCustomerCascade';
import { logActivity } from '@/commands/activity/logActivity';
import { formatEuro } from '@/shared/lib/format';
import { useCreateDraftInvoice } from '@/features/invoice/actions/createDraftInvoice';
import { NewCustomerDialog } from './components/NewCustomerDialog';
import { initialsOf } from './lib/util';
import { cn } from '@/lib/cn';

interface IdRow {
  id: number;
  nameKey: string;
  emailKey: string;
}

export default function CustomersTab() {
  const [search, setSearch] = React.useState('');
  const rows = useAsyncQuery(
    'SELECT customers.id, customers.name, customers.email FROM customers.all() ORDER BY customers.name',
    ([id, name, email]) => ({
      id: id as number,
      nameKey: ((name as string) ?? '').toLowerCase(),
      emailKey: ((email as string) ?? '').toLowerCase(),
    }),
  );

  const q = search.trim().toLowerCase();
  const filtered: IdRow[] = q
    ? rows.filter((r) => r.nameKey.includes(q) || r.emailKey.includes(q))
    : rows;

  return (
    <>
      <PageHeader
        title="Kunden"
        description="Stammdaten, Ansprechpartner, Mandate"
        actions={
          <>
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Suche nach Name oder E-Mail"
                className="h-8 w-64 pl-8 text-sm"
              />
            </div>
            <NewCustomerDialog />
          </>
        }
      />

      <PageBody>
        {rows.length === 0 ? (
          <EmptyState />
        ) : (
          <Card className="overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/30 hover:bg-muted/30">
                  <TableHead>Kunde</TableHead>
                  <TableHead>E-Mail</TableHead>
                  <TableHead className="w-[120px] text-right">Zahlungsziel</TableHead>
                  <TableHead className="w-[120px] text-right">Rechnungen</TableHead>
                  <TableHead className="w-[140px] text-right">Offen</TableHead>
                  <TableHead className="w-[48px]" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={6} className="py-10 text-center text-sm text-muted-foreground">
                      Keine Kunden passen zur Suche „{search}".
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((r) => <CustomerListRow key={r.id} customerId={r.id} />)
                )}
              </TableBody>
            </Table>
          </Card>
        )}
      </PageBody>
    </>
  );
}

function EmptyState() {
  return (
    <Card>
      <CardContent className="flex flex-col items-center justify-center gap-4 py-16 text-center">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
          <Users className="h-5 w-5" />
        </div>
        <div className="space-y-1">
          <div className="text-sm font-medium">Noch keine Kunden</div>
          <div className="max-w-sm text-xs text-muted-foreground">
            Lege deinen ersten Kunden an, oder seede dir einen Demo-Datensatz über den Eintrag „Dev-Werkzeuge" in der Seitenleiste.
          </div>
        </div>
        <NewCustomerDialog />
      </CardContent>
    </Card>
  );
}

const CustomerListRow = React.memo(function CustomerListRow({ customerId }: { customerId: number }) {
  const navigate = useNavigate();
  const createDraft = useCreateDraftInvoice();
  const [confirmOpen, setConfirmOpen] = React.useState(false);

  const customer = useQuery(
    `SELECT customers.name, customers.email, customers.payment_terms_days ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([name, email, terms]) => ({
      name: name as string,
      email: email as string,
      paymentTermsDays: terms as number,
    }),
  )[0];

  const invoiceIds = useQuery(
    `SELECT invoices.id FROM invoices WHERE invoices.customer_id = ${customerId} AND invoices.doc_type = 'invoice'`,
    ([id]) => id as number,
  );

  const openPositions = useQuery(
    `SELECT positions.quantity, positions.unit_price, positions.tax_rate, positions.discount_pct, positions.position_type, positions.invoice_id ` +
    `FROM positions JOIN invoices ON invoices.id = positions.invoice_id ` +
    `WHERE invoices.customer_id = ${customerId} ` +
    `AND invoices.doc_type = 'invoice' ` +
    `AND invoices.status IN ('draft', 'sent')`,
    ([q, p, t, d, pt]) => ({
      quantity: q as number, unit_price: p as number, tax_rate: t as number,
      discount_pct: d as number, position_type: pt as string,
    }),
  );

  const openPayments = useQuery(
    `SELECT payments.amount FROM payments JOIN invoices ON invoices.id = payments.invoice_id ` +
    `WHERE invoices.customer_id = ${customerId} ` +
    `AND invoices.doc_type = 'invoice' ` +
    `AND invoices.status IN ('draft', 'sent')`,
    ([amount]) => amount as number,
  );

  const openCents = React.useMemo(() => {
    let gross = 0;
    for (const p of openPositions) {
      if (p.position_type !== 'service' && p.position_type !== 'product') continue;
      const raw = (p.quantity * p.unit_price) / 1000;
      const afterDisc = Math.round(raw * (10000 - p.discount_pct) / 10000);
      gross += Math.round(afterDisc * (10000 + p.tax_rate) / 10000);
    }
    const paid = openPayments.reduce((s, n) => s + n, 0);
    return Math.max(0, gross - paid);
  }, [openPositions, openPayments]);

  if (!customer) return null;

  const onRowClick = () => {
    navigate({ to: '/customers/$customerId', params: { customerId } });
  };

  const doDelete = async () => {
    const stream = createStream(2);
    executeOnStream(stream, deleteCustomerCascade({ id: customerId }));
    executeOnStream(stream, logActivity({
      entityType: 'customer', entityId: customerId,
      action: 'delete', detail: `Kunde "${customer.name}" gelöscht (Kaskade)`,
    }));
    try {
      await flushStream(stream);
      toast.success('Kunde gelöscht');
    } catch (err) {
      toast.error(`Löschen fehlgeschlagen: ${(err as Error).message}`);
    }
    setConfirmOpen(false);
  };

  return (
    <>
      <TableRow
        className="cursor-pointer"
        onClick={onRowClick}
      >
        <TableCell className="max-w-0">
          <div className="flex items-center gap-3">
            <Avatar className="h-8 w-8 shrink-0">
              <AvatarFallback className="bg-primary/10 text-primary">
                {initialsOf(customer.name)}
              </AvatarFallback>
            </Avatar>
            <div className="min-w-0">
              <div className="truncate text-sm font-medium">{customer.name || 'Unbenannt'}</div>
            </div>
          </div>
        </TableCell>
        <TableCell className="max-w-0">
          <span className="block truncate text-sm text-muted-foreground">
            {customer.email || '—'}
          </span>
        </TableCell>
        <TableCell className="text-right text-sm tabular-nums text-muted-foreground">
          {customer.paymentTermsDays} Tage
        </TableCell>
        <TableCell className="text-right text-sm tabular-nums text-muted-foreground">
          {invoiceIds.length}
        </TableCell>
        <TableCell className={cn(
          'text-right text-sm tabular-nums font-medium',
          openCents > 0 ? 'text-foreground' : 'text-muted-foreground',
        )}>
          {formatEuro(openCents)}
        </TableCell>
        <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                aria-label={`Menü für ${customer.name}`}
              >
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-48">
              <DropdownMenuItem onSelect={() => navigate({ to: '/customers/$customerId', params: { customerId } })}>
                <Users className="h-4 w-4" />
                Öffnen
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => { void createDraft(customerId); }}>
                <FilePlus className="h-4 w-4" />
                Rechnung anlegen
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                className="text-destructive focus:text-destructive"
                onSelect={(e) => { e.preventDefault(); setConfirmOpen(true); }}
              >
                <Trash2 className="h-4 w-4" />
                Löschen …
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </TableCell>
      </TableRow>
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Kunde löschen?</DialogTitle>
            <DialogDescription>
              Der Kunde <span className="font-medium text-foreground">„{customer.name}"</span> und alle
              zugehörigen Rechnungen, Positionen, Zahlungen, Kontakte, Mandate sowie Serien werden dauerhaft entfernt.
              Diese Aktion lässt sich nicht rückgängig machen.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setConfirmOpen(false)}>Abbrechen</Button>
            <Button variant="destructive" onClick={doDelete}>Endgültig löschen</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
});

