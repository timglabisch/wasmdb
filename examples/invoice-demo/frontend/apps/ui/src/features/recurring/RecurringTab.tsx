import * as React from 'react';
import { Link, useNavigate } from '@tanstack/react-router';
import {
  MoreHorizontal, Play, Search, Trash2, ExternalLink, RefreshCw,
} from 'lucide-react';
import { useQuery, useRequirements, createStream, flushStream } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { requirements } from 'invoice-demo-generated/requirements';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/sonner';
import { formatDateISO, relativeDaysFromToday } from '@/shared/lib/format';
import { cn } from '@/lib/cn';
import { deleteRecurring } from 'invoice-demo-generated/InvoiceCommandFactories';
import { NewRecurringDialog } from './components/NewRecurringDialog';
import { runRecurringAction } from './actions/runRecurring';
import { formatInterval, formatRelativeDays } from './lib/interval';

interface RowId {
  id: string;
  nextRun: string;
}

export default function RecurringTab() {
  const [query, setQuery] = React.useState('');

  const { status, error } = useRequirements([
    requirements.recurring.recurringInvoiceServer.all(),
    requirements.customers.customerServer.all(),
  ]);
  const ids = useQuery(
    'SELECT REACTIVE(recurring_invoices.id), recurring_invoices.id, recurring_invoices.next_run ' +
    'FROM recurring_invoices ORDER BY recurring_invoices.next_run ASC',
    ([_r, id, nextRun]) => ({ id: id as string, nextRun: nextRun as string }),
  );

  return (
    <>
      <PageHeader
        title="Serien"
        description="Wiederkehrende Rechnungen und deren Vorlagen."
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                className="h-8 w-56 pl-8"
                placeholder="Vorlage suchen …"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            </div>
            <NewRecurringDialog />
          </div>
        }
      />
      <PageBody>
        <RequirementsGate status={status} error={error} loadingLabel="Lade Serien…">
        {ids.length === 0 ? (
          <EmptyState />
        ) : (
          <Card className="overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Vorlage</TableHead>
                  <TableHead>Kunde</TableHead>
                  <TableHead>Intervall</TableHead>
                  <TableHead>Nächste Ausführung</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {ids.map((row) => (
                  <RecurringTableRow
                    key={row.id}
                    recurringId={row.id}
                    query={query}
                  />
                ))}
              </TableBody>
            </Table>
          </Card>
        )}
        </RequirementsGate>
      </PageBody>
    </>
  );
}

function EmptyState() {
  return (
    <Card className="mx-auto max-w-xl border-dashed">
      <CardHeader className="items-center text-center">
        <div className="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-muted text-muted-foreground">
          <RefreshCw className="h-5 w-5" />
        </div>
        <CardTitle className="mt-2">Noch keine Serien</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col items-center gap-3 text-center text-sm text-muted-foreground">
        <p>
          Wiederkehrende Rechnungen laufen automatisch nach einem Intervall.
          Lege eine Vorlage an, füge Positionen hinzu und führe sie aus.
        </p>
        <p className="text-xs">Seed-Daten enthalten Beispiel-Serien.</p>
        <NewRecurringDialog />
      </CardContent>
    </Card>
  );
}

interface RowProps {
  recurringId: string;
  query: string;
}

const RecurringTableRow = React.memo(function RecurringTableRow({ recurringId, query }: RowProps) {
  const navigate = useNavigate();
  const rows = useQuery(
    `SELECT recurring_invoices.template_name, recurring_invoices.customer_id, ` +
    `recurring_invoices.interval_unit, recurring_invoices.interval_value, ` +
    `recurring_invoices.next_run, recurring_invoices.enabled, recurring_invoices.status_template ` +
    `FROM recurring_invoices WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([name, cid, unit, value, next, enabled, status]) => ({
      name: name as string,
      customerId: cid as string,
      intervalUnit: unit as string,
      intervalValue: value as number,
      nextRun: next as string,
      enabled: enabled as number,
      statusTemplate: status as string,
    }),
  );
  const t = rows[0];

  if (!t) return null;

  const q = query.trim().toLowerCase();
  if (q && !t.name.toLowerCase().includes(q)) return null;

  const days = t.nextRun ? relativeDaysFromToday(t.nextRun) : 0;
  const overdue = t.nextRun && days < 0;

  const openDetail = () => navigate({
    to: '/recurring/$recurringId', params: { recurringId },
  });

  const handleRun = async () => {
    try {
      const r = await runRecurringAction(recurringId);
      if (r) toast.success(`Einmalausführung erstellt — Rechnung ${r.invoiceNumber}`);
      else toast.error('Serie nicht gefunden.');
    } catch (err) {
      toast.error(`Ausführung fehlgeschlagen: ${(err as Error).message}`);
    }
  };

  const handleDelete = async () => {
    if (!confirm(`Serie "${t.name}" inkl. aller Positionen löschen?`)) return;
    const stream = createStream(2);
    executeOnStream(stream, deleteRecurring({ id: recurringId, label_for_detail: t.name }));
    await flushStream(stream);
    toast.success('Serie gelöscht');
  };

  return (
    <TableRow
      className="cursor-pointer"
      onClick={openDetail}
    >
      <TableCell className="font-medium">
        {t.name || <span className="text-muted-foreground">— unbenannt —</span>}
      </TableCell>
      <TableCell>
        <CustomerCell customerId={t.customerId} />
      </TableCell>
      <TableCell className="text-sm text-muted-foreground">
        {formatInterval(t.intervalUnit, t.intervalValue)}
      </TableCell>
      <TableCell className="tabular-nums">
        {t.nextRun ? (
          <div className="flex flex-col leading-tight">
            <span className={cn('text-sm', overdue && 'font-medium text-destructive')}>
              {formatDateISO(t.nextRun)}
            </span>
            <span className={cn('text-xs', overdue ? 'text-destructive' : 'text-muted-foreground')}>
              {formatRelativeDays(days)}
            </span>
          </div>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </TableCell>
      <TableCell>
        <Badge variant={t.enabled ? 'success' : 'muted'}>
          {t.enabled ? 'Aktiv' : 'Pausiert'}
        </Badge>
      </TableCell>
      <TableCell className="text-right">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={(e) => e.stopPropagation()}
            >
              <MoreHorizontal className="h-4 w-4" />
              <span className="sr-only">Aktionen</span>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
            <DropdownMenuItem onSelect={() => openDetail()}>
              <ExternalLink className="h-4 w-4" />
              Öffnen
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => { void handleRun(); }}>
              <Play className="h-4 w-4" />
              Jetzt ausführen
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              className="text-destructive focus:text-destructive"
              onSelect={() => { void handleDelete(); }}
            >
              <Trash2 className="h-4 w-4" />
              Löschen
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </TableCell>
    </TableRow>
  );
});

const CustomerCell = React.memo(function CustomerCell({ customerId }: { customerId: string }) {
  const rows = useQuery(
    `SELECT customers.name FROM customers WHERE REACTIVE(customers.id = UUID '${customerId}')`,
    ([name]) => name as string,
  );
  const name = rows[0];
  if (name === undefined) {
    return <Skeleton className="h-3.5 w-32" />;
  }
  return (
    <Link
      to="/customers/$customerId"
      params={{ customerId }}
      onClick={(e) => e.stopPropagation()}
      className="text-sm text-muted-foreground hover:text-foreground hover:underline"
    >
      {name || '—'}
    </Link>
  );
});
