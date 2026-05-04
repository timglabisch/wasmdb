import * as React from 'react';
import { Link, useNavigate, useParams } from '@tanstack/react-router';
import {
  ArrowLeft, MoreHorizontal, Play, Plus, Trash2, X,
} from 'lucide-react';
import { useQuery, useRequirements, createStream, flushStream, nextId } from '@wasmdb/client';
import { execute, executeOnStream } from '@/commands';
import { requirements } from 'invoice-demo-generated/requirements';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Separator } from '@/components/ui/separator';
import {
  Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/sonner';
import {
  BlurInput, BlurTextarea, BlurNumberInput, BlurDateInput, BlurSelect,
  Field, FormSection,
} from '@/components/form';
import { formatEuro, formatDateISO, relativeDaysFromToday } from '@/shared/lib/format';
import { cn } from '@/lib/cn';
import {
  addRecurringPosition,
  updateRecurringPosition,
  deleteRecurringPosition,
  deleteRecurring,
} from 'invoice-demo-generated/InvoiceCommandFactories';
import { usePatchRecurring } from './hooks/usePatchRecurring';
import { runRecurringAction } from './actions/runRecurring';
import {
  formatInterval, formatRelativeDays,
  INTERVAL_UNIT_OPTIONS, STATUS_TEMPLATE_OPTIONS,
} from './lib/interval';

export default function RecurringDetailRoute() {
  const { recurringId } = useParams({ from: '/recurring/$recurringId' });
  const { status, error } = useRequirements([
    requirements.recurring.recurringInvoiceServer.all(),
    requirements.recurring.recurringPositionServer.all(),
    requirements.customers.customerServer.all(),
    requirements.products.productServer.all(),
    requirements.activityLog.activityLogServer.all(),
  ]);
  return (
    <RequirementsGate status={status} error={error} loadingLabel="Lade Serie…">
      <RecurringDetail key={recurringId} recurringId={recurringId} />
    </RequirementsGate>
  );
}

function useRecurringExists(recurringId: string): boolean {
  const rows = useQuery(
    `SELECT recurring_invoices.id FROM recurring_invoices ` +
    `WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([id]) => id as string,
  );
  return rows.length > 0;
}

function RecurringDetail({ recurringId }: { recurringId: string }) {
  const exists = useRecurringExists(recurringId);
  const navigate = useNavigate();
  const templateName = useQuery(
    `SELECT recurring_invoices.template_name FROM recurring_invoices ` +
    `WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([name]) => name as string,
  )[0] ?? recurringId;

  if (!exists) {
    return (
      <>
        <PageHeader title="Serie nicht gefunden" />
        <PageBody>
          <Card className="mx-auto max-w-lg">
            <CardHeader>
              <CardTitle>Serie nicht gefunden</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3 text-sm text-muted-foreground">
              <p>Die Serie mit der ID {recurringId} existiert nicht oder wurde gelöscht.</p>
              <Button asChild variant="secondary" className="self-start">
                <Link to="/recurring">
                  <ArrowLeft className="h-4 w-4" />
                  Zurück zur Übersicht
                </Link>
              </Button>
            </CardContent>
          </Card>
        </PageBody>
      </>
    );
  }

  const patch = usePatchRecurring(recurringId);

  const handleRun = async () => {
    try {
      const r = await runRecurringAction(recurringId);
      if (r) {
        toast.success(`Einmalausführung erstellt — Rechnung ${r.invoiceNumber} angelegt`);
      } else {
        toast.error('Serie nicht gefunden.');
      }
    } catch (err) {
      toast.error(`Ausführung fehlgeschlagen: ${(err as Error).message}`);
    }
  };

  const handleDelete = async () => {
    if (!confirm('Diese Serie inkl. aller Positionen löschen?')) return;
    const stream = createStream(2);
    executeOnStream(stream, deleteRecurring({ id: recurringId, label_for_detail: templateName }));
    await flushStream(stream);
    toast.success('Serie gelöscht');
    navigate({ to: '/recurring' });
  };

  return (
    <>
      <DetailHeader
        recurringId={recurringId}
        onRun={handleRun}
        onDelete={handleDelete}
      />
      <PageBody>
        <div className="mx-auto flex max-w-4xl flex-col gap-4">
          <TemplateCard recurringId={recurringId} patch={patch} />
          <RhythmCard recurringId={recurringId} patch={patch} />
          <InvoiceTemplateCard recurringId={recurringId} patch={patch} />
          <PositionsCard recurringId={recurringId} />
          <HistoryCard recurringId={recurringId} />
        </div>
      </PageBody>
    </>
  );
}

// ── Header (sticky) ──────────────────────────────────────────────────────

function DetailHeader({
  recurringId, onRun, onDelete,
}: {
  recurringId: string;
  onRun: () => void;
  onDelete: () => void;
}) {
  const rows = useQuery(
    `SELECT recurring_invoices.template_name, recurring_invoices.customer_id, recurring_invoices.enabled ` +
    `FROM recurring_invoices WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([name, cid, enabled]) => ({
      name: name as string,
      customerId: cid as string,
      enabled: enabled as number,
    }),
  );
  const t = rows[0];
  const patch = usePatchRecurring(recurringId);

  return (
    <PageHeader
      title={
        <div className="flex items-center gap-3">
          <BlurInput
            className="h-8 border-transparent bg-transparent px-2 text-base font-semibold shadow-none hover:border-input focus-visible:border-input"
            value={t?.name ?? ''}
            onCommit={(name) => patch({ template_name: name })}
            placeholder="Vorlagenname"
          />
          {t && (
            <Badge variant={t.enabled ? 'success' : 'muted'}>
              {t.enabled ? 'Aktiv' : 'Pausiert'}
            </Badge>
          )}
        </div>
      }
      description={
        t ? <HeaderSubtitle customerId={t.customerId} /> : <span>&nbsp;</span>
      }
      actions={
        <div className="flex items-center gap-2">
          <Button size="sm" onClick={onRun}>
            <Play className="h-4 w-4" />
            Jetzt ausführen
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button size="icon" variant="outline" className="h-8 w-8">
                <MoreHorizontal className="h-4 w-4" />
                <span className="sr-only">Aktionen</span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem
                className="text-destructive focus:text-destructive"
                onSelect={() => onDelete()}
              >
                <Trash2 className="h-4 w-4" />
                Löschen
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      }
    />
  );
}

function HeaderSubtitle({ customerId }: { customerId: string }) {
  const rows = useQuery(
    `SELECT customers.name FROM customers WHERE REACTIVE(customers.id = UUID '${customerId}')`,
    ([name]) => name as string,
  );
  const name = rows[0];
  if (name === undefined) return <Skeleton className="h-3 w-40" />;
  return (
    <span>
      Serie für{' '}
      <Link
        to="/customers/$customerId"
        params={{ customerId }}
        className="font-medium text-foreground hover:underline"
      >
        {name || '—'}
      </Link>
    </span>
  );
}

// ── Cards ────────────────────────────────────────────────────────────────

function TemplateCard({
  recurringId, patch,
}: {
  recurringId: string;
  patch: ReturnType<typeof usePatchRecurring>;
}) {
  const rows = useQuery(
    `SELECT recurring_invoices.template_name, recurring_invoices.customer_id ` +
    `FROM recurring_invoices WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([name, cid]) => ({ name: name as string, customerId: cid as string }),
  );
  const t = rows[0];

  const customers = useQuery(
    'SELECT REACTIVE(customers.id), customers.id, customers.name FROM customers ORDER BY customers.name',
    ([_r, id, name]) => ({ id: id as string, name: name as string }),
  );
  const customerOptions = React.useMemo(
    () => customers.map(c => ({ value: c.id, label: c.name || '—' })),
    [customers],
  );

  if (!t) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Vorlage</CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        <FormSection>
          <Field label="Vorlagenname">
            <BlurInput
              value={t.name}
              onCommit={(v) => patch({ template_name: v })}
              placeholder="z. B. Monats-Retainer"
            />
          </Field>
          <Field
            label="Kunde"
            hint="Kunde wird beim Anlegen festgelegt und kann hier nicht geändert werden."
          >
            <BlurSelect
              value={t.customerId}
              onCommit={() => {/* customer is immutable on the update path */}}
              options={customerOptions}
              disabled
              placeholder="Kunde auswählen"
            />
          </Field>
        </FormSection>
      </CardContent>
    </Card>
  );
}

function RhythmCard({
  recurringId, patch,
}: {
  recurringId: string;
  patch: ReturnType<typeof usePatchRecurring>;
}) {
  const rows = useQuery(
    `SELECT recurring_invoices.interval_unit, recurring_invoices.interval_value, ` +
    `recurring_invoices.next_run, recurring_invoices.last_run, recurring_invoices.enabled ` +
    `FROM recurring_invoices WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([unit, value, next, last, enabled]) => ({
      unit: unit as string,
      value: value as number,
      next: next as string,
      last: last as string,
      enabled: enabled as number,
    }),
  );
  const t = rows[0];
  if (!t) return null;

  const days = t.next ? relativeDaysFromToday(t.next) : 0;
  const overdue = t.next && days < 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Rhythmus</CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        <FormSection>
          <Field label="Intervall-Einheit">
            <BlurSelect
              value={t.unit}
              onCommit={(v) => patch({ interval_unit: v })}
              options={INTERVAL_UNIT_OPTIONS}
            />
          </Field>
          <Field label="Intervall-Wert">
            <BlurNumberInput
              value={t.value}
              onCommit={(v) => patch({ interval_value: Math.max(1, v) })}
              min={1}
            />
          </Field>
          <Field label="Nächste Ausführung">
            <BlurDateInput
              value={t.next}
              onCommit={(v) => patch({ next_run: v })}
            />
          </Field>
          <Field label="Status">
            <div className="flex items-center gap-2">
              <BlurSelect
                value={String(t.enabled)}
                onCommit={(v) => patch({ enabled: Number(v) })}
                options={[
                  { value: '1', label: 'Aktiv' },
                  { value: '0', label: 'Pausiert' },
                ]}
              />
            </div>
          </Field>
        </FormSection>

        <Separator className="my-4" />

        <div className="space-y-1 rounded-md bg-muted/40 p-3 text-sm">
          <div>
            <span className="text-muted-foreground">Zusammenfassung: </span>
            <span className="font-medium">{formatInterval(t.unit, t.value)}</span>
            {t.next && (
              <>
                <span className="text-muted-foreground"> — nächste Ausführung </span>
                <span className={cn('font-medium tabular-nums', overdue && 'text-destructive')}>
                  {formatDateISO(t.next)}
                </span>
                <span className={cn('ml-1 text-xs', overdue ? 'text-destructive' : 'text-muted-foreground')}>
                  ({formatRelativeDays(days)})
                </span>
              </>
            )}
          </div>
          {t.last && (
            <div className="text-xs text-muted-foreground">
              Letzte Ausführung: {formatDateISO(t.last)}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function InvoiceTemplateCard({
  recurringId, patch,
}: {
  recurringId: string;
  patch: ReturnType<typeof usePatchRecurring>;
}) {
  const rows = useQuery(
    `SELECT recurring_invoices.status_template, recurring_invoices.notes_template ` +
    `FROM recurring_invoices WHERE REACTIVE(recurring_invoices.id = UUID '${recurringId}')`,
    ([status, notes]) => ({ status: status as string, notes: notes as string }),
  );
  const t = rows[0];
  if (!t) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Rechnungsvorlage</CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        <FormSection>
          <Field
            label="Status bei Anlage"
            hint="Status, mit dem neue Rechnungen aus dieser Serie angelegt werden."
          >
            <BlurSelect
              value={t.status || 'draft'}
              onCommit={(v) => patch({ status_template: v })}
              options={STATUS_TEMPLATE_OPTIONS}
            />
          </Field>
          <Field label="Notizen-Vorlage">
            <BlurTextarea
              value={t.notes}
              onCommit={(v) => patch({ notes_template: v })}
              placeholder="z. B. Monatlicher Retainer gemäß Rahmenvertrag."
            />
          </Field>
        </FormSection>
      </CardContent>
    </Card>
  );
}

// ── Positions ────────────────────────────────────────────────────────────

interface PositionSummary {
  id: string;
  positionNr: number;
}

function PositionsCard({ recurringId }: { recurringId: string }) {
  const positions = useQuery(
    `SELECT recurring_positions.id, recurring_positions.position_nr ` +
    `FROM recurring_positions WHERE REACTIVE(recurring_positions.recurring_id = UUID '${recurringId}') ` +
    `ORDER BY recurring_positions.position_nr`,
    ([id, nr]): PositionSummary => ({ id: id as string, positionNr: nr as number }),
  );

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <CardTitle className="text-sm">Positionen</CardTitle>
        <span className="text-xs text-muted-foreground">{positions.length} Zeilen</span>
      </CardHeader>
      <CardContent className="px-0 pt-0">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="w-10 pl-5">#</TableHead>
              <TableHead>Beschreibung</TableHead>
              <TableHead className="w-24 text-right">Menge</TableHead>
              <TableHead className="w-20">Einheit</TableHead>
              <TableHead className="w-32 text-right">Einzelpreis</TableHead>
              <TableHead className="w-20 text-right">MwSt</TableHead>
              <TableHead className="w-32 text-right">Summe</TableHead>
              <TableHead className="w-10 pr-5" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {positions.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="py-6 text-center text-sm text-muted-foreground">
                  Noch keine Positionen. Füge unten eine Zeile hinzu.
                </TableCell>
              </TableRow>
            ) : (
              positions.map((p, idx) => (
                <PositionRow key={p.id} positionId={p.id} index={idx + 1} />
              ))
            )}
          </TableBody>
          <TableFooter className="bg-transparent">
            <TotalsRow recurringId={recurringId} />
          </TableFooter>
        </Table>

        <Separator />

        <AddPositionForm
          recurringId={recurringId}
          nextNr={nextPositionNr(positions)}
        />
      </CardContent>
    </Card>
  );
}

function nextPositionNr(positions: PositionSummary[]): number {
  if (positions.length === 0) return 1000;
  let max = 0;
  for (const p of positions) if (p.positionNr > max) max = p.positionNr;
  return max + 1000;
}

const transparentCell =
  'h-8 border-transparent bg-transparent px-2 shadow-none hover:border-input focus-visible:border-input';

const PositionRow = React.memo(function PositionRow({
  positionId, index,
}: {
  positionId: string;
  index: number;
}) {
  const rows = useQuery(
    `SELECT recurring_positions.description, recurring_positions.quantity, ` +
    `recurring_positions.unit_price, recurring_positions.tax_rate, recurring_positions.unit, ` +
    `recurring_positions.item_number, recurring_positions.discount_pct ` +
    `FROM recurring_positions WHERE REACTIVE(recurring_positions.id = UUID '${positionId}')`,
    ([desc, qty, price, tax, unit, item, disc]) => ({
      description: desc as string,
      quantity: qty as number,
      unitPrice: price as number,
      taxRate: tax as number,
      unit: unit as string,
      itemNumber: item as string,
      discountPct: disc as number,
    }),
  );
  const p = rows[0];
  if (!p) return null;

  const patch = (partial: Partial<{
    description: string; quantity: number; unit_price: number;
    tax_rate: number; unit: string; item_number: string; discount_pct: number;
  }>) => {
    execute(updateRecurringPosition({
      id: positionId,
      description: p.description,
      quantity: p.quantity,
      unit_price: p.unitPrice,
      tax_rate: p.taxRate,
      unit: p.unit,
      item_number: p.itemNumber,
      discount_pct: p.discountPct,
      ...partial,
    }));
  };

  const net = Math.round((p.quantity * p.unitPrice) / 1000
    * (10000 - p.discountPct) / 10000);
  const lineGross = Math.round(net * (10000 + p.taxRate) / 10000);

  return (
    <TableRow>
      <TableCell className="pl-5 text-xs text-muted-foreground tabular-nums">{index}</TableCell>
      <TableCell>
        <BlurInput
          className={transparentCell}
          value={p.description}
          onCommit={(v) => patch({ description: v })}
          placeholder="Beschreibung"
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={cn(transparentCell, 'text-right tabular-nums')}
          value={p.quantity}
          onCommit={(v) => patch({ quantity: v })}
          min={0}
          step={100}
        />
      </TableCell>
      <TableCell>
        <BlurInput
          className={transparentCell}
          value={p.unit}
          onCommit={(v) => patch({ unit: v })}
          placeholder="Stk"
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={cn(transparentCell, 'text-right tabular-nums')}
          value={p.unitPrice}
          onCommit={(v) => patch({ unit_price: v })}
          min={0}
          step={1}
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={cn(transparentCell, 'text-right tabular-nums')}
          value={p.taxRate}
          onCommit={(v) => patch({ tax_rate: v })}
          min={0}
          step={100}
        />
      </TableCell>
      <TableCell className="text-right tabular-nums">{formatEuro(lineGross)}</TableCell>
      <TableCell className="pr-5 text-right">
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7 text-muted-foreground hover:text-destructive"
          onClick={() => execute(deleteRecurringPosition({ id: positionId }))}
          aria-label="Position löschen"
        >
          <X className="h-4 w-4" />
        </Button>
      </TableCell>
    </TableRow>
  );
});

function TotalsRow({ recurringId }: { recurringId: string }) {
  const positions = useQuery(
    `SELECT recurring_positions.quantity, recurring_positions.unit_price, ` +
    `recurring_positions.tax_rate, recurring_positions.discount_pct ` +
    `FROM recurring_positions WHERE REACTIVE(recurring_positions.recurring_id = UUID '${recurringId}')`,
    ([q, p, t, d]) => ({
      quantity: q as number,
      unitPrice: p as number,
      taxRate: t as number,
      discountPct: d as number,
    }),
  );

  let net = 0;
  let gross = 0;
  for (const p of positions) {
    const raw = (p.quantity * p.unitPrice) / 1000;
    const afterDisc = Math.round(raw * (10000 - p.discountPct) / 10000);
    net += afterDisc;
    gross += Math.round(afterDisc * (10000 + p.taxRate) / 10000);
  }
  const tax = gross - net;

  return (
    <>
      <TableRow className="hover:bg-transparent">
        <TableCell colSpan={5} />
        <TableCell className="text-right text-xs text-muted-foreground">Netto</TableCell>
        <TableCell className="text-right tabular-nums">{formatEuro(net)}</TableCell>
        <TableCell />
      </TableRow>
      <TableRow className="hover:bg-transparent">
        <TableCell colSpan={5} />
        <TableCell className="text-right text-xs text-muted-foreground">MwSt</TableCell>
        <TableCell className="text-right tabular-nums">{formatEuro(tax)}</TableCell>
        <TableCell />
      </TableRow>
      <TableRow className="hover:bg-transparent">
        <TableCell colSpan={5} />
        <TableCell className="text-right text-sm font-semibold">Brutto</TableCell>
        <TableCell className="text-right tabular-nums text-sm font-semibold">{formatEuro(gross)}</TableCell>
        <TableCell />
      </TableRow>
    </>
  );
}

function AddPositionForm({
  recurringId, nextNr,
}: {
  recurringId: string;
  nextNr: number;
}) {
  const [description, setDescription] = React.useState('');
  const [quantity, setQuantity] = React.useState<number>(1);
  const [unitPrice, setUnitPrice] = React.useState<number>(0);

  const reset = () => {
    setDescription('');
    setQuantity(1);
    setUnitPrice(0);
  };

  const submit = (e?: React.FormEvent) => {
    e?.preventDefault();
    const trimmed = description.trim();
    if (!trimmed) {
      toast.error('Bitte eine Beschreibung angeben.');
      return;
    }
    const qty = Math.max(0, Math.round(quantity * 1000));
    const price = Math.max(0, Math.round(unitPrice * 100));
    execute(addRecurringPosition({
      id: nextId(),
      recurring_id: recurringId,
      position_nr: nextNr,
      description: trimmed,
      quantity: qty || 1000,
      unit_price: price,
      tax_rate: 1900,
      unit: 'Stk',
      item_number: '',
      discount_pct: 0,
    }));
    reset();
  };

  return (
    <form onSubmit={submit} className="flex items-center gap-2 px-5 py-3">
      <Input
        className="flex-1"
        placeholder="Beschreibung"
        value={description}
        onChange={(e) => setDescription(e.target.value)}
      />
      <Input
        className="w-20 text-right tabular-nums"
        type="number"
        min={0}
        step={0.25}
        placeholder="Menge"
        value={quantity}
        onChange={(e) => setQuantity(Number(e.target.value) || 0)}
      />
      <Input
        className="w-28 text-right tabular-nums"
        type="number"
        min={0}
        step={0.01}
        placeholder="Einzelpreis (€)"
        value={unitPrice}
        onChange={(e) => setUnitPrice(Number(e.target.value) || 0)}
      />
      <Button type="submit" size="sm" variant="secondary">
        <Plus className="h-4 w-4" />
        Zeile hinzufügen
      </Button>
    </form>
  );
}

// ── History ──────────────────────────────────────────────────────────────

function HistoryCard({ recurringId }: { recurringId: string }) {
  const activity = useQuery(
    `SELECT activity_log.id, activity_log.timestamp, activity_log.action, activity_log.detail ` +
    `FROM activity_log ` +
    `WHERE activity_log.entity_type = 'recurring' ` +
    `AND REACTIVE(activity_log.entity_id = UUID '${recurringId}') ` +
    `ORDER BY activity_log.timestamp DESC`,
    ([id, ts, action, detail]) => ({
      id: id as string,
      timestamp: ts as string,
      action: action as string,
      detail: detail as string,
    }),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Ausführungshistorie</CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        {activity.length === 0 ? (
          <p className="text-sm text-muted-foreground">Noch keine Aktivität für diese Serie.</p>
        ) : (
          <ul className="flex flex-col divide-y">
            {activity.map((a) => (
              <li key={a.id} className="flex items-start gap-3 py-2 text-sm">
                <Badge variant={a.action === 'run' ? 'success' : 'muted'} className="mt-0.5 shrink-0">
                  {a.action}
                </Badge>
                <div className="flex min-w-0 flex-1 flex-col leading-tight">
                  <span className="truncate">{a.detail}</span>
                  <span className="text-xs text-muted-foreground tabular-nums">
                    {formatDateISO(a.timestamp.slice(0, 10))}
                  </span>
                </div>
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}
