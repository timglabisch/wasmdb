import * as React from 'react';
import { Link, useNavigate, useParams } from '@tanstack/react-router';
import {
  ArrowLeft, MoreHorizontal, Package, Trash2, TrendingDown, TrendingUp,
} from 'lucide-react';
import { useQuery, useRequirements, createStream, flushStream } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { requirements } from 'invoice-demo-generated/requirements';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/sonner';
import {
  BlurInput, BlurTextarea, BlurNumberInput, BlurSelect, Field, FormSection,
  type BlurSelectOption,
} from '@/components/form';
import { formatEuro } from '@/shared/lib/format';
import { formatBp } from '@/shared/lib/calc';
import { cn } from '@/lib/cn';
import { useProductExists } from './hooks/useProductExists';
import { usePatchProduct } from './hooks/usePatchProduct';
import { peekProduct } from './reads/peekProduct';
import { deleteProduct } from 'invoice-demo-generated/InvoiceCommandFactories';

const UNIT_OPTIONS: BlurSelectOption[] = [
  { value: 'Stk',      label: 'Stk' },
  { value: 'h',        label: 'h (Stunde)' },
  { value: 'Tag',      label: 'Tag' },
  { value: 'Pauschal', label: 'Pauschal' },
  { value: 'Monat',    label: 'Monat' },
  { value: 'kg',       label: 'kg' },
  { value: 'm²',       label: 'm²' },
  { value: 'l',        label: 'l (Liter)' },
];

/**
 * Product detail page. The shell only subscribes to the row's existence via
 * useProductExists; child sections subscribe to the exact columns they render,
 * so field edits are re-render-scoped to their own card.
 */
export default function ProductDetailRoute() {
  const { productId } = useParams({ from: '/products/$productId' });
  const { status, error } = useRequirements([
    requirements.products.productServer.all(),
    requirements.invoices.invoiceServer.all(),
    requirements.positions.positionServer.all(),
  ]);
  const exists = useProductExists(productId);

  if (status === 'loading' || status === 'idle') {
    return (
      <PageBody>
        <RequirementsGate status={status} error={error} loadingLabel="Lade Produkt…">
          <></>
        </RequirementsGate>
      </PageBody>
    );
  }
  if (status === 'error') {
    return (
      <PageBody>
        <RequirementsGate status={status} error={error}>
          <></>
        </RequirementsGate>
      </PageBody>
    );
  }
  if (!exists) return <NotFound />;

  return (
    <>
      <DetailHeader productId={productId} />
      <PageBody>
        <div className="mx-auto flex max-w-3xl flex-col gap-4">
          <MasterDataCard productId={productId} />
          <PriceCard productId={productId} />
          <UsageCard productId={productId} />
        </div>
      </PageBody>
    </>
  );
}

function NotFound() {
  return (
    <>
      <PageHeader title="Produkt" />
      <PageBody>
        <Card className="mx-auto max-w-xl border-dashed">
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
              <Package className="h-5 w-5" />
            </div>
            <div className="text-sm font-semibold">Produkt nicht gefunden</div>
            <div className="text-xs text-muted-foreground">
              Der Artikel wurde entfernt oder existiert nicht.
            </div>
            <Button asChild variant="outline" size="sm">
              <Link to="/products">
                <ArrowLeft className="h-4 w-4" />
                Zurück zu Produkten
              </Link>
            </Button>
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}

// ---------------------------------------------------------------------------
// Header (name inline-edit + status toggle + overflow menu)
// ---------------------------------------------------------------------------

const DetailHeader = React.memo(function DetailHeader({ productId }: { productId: string }) {
  const navigate = useNavigate();
  const patch = usePatchProduct(productId);

  const rows = useQuery(
    `SELECT products.name, products.sku, products.unit, products.active ` +
    `FROM products WHERE REACTIVE(products.id = UUID '${productId}')`,
    ([name, sku, unit, active]) => ({
      name: name as string,
      sku: sku as string,
      unit: unit as string,
      active: active as number,
    }),
  );
  const p = rows[0];

  const toggleActive = async () => {
    if (!p) return;
    const full = peekProduct(productId);
    if (!full) return;
    const nextActive = full.active === 1 ? 0 : 1;
    patch({ active: nextActive });
    toast.success(nextActive === 1 ? 'Produkt aktiviert' : 'Produkt deaktiviert');
  };

  const remove = async () => {
    if (!p) return;
    if (!confirm(`Produkt „${p.name}” wirklich löschen?`)) return;
    const stream = createStream(2);
    executeOnStream(stream, deleteProduct({ id: productId, name: p.name }));
    try {
      await flushStream(stream);
      toast.success('Produkt gelöscht');
      navigate({ to: '/products' });
    } catch (err) {
      toast.error(`Löschen fehlgeschlagen: ${(err as Error).message}`);
    }
  };

  return (
    <header className="sticky top-0 z-20 border-b bg-background/95 px-6 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/70">
      <div className="flex items-start justify-between gap-4">
        <div className="flex min-w-0 items-start gap-3">
          <Button asChild variant="ghost" size="icon" className="mt-1 h-7 w-7 shrink-0 text-muted-foreground">
            <Link to="/products" aria-label="Zurück zu Produkten">
              <ArrowLeft className="h-4 w-4" />
            </Link>
          </Button>
          <div className="min-w-0">
            {p ? (
              <BlurInput
                value={p.name}
                onCommit={(next) => patch({ name: next })}
                placeholder="Produktname"
                className="h-8 border-0 bg-transparent px-0 text-lg font-semibold shadow-none focus-visible:ring-0 focus-visible:border-b focus-visible:rounded-none"
              />
            ) : (
              <div className="h-8" />
            )}
            <div className="mt-0.5 flex items-center gap-2 text-xs text-muted-foreground">
              <span className="font-mono tabular-nums">{p?.sku || '—'}</span>
              <span aria-hidden>·</span>
              <span>{p?.unit || '—'}</span>
            </div>
          </div>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {p && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => void toggleActive()}
              className={cn(
                'h-8 gap-2',
                p.active === 1 ? 'text-emerald-600 dark:text-emerald-500' : 'text-muted-foreground',
              )}
            >
              <span className={cn('h-1.5 w-1.5 rounded-full', p.active === 1 ? 'bg-emerald-500' : 'bg-muted-foreground')} />
              {p.active === 1 ? 'Aktiv' : 'Inaktiv'}
            </Button>
          )}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="icon" className="h-8 w-8" aria-label="Weitere Aktionen">
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-40">
              <DropdownMenuItem
                onSelect={() => void remove()}
                className="text-destructive focus:text-destructive"
              >
                <Trash2 className="h-4 w-4" />
                Löschen
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  );
});

// ---------------------------------------------------------------------------
// Stammdaten
// ---------------------------------------------------------------------------

const MasterDataCard = React.memo(function MasterDataCard({ productId }: { productId: string }) {
  const patch = usePatchProduct(productId);
  const rows = useQuery(
    `SELECT products.sku, products.description, products.unit ` +
    `FROM products WHERE REACTIVE(products.id = UUID '${productId}')`,
    ([sku, description, unit]) => ({
      sku: sku as string,
      description: description as string,
      unit: unit as string,
    }),
  );
  const p = rows[0];
  if (!p) return null;

  const unitValue = UNIT_OPTIONS.some((o) => o.value === p.unit) ? p.unit : '';

  return (
    <Card className="border-border shadow-none">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm font-semibold">Stammdaten</CardTitle>
      </CardHeader>
      <CardContent className="p-4 pt-2">
        <FormSection>
          <Field label="SKU" htmlFor={`prd-${productId}-sku`}>
            <BlurInput
              id={`prd-${productId}-sku`}
              value={p.sku}
              onCommit={(next) => patch({ sku: next })}
              placeholder="PRD-…"
              className="font-mono"
            />
          </Field>
          <Field label="Beschreibung" htmlFor={`prd-${productId}-desc`}>
            <BlurTextarea
              id={`prd-${productId}-desc`}
              value={p.description}
              onCommit={(next) => patch({ description: next })}
              placeholder="Kurzbeschreibung für Rechnungen"
            />
          </Field>
          <Field label="Einheit" htmlFor={`prd-${productId}-unit`}>
            <div className="flex flex-col gap-2 sm:flex-row">
              <BlurSelect
                value={unitValue}
                onCommit={(next) => patch({ unit: next })}
                options={UNIT_OPTIONS}
                placeholder="Einheit wählen…"
                className="sm:w-40"
              />
              <BlurInput
                id={`prd-${productId}-unit`}
                value={p.unit}
                onCommit={(next) => patch({ unit: next })}
                placeholder="frei eingeben"
                className="sm:flex-1"
              />
            </div>
          </Field>
        </FormSection>
      </CardContent>
    </Card>
  );
});

// ---------------------------------------------------------------------------
// Preis & Steuer
// ---------------------------------------------------------------------------

const PriceCard = React.memo(function PriceCard({ productId }: { productId: string }) {
  const patch = usePatchProduct(productId);
  const rows = useQuery(
    `SELECT products.unit_price, products.tax_rate, products.cost_price ` +
    `FROM products WHERE REACTIVE(products.id = UUID '${productId}')`,
    ([unit_price, tax_rate, cost_price]) => ({
      unit_price: unit_price as number,
      tax_rate: tax_rate as number,
      cost_price: cost_price as number,
    }),
  );
  const p = rows[0];
  if (!p) return null;

  const margeAbs = p.unit_price - p.cost_price;
  const margePct = p.unit_price > 0 ? (margeAbs / p.unit_price) * 100 : 0;
  const tone =
    p.unit_price <= 0
      ? 'muted'
      : margePct <= 0
        ? 'destructive'
        : margePct < 20
          ? 'warning'
          : 'success';

  const toneText = {
    muted: 'text-muted-foreground',
    destructive: 'text-destructive',
    warning: 'text-amber-600 dark:text-amber-500',
    success: 'text-emerald-600 dark:text-emerald-500',
  }[tone];

  const toneBar = {
    muted: 'bg-muted-foreground/30',
    destructive: 'bg-destructive',
    warning: 'bg-amber-500',
    success: 'bg-emerald-500',
  }[tone];

  const barWidth = p.unit_price > 0 ? Math.max(0, Math.min(100, margePct)) : 0;

  return (
    <Card className="border-border shadow-none">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm font-semibold">Preis & Steuer</CardTitle>
      </CardHeader>
      <CardContent className="p-4 pt-2">
        <FormSection>
          <Field label="Einzelpreis" htmlFor={`prd-${productId}-price`} hint="in Cent">
            <BlurNumberInput
              id={`prd-${productId}-price`}
              value={p.unit_price}
              onCommit={(next) => patch({ unit_price: next })}
              min={0}
              step={1}
              className="tabular-nums"
            />
          </Field>
          <Field
            label="Steuersatz"
            htmlFor={`prd-${productId}-tax`}
            hint={`Basispunkte — 100 bp = 1 % (aktuell ${formatBp(p.tax_rate)})`}
          >
            <BlurNumberInput
              id={`prd-${productId}-tax`}
              value={p.tax_rate}
              onCommit={(next) => patch({ tax_rate: next })}
              min={0}
              step={1}
              className="tabular-nums"
            />
          </Field>
          <Field label="Kosten" htmlFor={`prd-${productId}-cost`} hint="Einstandskosten in Cent">
            <BlurNumberInput
              id={`prd-${productId}-cost`}
              value={p.cost_price}
              onCommit={(next) => patch({ cost_price: next })}
              min={0}
              step={1}
              className="tabular-nums"
            />
          </Field>
        </FormSection>

        <div className="mt-3 rounded-md border bg-muted/30 p-3">
          <div className="flex items-baseline justify-between gap-4">
            <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
              {margeAbs >= 0 ? (
                <TrendingUp className="h-3.5 w-3.5" aria-hidden />
              ) : (
                <TrendingDown className="h-3.5 w-3.5" aria-hidden />
              )}
              Marge
            </div>
            <div className="flex items-baseline gap-3 tabular-nums">
              <span className={cn('text-base font-semibold', toneText)}>
                {p.unit_price > 0 ? `${margePct.toFixed(1)} %` : '—'}
              </span>
              <span className={cn('text-sm font-medium', toneText)}>
                {formatEuro(margeAbs)}
              </span>
            </div>
          </div>
          <div className="mt-2 h-1 w-full overflow-hidden rounded-full bg-muted">
            <div
              className={cn('h-full rounded-full transition-all', toneBar)}
              style={{ width: `${barWidth}%` }}
            />
          </div>
        </div>
      </CardContent>
    </Card>
  );
});

// ---------------------------------------------------------------------------
// Verwendung
// ---------------------------------------------------------------------------

const UsageCard = React.memo(function UsageCard({ productId }: { productId: string }) {
  const invoiceRows = useQuery(
    `SELECT positions.invoice_id FROM positions WHERE REACTIVE(positions.product_id = UUID '${productId}')`,
    ([invoice_id]) => invoice_id as string,
  );
  const uniqueInvoices = React.useMemo(() => {
    const s = new Set<string>();
    for (const iid of invoiceRows) s.add(iid);
    return s.size;
  }, [invoiceRows]);

  const positionCount = invoiceRows.length;

  return (
    <Card className="border-border shadow-none">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm font-semibold">Verwendung</CardTitle>
      </CardHeader>
      <CardContent className="p-4 pt-2">
        {positionCount === 0 ? (
          <div className="text-sm text-muted-foreground">
            Dieses Produkt wird noch in keiner Rechnung verwendet.
          </div>
        ) : (
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
            <div className="flex items-center gap-2">
              <Badge variant="secondary" className="tabular-nums">{uniqueInvoices}</Badge>
              <span className="text-muted-foreground">
                {uniqueInvoices === 1 ? 'Rechnung' : 'Rechnungen'}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant="muted" className="tabular-nums">{positionCount}</Badge>
              <span className="text-muted-foreground">
                {positionCount === 1 ? 'Position' : 'Positionen'}
              </span>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
});
