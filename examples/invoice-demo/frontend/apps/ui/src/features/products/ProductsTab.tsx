import * as React from 'react';
import { useNavigate } from '@tanstack/react-router';
import {
  MoreHorizontal, Package, Search, ExternalLink, Power, Trash2,
} from 'lucide-react';
import { useQuery, useRequirements, createStream, flushStream } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { requirements } from 'invoice-demo-generated/requirements';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { toast } from '@/components/ui/sonner';
import { formatEuro } from '@/shared/lib/format';
import { formatBp } from '@/shared/lib/calc';
import { cn } from '@/lib/cn';
import { NewProductDialog } from './components/NewProductDialog';
import { setProductActive, deleteProduct } from 'invoice-demo-generated/InvoiceCommandFactories';

/**
 * Products list page.
 *
 * The outer list subscribes only to product ids (+ name for client-side
 * filtering). Each row is its own memoized subscription so a mutation to one
 * product does not rerender its siblings.
 */
export default function ProductsTab() {
  const [filter, setFilter] = React.useState('');
  const { status, error } = useRequirements([requirements.products.productServer.all()]);

  return (
    <>
      <PageHeader
        title="Produkte"
        description="Artikelkatalog mit Preisen und Steuersätzen"
        actions={
          <>
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden />
              <Input
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="SKU oder Name suchen…"
                className="h-8 w-64 pl-8 text-sm"
              />
            </div>
            <NewProductDialog />
          </>
        }
      />
      <PageBody>
        <RequirementsGate status={status} error={error} loadingLabel="Lade Produkte…">
          <ProductsList filter={filter} />
        </RequirementsGate>
      </PageBody>
    </>
  );
}

interface ProductListRow {
  id: string;
  sku: string;
  name: string;
}

function ProductsList({ filter }: { filter: string }) {
  const rows = useQuery(
    'SELECT REACTIVE(products.id), products.id, products.sku, products.name FROM products ' +
      'ORDER BY products.name ASC',
    ([_r, id, sku, name]): ProductListRow => ({
      id: id as string,
      sku: sku as string,
      name: name as string,
    }),
  );

  const needle = filter.trim().toLowerCase();
  const filtered = React.useMemo(() => {
    if (!needle) return rows;
    return rows.filter((r) =>
      r.sku.toLowerCase().includes(needle) ||
      r.name.toLowerCase().includes(needle),
    );
  }, [rows, needle]);

  if (rows.length === 0) {
    return <EmptyState />;
  }

  return (
    <Card className="overflow-hidden border-border shadow-none">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/30 hover:bg-muted/30">
            <TableHead className="w-[140px]">SKU</TableHead>
            <TableHead>Name</TableHead>
            <TableHead className="w-[80px]">Einheit</TableHead>
            <TableHead className="w-[110px] text-right">Preis</TableHead>
            <TableHead className="w-[80px] text-right">
              <HeaderWithTooltip label="MwSt" hint="Umsatzsteuer (basispunktbasiert: 1900 = 19 %)" />
            </TableHead>
            <TableHead className="w-[110px] text-right">Kosten</TableHead>
            <TableHead className="w-[100px] text-right">
              <HeaderWithTooltip label="Marge" hint="(Preis – Kosten) / Preis" />
            </TableHead>
            <TableHead className="w-[100px]">Status</TableHead>
            <TableHead className="w-[44px]" aria-label="Aktionen" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {filtered.length === 0 ? (
            <TableRow>
              <TableCell colSpan={9} className="h-24 text-center text-sm text-muted-foreground">
                Keine Treffer für „{filter}“.
              </TableCell>
            </TableRow>
          ) : (
            filtered.map((r) => <ProductListRow key={r.id} productId={r.id} />)
          )}
        </TableBody>
      </Table>
    </Card>
  );
}

function HeaderWithTooltip({ label, hint }: { label: string; hint: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="cursor-help border-b border-dotted border-muted-foreground/50">{label}</span>
      </TooltipTrigger>
      <TooltipContent>{hint}</TooltipContent>
    </Tooltip>
  );
}

interface ProductRowData {
  sku: string;
  name: string;
  unit: string;
  unit_price: number;
  tax_rate: number;
  cost_price: number;
  active: number;
}

const ProductListRow = React.memo(function ProductListRow({ productId }: { productId: string }) {
  const navigate = useNavigate();
  const rows = useQuery(
    `SELECT products.sku, products.name, products.unit, products.unit_price, ` +
    `products.tax_rate, products.cost_price, products.active ` +
    `FROM products WHERE REACTIVE(products.id = UUID '${productId}')`,
    ([sku, name, unit, unit_price, tax_rate, cost_price, active]): ProductRowData => ({
      sku: sku as string,
      name: name as string,
      unit: unit as string,
      unit_price: unit_price as number,
      tax_rate: tax_rate as number,
      cost_price: cost_price as number,
      active: active as number,
    }),
  );
  const p = rows[0];

  if (!p) {
    return (
      <TableRow>
        <TableCell colSpan={9}>
          <Skeleton className="h-4 w-full" />
        </TableCell>
      </TableRow>
    );
  }

  const inactive = p.active === 0;
  const margeAbs = p.unit_price - p.cost_price;
  const margePct = p.unit_price > 0 ? (margeAbs / p.unit_price) * 100 : 0;

  const marginTone =
    p.unit_price <= 0
      ? 'muted'
      : margePct <= 0
        ? 'destructive'
        : margePct < 20
          ? 'warning'
          : 'success';

  const marginClass = {
    muted: 'text-muted-foreground',
    destructive: 'text-destructive',
    warning: 'text-amber-600 dark:text-amber-500',
    success: 'text-emerald-600 dark:text-emerald-500',
  }[marginTone];

  const open = () => navigate({ to: '/products/$productId', params: { productId } });

  const toggleActive = (e?: React.MouseEvent) => {
    e?.stopPropagation();
    const nextActive = p.active === 1 ? 0 : 1;
    const stream = createStream(1);
    executeOnStream(stream, setProductActive({ id: productId, active: nextActive }));
    flushStream(stream).catch((err: unknown) => {
      toast.error(`Statuswechsel fehlgeschlagen: ${(err as Error).message}`);
    });
    toast.success(nextActive === 1 ? 'Produkt aktiviert' : 'Produkt deaktiviert');
  };

  const remove = async (e?: React.MouseEvent) => {
    e?.stopPropagation();
    if (!confirm(`Produkt „${p.name}” wirklich löschen?`)) return;
    const stream = createStream(2);
    executeOnStream(stream, deleteProduct({ id: productId, name: p.name }));
    try {
      await flushStream(stream);
      toast.success('Produkt gelöscht');
    } catch (err) {
      toast.error(`Löschen fehlgeschlagen: ${(err as Error).message}`);
    }
  };

  return (
    <TableRow
      onClick={open}
      className={cn(
        'cursor-pointer',
        inactive && 'text-muted-foreground',
      )}
    >
      <TableCell className="font-mono text-xs tabular-nums">{p.sku || '—'}</TableCell>
      <TableCell>
        <span className={cn('font-medium', inactive && 'text-muted-foreground')}>
          {p.name || '—'}
        </span>
      </TableCell>
      <TableCell className="text-xs text-muted-foreground">{p.unit || '—'}</TableCell>
      <TableCell className="text-right tabular-nums">{formatEuro(p.unit_price)}</TableCell>
      <TableCell className="text-right text-xs tabular-nums text-muted-foreground">
        {formatBp(p.tax_rate)}
      </TableCell>
      <TableCell className="text-right tabular-nums text-muted-foreground">
        {formatEuro(p.cost_price)}
      </TableCell>
      <TableCell className={cn('text-right tabular-nums font-medium', marginClass)}>
        {p.unit_price > 0 ? `${margePct.toFixed(1)} %` : '—'}
      </TableCell>
      <TableCell>
        {inactive ? (
          <Badge variant="muted">inaktiv</Badge>
        ) : (
          <Badge variant="success">aktiv</Badge>
        )}
      </TableCell>
      <TableCell onClick={(e) => e.stopPropagation()}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-muted-foreground"
              aria-label="Aktionen"
            >
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-44">
            <DropdownMenuItem onSelect={() => navigate({ to: '/products/$productId', params: { productId } })}>
              <ExternalLink className="h-4 w-4" />
              Öffnen
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => void toggleActive()}>
              <Power className="h-4 w-4" />
              {inactive ? 'Aktivieren' : 'Deaktivieren'}
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onSelect={() => void remove()}
              className="text-destructive focus:text-destructive"
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

function EmptyState() {
  return (
    <Card className="border-dashed">
      <CardContent className="flex flex-col items-center justify-center gap-4 py-16 text-center">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
          <Package className="h-5 w-5" />
        </div>
        <div className="space-y-1">
          <div className="text-sm font-semibold">Noch keine Produkte</div>
          <div className="text-xs text-muted-foreground">
            Lege einen Artikel an, um ihn auf Rechnungen und Serien zu verwenden.
          </div>
        </div>
        <NewProductDialog />
      </CardContent>
    </Card>
  );
}
