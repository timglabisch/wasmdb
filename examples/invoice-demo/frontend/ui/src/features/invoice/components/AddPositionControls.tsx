import { memo, useCallback, useMemo, useState } from 'react';
import { Plus, Search, Package } from 'lucide-react';
import {
  Popover, PopoverContent, PopoverTrigger,
} from '@/components/ui/popover';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useQuery, nextId } from '@wasmdb/client';
import { execute } from '@/commands';
import { addPosition } from '@/generated/InvoiceCommandFactories';
import { formatEuro } from '@/shared/lib/format';

interface ProductRow {
  id: string;
  sku: string;
  name: string;
  unit: string;
  unit_price: number;
  tax_rate: number;
  cost_price: number;
}

/**
 * Inline row-adding controls at the bottom of the positions grid.
 *
 * Primary: "+ Zeile" — one click adds an empty position and focuses its
 * description cell. This is the 99%-case and should feel instant.
 *
 * Secondary: "+ Produkt" — opens a compact Popover with a product picker
 * for the rarer case where the user wants to copy prices from a product.
 * Using a Popover over a Dialog avoids the heavy overlay + slide animation.
 */
export const AddPositionControls = memo(function AddPositionControls({
  invoiceId,
  nextPositionNr,
  onAdded,
}: {
  invoiceId: string;
  nextPositionNr: number;
  onAdded: (id: string) => void;
}) {
  const onEmpty = useCallback(() => {
    const id = nextId();
    execute(addPosition({
      id, invoice_id: invoiceId, position_nr: nextPositionNr,
      description: '',
      quantity: 1000,
      unit_price: 0,
      tax_rate: 1900,
      product_id: null,
      item_number: '',
      unit: 'Stk',
      discount_pct: 0,
      cost_price: 0,
      position_type: 'service',
    }));
    onAdded(id);
  }, [invoiceId, nextPositionNr, onAdded]);

  return (
    <div className="flex items-center gap-1.5">
      <Button variant="outline" size="sm" onClick={onEmpty}>
        <Plus className="h-3.5 w-3.5" /> Zeile
      </Button>
      <ProductPicker
        invoiceId={invoiceId}
        nextPositionNr={nextPositionNr}
        onAdded={onAdded}
      />
    </div>
  );
});

const ProductPicker = memo(function ProductPicker({
  invoiceId,
  nextPositionNr,
  onAdded,
}: {
  invoiceId: string;
  nextPositionNr: number;
  onAdded: (id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [term, setTerm] = useState('');

  const products = useQuery<ProductRow>(
    'SELECT REACTIVE(products.id), products.id, products.sku, products.name, products.unit, products.unit_price, products.tax_rate, products.cost_price FROM products WHERE products.active = 1 ORDER BY products.name',
    ([_r, id, sku, name, unit, unit_price, tax_rate, cost_price]) => ({
      id: id as string,
      sku: sku as string,
      name: name as string,
      unit: unit as string,
      unit_price: unit_price as number,
      tax_rate: tax_rate as number,
      cost_price: cost_price as number,
    }),
  );

  const filtered = useMemo(() => {
    const t = term.trim().toLowerCase();
    if (!t) return products;
    return products.filter((p) =>
      p.name.toLowerCase().includes(t) ||
      p.sku.toLowerCase().includes(t),
    );
  }, [products, term]);

  const onPickProduct = useCallback((p: ProductRow) => {
    const id = nextId();
    execute(addPosition({
      id, invoice_id: invoiceId, position_nr: nextPositionNr,
      description: p.name,
      quantity: 1000,
      unit_price: p.unit_price,
      tax_rate: p.tax_rate,
      product_id: p.id,
      item_number: p.sku,
      unit: p.unit,
      discount_pct: 0,
      cost_price: p.cost_price,
      position_type: 'product',
    }));
    onAdded(id);
    setOpen(false);
    setTerm('');
  }, [invoiceId, nextPositionNr, onAdded]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="sm">
          <Package className="h-3.5 w-3.5" /> aus Produkt
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-[380px] p-0">
        <div className="relative border-b">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            autoFocus
            value={term}
            onChange={(e) => setTerm(e.target.value)}
            placeholder="Produkt oder SKU suchen..."
            className="h-9 rounded-none border-0 pl-9 focus-visible:ring-0"
          />
        </div>
        <ScrollArea className="max-h-64">
          {filtered.length === 0 ? (
            <div className="px-4 py-6 text-center text-xs text-muted-foreground">
              Keine Produkte gefunden.
            </div>
          ) : (
            <ul className="divide-y">
              {filtered.map((p) => (
                <li key={p.id}>
                  <button
                    type="button"
                    onClick={() => onPickProduct(p)}
                    className="flex w-full items-center justify-between gap-3 px-3 py-1.5 text-left transition-colors hover:bg-accent"
                  >
                    <div className="min-w-0">
                      <div className="truncate text-[13px] font-medium leading-tight">{p.name}</div>
                      <div className="truncate text-[11px] text-muted-foreground">
                        {p.sku}{p.unit && ` · ${p.unit}`}
                      </div>
                    </div>
                    <div className="shrink-0 text-[12px] tabular-nums text-muted-foreground">
                      {formatEuro(p.unit_price)}
                    </div>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </ScrollArea>
      </PopoverContent>
    </Popover>
  );
});
