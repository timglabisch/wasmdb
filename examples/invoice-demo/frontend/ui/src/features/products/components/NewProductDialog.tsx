import * as React from 'react';
import { useNavigate } from '@tanstack/react-router';
import { Plus } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Button } from '@/components/ui/button';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Field } from '@/components/form';
import { createStream, executeOnStream, flushStream, nextId } from '@/wasm';
import { createProduct } from '@/generated/InvoiceCommandFactories';

/**
 * Modal for the `+ Neues Produkt` action. Creates a minimal product row, logs
 * an activity entry atomically, and navigates into the detail page.
 */
export function NewProductDialog({
  trigger,
}: {
  trigger?: React.ReactNode;
}) {
  const navigate = useNavigate();
  const [open, setOpen] = React.useState(false);
  const [sku, setSku] = React.useState('');
  const [name, setName] = React.useState('');
  const [priceCents, setPriceCents] = React.useState<string>('');
  const [taxBp, setTaxBp] = React.useState<string>('1900');
  const [unit, setUnit] = React.useState('Stk');
  const [busy, setBusy] = React.useState(false);

  const reset = () => {
    setSku(''); setName(''); setPriceCents(''); setTaxBp('1900'); setUnit('Stk'); setBusy(false);
  };

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    const trimmedName = name.trim();
    if (!trimmedName) {
      toast.error('Bitte einen Namen eingeben.');
      return;
    }
    setBusy(true);
    const id = nextId();
    const trimmedSku = sku.trim() || `PRD-${id}`;
    const unitPrice = priceCents === '' ? 0 : Math.max(0, Math.round(Number(priceCents)));
    const taxRate = taxBp === '' ? 1900 : Math.max(0, Math.round(Number(taxBp)));
    const trimmedUnit = unit.trim() || 'Stk';

    const stream = createStream(2);
    executeOnStream(stream, createProduct({
      id,
      sku: trimmedSku,
      name: trimmedName,
      description: '',
      unit: trimmedUnit,
      unit_price: unitPrice,
      tax_rate: taxRate,
      cost_price: 0,
      active: 1,
    }));
    try {
      await flushStream(stream);
      toast.success('Produkt angelegt');
      setOpen(false);
      reset();
      navigate({ to: '/products/$productId', params: { productId: id } });
    } catch (err) {
      toast.error(`Anlegen fehlgeschlagen: ${(err as Error).message}`);
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => { setOpen(o); if (!o) reset(); }}>
      <DialogTrigger asChild>
        {trigger ?? (
          <Button size="sm">
            <Plus className="h-4 w-4" />
            Neues Produkt
          </Button>
        )}
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <form onSubmit={submit}>
          <DialogHeader>
            <DialogTitle>Neues Produkt</DialogTitle>
            <DialogDescription>
              Lege einen neuen Artikel mit minimalen Angaben an. Weitere Felder bearbeitest du direkt in der Detailansicht.
            </DialogDescription>
          </DialogHeader>
          <div className="mt-4 space-y-1">
            <Field label="SKU" htmlFor="new-product-sku" hint="Leer lassen für automatische Vergabe (PRD-…)">
              <Input
                id="new-product-sku"
                value={sku}
                onChange={(e) => setSku(e.target.value)}
                placeholder="z. B. PRD-1001"
                autoComplete="off"
              />
            </Field>
            <Field label="Name" htmlFor="new-product-name">
              <Input
                id="new-product-name"
                autoFocus
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="z. B. Beratungsstunde"
                required
              />
            </Field>
            <Field label="Einzelpreis" htmlFor="new-product-price" hint="in Cent">
              <Input
                id="new-product-price"
                type="number"
                min={0}
                step={1}
                value={priceCents}
                onChange={(e) => setPriceCents(e.target.value)}
                placeholder="0"
                inputMode="numeric"
              />
            </Field>
            <Field label="Steuersatz" htmlFor="new-product-tax" hint="Basispunkte (1900 = 19 %)">
              <Input
                id="new-product-tax"
                type="number"
                min={0}
                step={1}
                value={taxBp}
                onChange={(e) => setTaxBp(e.target.value)}
                placeholder="1900"
                inputMode="numeric"
              />
            </Field>
            <Field label="Einheit" htmlFor="new-product-unit">
              <Input
                id="new-product-unit"
                value={unit}
                onChange={(e) => setUnit(e.target.value)}
                placeholder="Stk"
              />
            </Field>
          </div>
          <DialogFooter className="mt-4">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Abbrechen
            </Button>
            <Button type="submit" disabled={busy || !name.trim()}>
              Produkt anlegen
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
