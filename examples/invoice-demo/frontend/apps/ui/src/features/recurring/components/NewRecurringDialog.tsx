import * as React from 'react';
import { useNavigate } from '@tanstack/react-router';
import { Plus, Search } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Button } from '@/components/ui/button';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';
import { Field } from '@/components/form';
import { cn } from '@/lib/cn';
import { useQuery, createStream, flushStream, nextId } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { createRecurring } from 'invoice-demo-generated/InvoiceCommandFactories';
import { INTERVAL_UNIT_OPTIONS } from '../lib/interval';

interface CustomerOption { id: string; name: string }

/**
 * Modal for "+ Neue Serie". Picks a customer, sets template name, interval,
 * and next run — creates the recurring template atomically and navigates in.
 */
export function NewRecurringDialog({
  trigger,
}: {
  trigger?: React.ReactNode;
}) {
  const navigate = useNavigate();
  const [open, setOpen] = React.useState(false);
  const [customerId, setCustomerId] = React.useState<string>('');
  const [customerQuery, setCustomerQuery] = React.useState('');
  const [templateName, setTemplateName] = React.useState('');
  const [intervalUnit, setIntervalUnit] = React.useState<string>('month');
  const [intervalValue, setIntervalValue] = React.useState<number>(1);
  const [nextRun, setNextRun] = React.useState<string>(() => {
    const d = new Date();
    d.setDate(d.getDate() + 30);
    return d.toISOString().slice(0, 10);
  });
  const [busy, setBusy] = React.useState(false);

  const customers = useQuery(
    'SELECT REACTIVE(customers.id), customers.id, customers.name FROM customers ORDER BY customers.name',
    ([_r, id, name]) => ({ id: id as string, name: name as string }),
  );

  const filtered = React.useMemo(() => {
    const q = customerQuery.trim().toLowerCase();
    if (!q) return customers;
    return customers.filter(c => c.name.toLowerCase().includes(q));
  }, [customers, customerQuery]);

  const selected = React.useMemo<CustomerOption | undefined>(
    () => customers.find(c => c.id === customerId),
    [customers, customerId],
  );

  const reset = () => {
    setCustomerId('');
    setCustomerQuery('');
    setTemplateName('');
    setIntervalUnit('month');
    setIntervalValue(1);
    const d = new Date();
    d.setDate(d.getDate() + 30);
    setNextRun(d.toISOString().slice(0, 10));
    setBusy(false);
  };

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    if (!customerId) {
      toast.error('Bitte einen Kunden auswählen.');
      return;
    }
    const trimmedName = templateName.trim();
    if (!trimmedName) {
      toast.error('Bitte einen Vorlagennamen eingeben.');
      return;
    }
    if (!nextRun) {
      toast.error('Bitte eine nächste Ausführung wählen.');
      return;
    }
    setBusy(true);
    const id = nextId();
    const stream = createStream(2);
    executeOnStream(stream, createRecurring({
      id,
      customer_id: customerId,
      template_name: trimmedName,
      interval_unit: intervalUnit,
      interval_value: Math.max(1, intervalValue | 0),
      next_run: nextRun,
      status_template: 'draft',
      notes_template: '',
    }));
    try {
      await flushStream(stream);
      toast.success('Serie angelegt');
      setOpen(false);
      reset();
      navigate({ to: '/recurring/$recurringId', params: { recurringId: id } });
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
            Neue Serie
          </Button>
        )}
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={submit}>
          <DialogHeader>
            <DialogTitle>Neue Serie</DialogTitle>
            <DialogDescription>
              Lege eine wiederkehrende Rechnungs-Vorlage an. Positionen fügst du
              in der Detailansicht hinzu.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 space-y-1">
            <Field label="Kunde">
              <div className="space-y-2">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    className="pl-8"
                    placeholder="Kunden suchen …"
                    value={customerQuery}
                    onChange={(e) => setCustomerQuery(e.target.value)}
                  />
                </div>
                <ScrollArea className="h-40 rounded-md border">
                  <ul className="flex flex-col p-1">
                    {filtered.length === 0 ? (
                      <li className="px-2 py-4 text-center text-xs text-muted-foreground">
                        Keine Treffer
                      </li>
                    ) : (
                      filtered.map((c) => (
                        <li key={c.id}>
                          <button
                            type="button"
                            onClick={() => setCustomerId(c.id)}
                            className={cn(
                              'w-full truncate rounded-sm px-2 py-1.5 text-left text-sm outline-none transition-colors',
                              customerId === c.id
                                ? 'bg-primary text-primary-foreground'
                                : 'hover:bg-accent hover:text-accent-foreground',
                            )}
                          >
                            {c.name || '—'}
                          </button>
                        </li>
                      ))
                    )}
                  </ul>
                </ScrollArea>
                {selected && (
                  <div className="text-xs text-muted-foreground">
                    Ausgewählt: <span className="font-medium text-foreground">{selected.name}</span>
                  </div>
                )}
              </div>
            </Field>

            <Field label="Vorlagenname" htmlFor="new-recurring-name">
              <Input
                id="new-recurring-name"
                value={templateName}
                onChange={(e) => setTemplateName(e.target.value)}
                placeholder="z. B. Monats-Retainer"
                required
              />
            </Field>

            <Field label="Intervall">
              <div className="flex gap-2">
                <Input
                  type="number"
                  min={1}
                  className="w-24"
                  value={intervalValue}
                  onChange={(e) => setIntervalValue(Number(e.target.value) || 1)}
                />
                <Select value={intervalUnit} onValueChange={setIntervalUnit}>
                  <SelectTrigger className="flex-1"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {INTERVAL_UNIT_OPTIONS.map((o) => (
                      <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </Field>

            <Field label="Nächste Ausführung" htmlFor="new-recurring-next">
              <Input
                id="new-recurring-next"
                type="date"
                value={nextRun}
                onChange={(e) => setNextRun(e.target.value)}
                required
              />
            </Field>
          </div>

          <DialogFooter className="mt-4">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Abbrechen
            </Button>
            <Button type="submit" disabled={busy || !customerId || !templateName.trim()}>
              Serie anlegen
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
