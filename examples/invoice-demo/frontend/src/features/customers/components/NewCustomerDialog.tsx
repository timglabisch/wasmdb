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
import { createCustomer } from '@/commands/customer/createCustomer';
import { todayISO } from '../lib/util';

/**
 * Modal for the `+ Neuer Kunde` action. Creates a minimal customer row,
 * logs an activity entry atomically, and navigates into the detail page.
 */
export function NewCustomerDialog({
  trigger,
}: {
  trigger?: React.ReactNode;
}) {
  const navigate = useNavigate();
  const [open, setOpen] = React.useState(false);
  const [name, setName] = React.useState('');
  const [email, setEmail] = React.useState('');
  const [busy, setBusy] = React.useState(false);

  const reset = () => {
    setName(''); setEmail(''); setBusy(false);
  };

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) {
      toast.error('Bitte einen Namen eingeben.');
      return;
    }
    setBusy(true);
    const id = nextId();
    const stream = createStream(2);
    executeOnStream(stream, createCustomer({
      id, name: trimmed, email: email.trim(),
      created_at: todayISO(),
      company_type: 'company', payment_terms_days: 14,
      billing_country: 'DE', shipping_country: 'DE',
    }));
    try {
      await flushStream(stream);
      toast.success('Kunde angelegt');
      setOpen(false);
      reset();
      navigate({ to: '/customers/$customerId', params: { customerId: id } });
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
            Neuer Kunde
          </Button>
        )}
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <form onSubmit={submit}>
          <DialogHeader>
            <DialogTitle>Neuer Kunde</DialogTitle>
            <DialogDescription>
              Lege einen neuen Kunden mit minimalen Angaben an. Weitere Felder bearbeitest du direkt in der Detailansicht.
            </DialogDescription>
          </DialogHeader>
          <div className="mt-4 space-y-1">
            <Field label="Name" htmlFor="new-customer-name">
              <Input
                id="new-customer-name"
                autoFocus
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="z. B. Acme Industries GmbH"
                required
              />
            </Field>
            <Field label="E-Mail" htmlFor="new-customer-email">
              <Input
                id="new-customer-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="rechnungen@example.com"
              />
            </Field>
          </div>
          <DialogFooter className="mt-4">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Abbrechen
            </Button>
            <Button type="submit" disabled={busy || !name.trim()}>
              Kunde anlegen
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
