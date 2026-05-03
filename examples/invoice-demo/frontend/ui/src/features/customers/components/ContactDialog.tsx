import * as React from 'react';
import { Plus } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Field } from '@/components/form';
import { createStream, flushStream, nextId } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { createContact } from '@/generated/InvoiceCommandFactories';

/**
 * Inline dialog for adding an Ansprechpartner to a customer.
 */
export function NewContactDialog({ customerId }: { customerId: string }) {
  const [open, setOpen] = React.useState(false);
  const [name, setName] = React.useState('');
  const [role, setRole] = React.useState('');
  const [email, setEmail] = React.useState('');
  const [phone, setPhone] = React.useState('');
  const [isPrimary, setIsPrimary] = React.useState(false);
  const [busy, setBusy] = React.useState(false);

  const reset = () => {
    setName(''); setRole(''); setEmail(''); setPhone(''); setIsPrimary(false); setBusy(false);
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) { toast.error('Name erforderlich.'); return; }
    setBusy(true);
    const id = nextId();
    const stream = createStream(2);
    executeOnStream(stream, createContact({
      id, customer_id: customerId,
      name: trimmed, role: role.trim(),
      email: email.trim(), phone: phone.trim(),
      is_primary: isPrimary ? 1 : 0,
    }));
    try {
      await flushStream(stream);
      toast.success('Ansprechpartner angelegt');
      setOpen(false);
      reset();
    } catch (err) {
      toast.error(`Fehlgeschlagen: ${(err as Error).message}`);
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => { setOpen(o); if (!o) reset(); }}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Plus className="h-4 w-4" />
          Hinzufügen
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <form onSubmit={submit}>
          <DialogHeader>
            <DialogTitle>Neuer Ansprechpartner</DialogTitle>
          </DialogHeader>
          <div className="mt-4 space-y-1">
            <Field label="Name" htmlFor="contact-name">
              <Input
                id="contact-name" autoFocus required
                value={name} onChange={(e) => setName(e.target.value)}
              />
            </Field>
            <Field label="Rolle" htmlFor="contact-role">
              <Input
                id="contact-role"
                placeholder="z. B. Buchhaltung"
                value={role} onChange={(e) => setRole(e.target.value)}
              />
            </Field>
            <Field label="E-Mail" htmlFor="contact-email">
              <Input
                id="contact-email" type="email"
                value={email} onChange={(e) => setEmail(e.target.value)}
              />
            </Field>
            <Field label="Telefon" htmlFor="contact-phone">
              <Input
                id="contact-phone"
                value={phone} onChange={(e) => setPhone(e.target.value)}
              />
            </Field>
            <Field label="Hauptkontakt">
              <label className="flex cursor-pointer items-center gap-2 text-sm text-muted-foreground">
                <Checkbox
                  checked={isPrimary}
                  onCheckedChange={(v) => setIsPrimary(v === true)}
                />
                Als Hauptkontakt markieren
              </label>
            </Field>
          </div>
          <DialogFooter className="mt-4">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Abbrechen
            </Button>
            <Button type="submit" disabled={busy || !name.trim()}>
              Anlegen
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
