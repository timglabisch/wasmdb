import * as React from 'react';
import { Plus } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Button } from '@/components/ui/button';
import {
  Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Field } from '@/components/form';
import { createStream, executeOnStream, flushStream, nextId } from '@/wasm';
import { createSepaMandate } from '@/commands/sepaMandate/createSepaMandate';
import { todayISO } from '../lib/util';

/** Inline dialog for adding a SEPA-Mandat to a customer. */
export function NewSepaMandateDialog({ customerId }: { customerId: string }) {
  const [open, setOpen] = React.useState(false);
  const [mandateRef, setMandateRef] = React.useState('');
  const [iban, setIban] = React.useState('');
  const [bic, setBic] = React.useState('');
  const [holder, setHolder] = React.useState('');
  const [signedAt, setSignedAt] = React.useState(todayISO());
  const [busy, setBusy] = React.useState(false);

  const reset = () => {
    setMandateRef(''); setIban(''); setBic('');
    setHolder(''); setSignedAt(todayISO()); setBusy(false);
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const ref = mandateRef.trim();
    if (!ref) { toast.error('Mandats-Referenz erforderlich.'); return; }
    setBusy(true);
    const id = nextId();
    const stream = createStream(2);
    executeOnStream(stream, createSepaMandate({
      id, customer_id: customerId,
      mandate_ref: ref,
      iban: iban.replace(/\s+/g, '').toUpperCase(),
      bic: bic.trim().toUpperCase(),
      holder_name: holder.trim(),
      signed_at: signedAt,
    }));
    try {
      await flushStream(stream);
      toast.success('SEPA-Mandat angelegt');
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
            <DialogTitle>Neues SEPA-Mandat</DialogTitle>
          </DialogHeader>
          <div className="mt-4 space-y-1">
            <Field label="Mandats-Ref" htmlFor="sepa-ref">
              <Input
                id="sepa-ref" autoFocus required
                placeholder="z. B. MAND-2026-0001"
                value={mandateRef} onChange={(e) => setMandateRef(e.target.value)}
              />
            </Field>
            <Field label="IBAN" htmlFor="sepa-iban">
              <Input
                id="sepa-iban"
                placeholder="DE00 0000 0000 0000 0000 00"
                value={iban} onChange={(e) => setIban(e.target.value)}
              />
            </Field>
            <Field label="BIC" htmlFor="sepa-bic">
              <Input
                id="sepa-bic"
                value={bic} onChange={(e) => setBic(e.target.value)}
              />
            </Field>
            <Field label="Kontoinhaber" htmlFor="sepa-holder">
              <Input
                id="sepa-holder"
                value={holder} onChange={(e) => setHolder(e.target.value)}
              />
            </Field>
            <Field label="Unterschrieben am" htmlFor="sepa-signed">
              <Input
                id="sepa-signed" type="date"
                value={signedAt} onChange={(e) => setSignedAt(e.target.value)}
              />
            </Field>
          </div>
          <DialogFooter className="mt-4">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Abbrechen
            </Button>
            <Button type="submit" disabled={busy || !mandateRef.trim()}>
              Anlegen
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
