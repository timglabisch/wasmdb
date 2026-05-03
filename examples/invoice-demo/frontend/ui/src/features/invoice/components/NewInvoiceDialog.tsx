import { useCallback } from 'react';
import { Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useCreateDraftInvoice } from '@/features/invoice/actions/createDraftInvoice';

/**
 * "+ Neue Rechnung" one-click action. Creates a blank draft (without a
 * customer) and navigates into the editor — the user picks a customer
 * inside the detail page's customer card.
 */
export function NewInvoiceDialog() {
  const createDraft = useCreateDraftInvoice();
  const onClick = useCallback(() => { void createDraft(); }, [createDraft]);
  return (
    <Button size="sm" onClick={onClick}>
      <Plus className="h-3.5 w-3.5" /> Neue Rechnung
    </Button>
  );
}
