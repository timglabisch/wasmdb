import type { ReactNode } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import type { RequirementState } from '@wasmdb/client';
interface Props {
  status: RequirementState;
  error?: string;
  loadingLabel?: string;
  children: ReactNode;
}

export function RequirementsGate({ status, error, loadingLabel = 'Lade Daten…', children }: Props) {
  if (status === 'loading' || status === 'idle') {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-16 text-sm text-muted-foreground">
          {loadingLabel}
        </CardContent>
      </Card>
    );
  }
  if (status === 'error') {
    return (
      <Card>
        <CardContent className="flex flex-col items-center justify-center gap-2 py-16 text-center">
          <div className="text-sm font-medium text-destructive">Fehler beim Laden</div>
          <div className="text-xs text-muted-foreground">{error ?? 'Unbekannter Fehler'}</div>
        </CardContent>
      </Card>
    );
  }
  return <>{children}</>;
}
