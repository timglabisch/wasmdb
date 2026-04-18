import { Wrench } from 'lucide-react';
import { PageHeader, PageBody } from './AppShell';
import { Card, CardContent } from '@/components/ui/card';

/**
 * Page stub. Replaced by the page-specific agent with a real implementation.
 */
export function Placeholder({ title, description }: { title: string; description?: string }) {
  return (
    <>
      <PageHeader title={title} description={description} />
      <PageBody>
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
              <Wrench className="h-5 w-5" />
            </div>
            <div className="text-sm font-medium">Seite wird gerade neu gebaut</div>
            <div className="text-xs text-muted-foreground">
              Diese Ansicht wird vom zuständigen Agenten in Kürze durch die endgültige UI ersetzt.
            </div>
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}
