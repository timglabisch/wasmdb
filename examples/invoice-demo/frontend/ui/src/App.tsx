import { RouterProvider } from '@tanstack/react-router';
import { useWasm } from '@/wasm';
import { router } from '@/router';
import { Skeleton } from '@/components/ui/skeleton';

export default function App() {
  const ready = useWasm();
  if (!ready) {
    return (
      <div className="flex h-screen w-full items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-muted-foreground">
          <Skeleton className="h-10 w-10 rounded-full" />
          <span className="text-xs">WASM wird geladen …</span>
        </div>
      </div>
    );
  }
  return <RouterProvider router={router} />;
}
