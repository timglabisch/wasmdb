import { Link, Outlet, useMatchRoute } from '@tanstack/react-router';
import {
  LayoutDashboard, FileText, Users, Package, RefreshCw, Activity, Database,
} from 'lucide-react';
import { DebugToolbar } from '@wasmdb/debug-toolbar';
import { Toaster } from '@/components/ui/sonner';
import { TooltipProvider } from '@/components/ui/tooltip';
import { SeedMenu } from '@/shared/layout/SeedMenu';
import { cn } from '@/lib/cn';

type NavItem = {
  to: string;
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  fuzzy?: boolean;
};

const PRIMARY_NAV: NavItem[] = [
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/invoices',  icon: FileText,        label: 'Rechnungen', fuzzy: true },
  { to: '/customers', icon: Users,           label: 'Kunden',     fuzzy: true },
  { to: '/products',  icon: Package,         label: 'Produkte',   fuzzy: true },
  { to: '/recurring', icon: RefreshCw,       label: 'Serien',     fuzzy: true },
  { to: '/activity',  icon: Activity,        label: 'Aktivität' },
];

export default function AppShell() {
  return (
    <TooltipProvider delayDuration={200}>
      <div className="flex h-screen w-full overflow-hidden bg-background text-foreground">
        <Sidebar />
        <main className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <Outlet />
        </main>
      </div>
      <Toaster />
      {import.meta.env.DEV && <DebugToolbar />}
    </TooltipProvider>
  );
}

function Sidebar() {
  return (
    <aside className="flex h-full w-[208px] shrink-0 flex-col border-r bg-muted/30">
      <div className="flex h-11 items-center gap-2 px-3">
        <div className="flex h-6 w-6 items-center justify-center rounded bg-primary text-primary-foreground">
          <Database className="h-3.5 w-3.5" />
        </div>
        <div className="flex flex-col leading-tight">
          <span className="text-[13px] font-semibold tracking-tight">wasmdb</span>
          <span className="text-[10px] text-muted-foreground">invoice demo</span>
        </div>
      </div>

      <nav className="flex flex-1 flex-col gap-px overflow-y-auto px-2 pb-2">
        {PRIMARY_NAV.map((item) => <NavLink key={item.to} item={item} />)}
      </nav>

      <div className="border-t p-1.5">
        <SeedMenu />
      </div>
    </aside>
  );
}

function NavLink({ item }: { item: NavItem }) {
  const match = useMatchRoute();
  const active = Boolean(match({ to: item.to, fuzzy: item.fuzzy ?? false }));
  const Icon = item.icon;
  return (
    <Link
      to={item.to}
      className={cn(
        'group flex h-7 items-center gap-2 rounded px-2 text-[13px] transition-colors',
        active
          ? 'bg-accent font-medium text-foreground'
          : 'text-muted-foreground hover:bg-accent/60 hover:text-foreground',
      )}
    >
      <Icon className={cn('h-3.5 w-3.5', active ? 'text-foreground' : 'text-muted-foreground group-hover:text-foreground')} />
      <span>{item.label}</span>
    </Link>
  );
}

/**
 * Sticky page header for child routes. Renders consistent title + actions.
 */
export function PageHeader({
  title,
  description,
  actions,
  children,
}: {
  title: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
  children?: React.ReactNode;
}) {
  return (
    <header className="sticky top-0 z-20 flex flex-col gap-1 border-b bg-background/95 px-4 py-2 backdrop-blur supports-[backdrop-filter]:bg-background/80">
      <div className="flex min-h-[28px] items-center justify-between gap-4">
        <div className="min-w-0">
          <h1 className="truncate text-[14px] font-semibold leading-tight">{title}</h1>
          {description && <p className="mt-0.5 text-xs text-muted-foreground">{description}</p>}
        </div>
        {actions && <div className="flex items-center gap-1.5">{actions}</div>}
      </div>
      {children}
    </header>
  );
}

/**
 * Standard scrollable page body.
 */
export function PageBody({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn('flex-1 overflow-y-auto px-4 py-3', className)}>
      {children}
    </div>
  );
}
