import {
  createRootRoute, createRoute, createRouter,
  createHashHistory, Outlet, redirect,
} from '@tanstack/react-router';
import AppShell from '@/shared/layout/AppShell';
import DashboardTab from '@/features/dashboard/DashboardTab';
import CustomersTab from '@/features/customers/CustomersTab';
import CustomerDetailRoute from '@/features/customers/CustomerDetailRoute';
import ProductsTab from '@/features/products/ProductsTab';
import ProductDetailRoute from '@/features/products/ProductDetailRoute';
import RecurringTab from '@/features/recurring/RecurringTab';
import RecurringDetailRoute from '@/features/recurring/RecurringDetailRoute';
import ActivityTab from '@/features/activity/ActivityTab';
import InvoicesTab from '@/features/invoice/routes/InvoicesTab';
import InvoiceDetail from '@/features/invoice/routes/InvoiceDetail';

const rootRoute = createRootRoute({ component: AppShell });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  beforeLoad: () => { throw redirect({ to: '/dashboard' }); },
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/dashboard',
  component: DashboardTab,
});

const invoicesLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/invoices',
  component: () => <Outlet />,
});

const invoicesIndexRoute = createRoute({
  getParentRoute: () => invoicesLayoutRoute,
  path: '/',
  component: InvoicesTab,
});

const invoiceDetailRoute = createRoute({
  getParentRoute: () => invoicesLayoutRoute,
  path: '$invoiceId',
  parseParams: (raw) => ({ invoiceId: Number(raw.invoiceId) }),
  stringifyParams: ({ invoiceId }) => ({ invoiceId: String(invoiceId) }),
  component: InvoiceDetail,
});

const customersLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/customers',
  component: () => <Outlet />,
});

const customersIndexRoute = createRoute({
  getParentRoute: () => customersLayoutRoute,
  path: '/',
  component: CustomersTab,
});

const customerDetailRoute = createRoute({
  getParentRoute: () => customersLayoutRoute,
  path: '$customerId',
  parseParams: (raw) => ({ customerId: Number(raw.customerId) }),
  stringifyParams: ({ customerId }) => ({ customerId: String(customerId) }),
  component: CustomerDetailRoute,
});

const productsLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/products',
  component: () => <Outlet />,
});

const productsIndexRoute = createRoute({
  getParentRoute: () => productsLayoutRoute,
  path: '/',
  component: ProductsTab,
});

const productDetailRoute = createRoute({
  getParentRoute: () => productsLayoutRoute,
  path: '$productId',
  parseParams: (raw) => ({ productId: Number(raw.productId) }),
  stringifyParams: ({ productId }) => ({ productId: String(productId) }),
  component: ProductDetailRoute,
});

const recurringLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/recurring',
  component: () => <Outlet />,
});

const recurringIndexRoute = createRoute({
  getParentRoute: () => recurringLayoutRoute,
  path: '/',
  component: RecurringTab,
});

const recurringDetailRoute = createRoute({
  getParentRoute: () => recurringLayoutRoute,
  path: '$recurringId',
  parseParams: (raw) => ({ recurringId: Number(raw.recurringId) }),
  stringifyParams: ({ recurringId }) => ({ recurringId: String(recurringId) }),
  component: RecurringDetailRoute,
});

const activityRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/activity',
  component: ActivityTab,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  dashboardRoute,
  invoicesLayoutRoute.addChildren([invoicesIndexRoute, invoiceDetailRoute]),
  customersLayoutRoute.addChildren([customersIndexRoute, customerDetailRoute]),
  productsLayoutRoute.addChildren([productsIndexRoute, productDetailRoute]),
  recurringLayoutRoute.addChildren([recurringIndexRoute, recurringDetailRoute]),
  activityRoute,
]);

export const router = createRouter({
  routeTree,
  history: createHashHistory(),
  defaultPreload: false,
});

declare module '@tanstack/react-router' {
  interface Register { router: typeof router; }
}

export { Outlet };
