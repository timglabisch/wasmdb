import { useQuery } from './sync.ts';

export default function StatsBar() {
  const userCount = useQuery(
    "SELECT reactive(users.id), COUNT(users.id) FROM users",
    ([_r, c]) => c as number,
  );
  const orderCount = useQuery(
    "SELECT reactive(orders.id), COUNT(orders.id) FROM orders",
    ([_r, c]) => c as number,
  );
  const totalRevenue = useQuery(
    "SELECT reactive(orders.id), SUM(orders.amount) FROM orders",
    ([_r, s]) => (s as number) ?? 0,
  );
  const maxOrder = useQuery(
    "SELECT reactive(orders.id), MAX(orders.amount) FROM orders",
    ([_r, m]) => (m as number) ?? 0,
  );

  const uc = userCount[0] ?? 0;
  const oc = orderCount[0] ?? 0;
  const rev = totalRevenue[0] ?? 0;
  const mx = maxOrder[0] ?? 0;

  return (
    <div className="stats-bar">
      <div className="stat">
        <span className="stat-label">Users</span>
        <span className="stat-value">{uc}</span>
      </div>
      <div className="stat">
        <span className="stat-label">Orders</span>
        <span className="stat-value">{oc}</span>
      </div>
      <div className="stat">
        <span className="stat-label">Revenue</span>
        <span className="stat-value">${(rev / 100).toFixed(2)}</span>
      </div>
      <div className="stat">
        <span className="stat-label">Max Order</span>
        <span className="stat-value">${(mx / 100).toFixed(2)}</span>
      </div>
    </div>
  );
}
