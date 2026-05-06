import { UserBadge } from '../../components/UserBadge';
import { SEED } from '../../seed';

export const StandaloneBadges = () => (
  <section className="panel">
    <h2>Standalone user badges</h2>
    <div className="row">
      <UserBadge id={SEED.users.A} ctx="standalone" />
      <UserBadge id={SEED.users.B} ctx="standalone" />
      <UserBadge id={SEED.users.C} ctx="standalone" />
    </div>
  </section>
);
