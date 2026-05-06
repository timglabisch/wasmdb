import { MessageList } from '../../components/MessageList';
import { SEED } from '../../seed';

export const ThreeLists = () => (
  <section className="panel">
    <h2>Messages by room</h2>
    <div className="message-grid">
      <MessageList roomId={SEED.rooms.R1} />
      <MessageList roomId={SEED.rooms.R2} />
      <MessageList roomId={SEED.rooms.R3} />
    </div>
  </section>
);
