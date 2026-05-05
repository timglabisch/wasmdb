import type { ReactNode } from 'react';

export type ScenarioCategory = 'counters' | 'users' | 'rooms' | 'messages' | 'joins' | 'hooks';

export interface Subscription {
  component: string;
  sql: string;
  note?: string;
}

export interface Scenario {
  id: string;
  category: ScenarioCategory;
  title: string;
  summary: string;
  expectations: string[];
  Body: () => ReactNode;
  /**
   * Patterns of render-log entries that *should* tick on the scenario's
   * driving action. Used by the live diff panel to mark entries with ✓.
   * Pattern syntax: exact | "Prefix*" | "*Suffix" | "*Substring*".
   */
  shouldRender?: string[];
  /**
   * Patterns that MUST stay quiet. If they appear in the diff, the live
   * panel marks them ✗ — visible "you broke isolation" feedback.
   */
  shouldStayQuiet?: string[];
  /**
   * SQL each visible component subscribes to. Optional; rendered as a
   * "what's wired up" reference panel. Helps users see the cause behind
   * the observed reactivity.
   */
  subscriptions?: Subscription[];
}

export const CATEGORY_LABEL: Record<ScenarioCategory, string> = {
  counters: 'Counters',
  users: 'Users',
  rooms: 'Rooms',
  messages: 'Messages',
  joins: 'Joins',
  hooks: 'Hook probes',
};
