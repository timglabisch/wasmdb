import type { ReactNode } from 'react';

export type ScenarioCategory =
  | 'reactivity'
  | 'filters'
  | 'joins'
  | 'aggregates'
  | 'ordering'
  | 'batching'
  | 'lifecycle';

export interface Subscription {
  component: string;
  sql: string;
  note?: string;
  /**
   * Executable substitute for `sql` when generating "→ open in playground"
   * deep-links. Use this when the displayed `sql` contains placeholders
   * (e.g. `<room-id>`) that wouldn't actually run. If absent, the displayed
   * `sql` is used directly.
   */
  playgroundSql?: string;
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
  /**
   * Drives the primary "Open in playground" button at the top of the
   * scenario layout. If absent, the button still appears but with no SQL
   * or table — just `from=<id>`.
   */
  playgroundFocus?: { table?: string; sql?: string };
}

export const CATEGORY_LABEL: Record<ScenarioCategory, string> = {
  reactivity: 'Reactivity — what ticks, what stays quiet',
  filters:    'Filters & predicates — WHERE boundaries',
  joins:      'Joins & cross-table queries',
  aggregates: 'Aggregates — COUNT / SUM / EXISTS',
  ordering:   'Ordering — ORDER BY behavior',
  batching:   'Batching & bulk writes',
  lifecycle:  'Lifecycle & hooks — teardown, rebind, peek',
};

export const CATEGORY_ORDER: ScenarioCategory[] = [
  'reactivity',
  'filters',
  'joins',
  'aggregates',
  'ordering',
  'batching',
  'lifecycle',
];
