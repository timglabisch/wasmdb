import { counterScenarios } from './counter-scenarios';
import { hookScenarios } from './hook-scenarios';
import { joinScenarios } from './join-scenarios';
import { messageScenarios } from './message-scenarios';
import { roomScenarios } from './room-scenarios';
import { userScenarios } from './user-scenarios';
import type { Scenario, ScenarioCategory } from './types';

export const SCENARIOS: Scenario[] = [
  ...counterScenarios,
  ...userScenarios,
  ...roomScenarios,
  ...messageScenarios,
  ...joinScenarios,
  ...hookScenarios,
];

export const SCENARIOS_BY_ID: Record<string, Scenario> = Object.fromEntries(
  SCENARIOS.map((s) => [s.id, s]),
);

export function scenariosByCategory(): Record<ScenarioCategory, Scenario[]> {
  const out: Record<ScenarioCategory, Scenario[]> = {
    counters: [],
    users: [],
    rooms: [],
    messages: [],
    joins: [],
    hooks: [],
  };
  for (const s of SCENARIOS) out[s.category].push(s);
  return out;
}
