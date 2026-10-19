import { cellReplayEventLabel } from "@t3tools/provider-cell/testing";

import type { OrchestratorFixtureInput } from "../shared.ts";

export const CELL_WAKE_PROMPT =
  "Delegate a review to a child cell. Reply with exactly: STARTED. When the child reports, reply with exactly: WAKE_DONE";
export const CELL_WAKE_FOLLOW_UP_PROMPT = "Respond with exactly: USER_REPLY";

/**
 * The child's completion is the third `task` event: the cell announces the
 * task as `starting` before the child is told and as `running` once the child
 * accepted its prompt, both inside the delegating run.
 */
const CELL_WAKE_RESULT_LABEL = cellReplayEventLabel("task", 3);

/**
 * The child finishes after the delegating turn settled, so the cell wakes on
 * its own. The wake reaches the thread as a continuation that the orchestrator
 * dispatches; the gate holds the child's result until the first run is idle.
 */
export function cellWakeInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: CELL_WAKE_PROMPT },
      { type: "release_replay_gate", label: CELL_WAKE_RESULT_LABEL },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
      { type: "message", text: CELL_WAKE_FOLLOW_UP_PROMPT },
    ],
  };
}
