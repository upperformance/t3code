import { cellReplayEventLabel } from "@t3tools/provider-cell/testing";

import type { OrchestratorFixtureInput } from "../shared.ts";

export const CELL_WAKE_AFTER_FAILURE_ERROR = "The model is overloaded";
export const CELL_WAKE_AFTER_FAILURE_PROMPT = `Delegate a review to a child cell. Reply with exactly: STARTED. Then fail with: ${CELL_WAKE_AFTER_FAILURE_ERROR}. When the child reports, reply with exactly: WAKE_DONE`;

/**
 * The child's completion is the third `task` event, as in `cell_wake`: the
 * cell announces the task as `starting` and `running` inside the delegating
 * run, which then fails.
 */
export const CELL_WAKE_AFTER_FAILURE_RESULT_LABEL = cellReplayEventLabel("task", 3);

/**
 * The delegating run fails while its child works on. The gate holds the
 * child's result until that run has settled as failed, so the result and the
 * wake it starts arrive after the failure, as they do when the cell's model
 * errors out after delegating.
 */
export function cellWakeAfterFailureInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: CELL_WAKE_AFTER_FAILURE_PROMPT },
      { type: "await_run_status", targetRunIndex: 1, status: "failed" },
      { type: "release_replay_gate", label: CELL_WAKE_AFTER_FAILURE_RESULT_LABEL },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
    ],
  };
}
