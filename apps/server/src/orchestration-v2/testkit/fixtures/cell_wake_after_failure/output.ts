import { assert } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import { CELL_WAKE_AFTER_FAILURE_ERROR, CELL_WAKE_AFTER_FAILURE_PROMPT } from "./input.ts";

function runAssistantTexts(
  projection: OrchestrationV2ThreadProjection,
  runId: string | undefined,
): ReadonlyArray<string> {
  return projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );
}

/**
 * A failed delegating run leaves its child working: the child's row stays
 * running through the failure instead of failing with it, the child's result
 * lands on that row, and the wake it starts runs as a continuation.
 */
export function assertCellWakeAfterFailureOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["failed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [CELL_WAKE_AFTER_FAILURE_PROMPT]);

  const [rootRun, wakeRun] = projection.runs;
  assert.deepEqual(runAssistantTexts(projection, rootRun?.id), ["STARTED"]);
  assert.deepEqual(runAssistantTexts(projection, wakeRun?.id), ["WAKE_DONE"]);
  const failures = projection.turnItems.flatMap((item) =>
    item.type === "error" ? [`${item.runId}:${item.failure.message}`] : [],
  );
  assert.deepEqual(failures, [`${rootRun?.id}:${CELL_WAKE_AFTER_FAILURE_ERROR}`]);

  const wakeMessage = projection.messages.find((message) => message.id === wakeRun?.userMessageId);
  assert.equal(`${wakeMessage?.createdBy}:${wakeMessage?.creationSource}`, "agent:provider");

  const subagents = projection.turnItems.filter((item) => item.type === "subagent");
  assert.lengthOf(subagents, 1, "the delegated child is one subagent row");
  const row = subagents[0];
  assert.equal(row?.runId, rootRun?.id, "the row belongs to the run that delegated");
  assert.equal(row?.status, "completed");
  assert.equal(row?.type === "subagent" ? row.result : null, "The review is done.");
  assert.deepEqual(
    projection.subagents.map((subagent) => `${subagent.runId}:${subagent.status}`),
    [`${rootRun?.id}:completed`],
  );

  // The run's failure did not settle the child: every report of the row
  // between the failure and the child's own end says it is still running.
  const rootFailedIndex = result.domainEvents.findIndex(
    (event) =>
      event.type === "run.updated" &&
      event.payload.id === rootRun?.id &&
      event.payload.status === "failed",
  );
  assert.isAtLeast(rootFailedIndex, 0, "the delegating run fails");
  const rowStatuses = result.domainEvents.flatMap((event) =>
    event.type === "subagent.updated" ? [event.payload.status] : [],
  );
  assert.notInclude(
    rowStatuses,
    "failed",
    `the child's row never fails (${rowStatuses.join(" ")})`,
  );
  const completedIndex = result.domainEvents.findIndex(
    (event) => event.type === "subagent.updated" && event.payload.status === "completed",
  );
  assert.isAbove(completedIndex, rootFailedIndex, "the child's result arrives after the failure");
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
}
