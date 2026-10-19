import { assert } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import { CELL_WAKE_FOLLOW_UP_PROMPT, CELL_WAKE_PROMPT } from "./input.ts";

function runAssistantTexts(
  projection: OrchestrationV2ThreadProjection,
  runId: string | undefined,
): ReadonlyArray<string> {
  return projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );
}

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * The wake is a cell-started run: the continuation turn adopts its output
 * without posting a prompt, and the delegated child shows as a subagent that
 * settles when the cell reports it done.
 */
export function assertCellWakeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["completed", "completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [CELL_WAKE_PROMPT, CELL_WAKE_FOLLOW_UP_PROMPT]);

  const [rootRun, wakeRun, userRun] = projection.runs;
  assert.deepEqual(runAssistantTexts(projection, rootRun?.id), ["STARTED"]);
  assert.deepEqual(runAssistantTexts(projection, wakeRun?.id), ["WAKE_DONE"]);
  assert.deepEqual(runAssistantTexts(projection, userRun?.id), ["USER_REPLY"]);

  const creators = projection.runs.map((run) => {
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    return `${message?.createdBy}:${message?.creationSource}`;
  });
  assert.deepEqual(creators, ["user:web", "agent:provider", "user:web"]);
  const wakeMessage = projection.messages.find((message) => message.id === wakeRun?.userMessageId);
  assert.include(wakeMessage?.notification?.summary ?? "", "review");

  const subagents = projection.turnItems.filter((item) => item.type === "subagent");
  assert.lengthOf(subagents, 1, "the delegated child is one subagent row");
  assert.equal(subagents[0]?.runId, rootRun?.id, "the row belongs to the run that delegated");
  assert.equal(subagents[0]?.status, "completed");

  const rosterIndex = result.domainEvents.findIndex(
    (event) =>
      event.type === "provider-thread.updated" &&
      (event.payload.pendingBackgroundTasks?.length ?? 0) > 0,
  );
  const clearedIndex = result.domainEvents.findIndex(
    (event, index) =>
      index > rosterIndex &&
      event.type === "provider-thread.updated" &&
      (event.payload.pendingBackgroundTasks?.length ?? 0) === 0,
  );
  const rosterHistory = result.domainEvents
    .flatMap((event) =>
      event.type === "provider-thread.updated"
        ? [`${event.payload.status}:${event.payload.pendingBackgroundTasks?.length ?? 0}`]
        : [],
    )
    .join(" ");
  assert.isAtLeast(rosterIndex, 0, `the running child must reach the roster (${rosterHistory})`);
  assert.isAbove(
    clearedIndex,
    rosterIndex,
    `the finished child must leave the roster (${rosterHistory})`,
  );
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);

  const prompts = transcript.entries.filter(
    (entry) =>
      entry.type === "expect_outbound" &&
      field(entry.frame, "method") === "POST" &&
      String(field(entry.frame, "path")).endsWith("/prompt"),
  );
  assert.lengthOf(prompts, 2, "the wake run was the cell's own; only the user turns post a prompt");
  assert.lengthOf(
    transcript.entries.filter(
      (entry) => entry.type === "expect_outbound" && field(entry.frame, "kind") === "ws_connect",
    ),
    1,
    "one socket carried all three runs",
  );
}
