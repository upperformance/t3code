import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertConversationMessageRoles,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  MULTI_TURN_FIRST_PROMPT,
  MULTI_TURN_SECOND_PROMPT,
  projectionFor,
} from "../shared.ts";

/** Both turns post to the same cell project: one provider thread, two prompts in order. */
export function assertCellMultiTurnOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertRunOrdinals(projection, [1, 2]);
  assertConversationMessageRoles(projection, ["user", "assistant", "user", "assistant"]);
  assertUserMessagesInclude(projection, [MULTI_TURN_FIRST_PROMPT, MULTI_TURN_SECOND_PROMPT]);
  assertAssistantTextIncludes(projection, "first fixture turn complete");
  assertAssistantTextIncludes(projection, "second fixture turn complete");
  assert.lengthOf(projection.providerThreads, 1);
  const prompts = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" &&
    typeof entry.frame === "object" &&
    entry.frame !== null &&
    Reflect.get(entry.frame, "kind") === "http" &&
    Reflect.get(entry.frame, "method") === "POST" &&
    String(Reflect.get(entry.frame, "path")).endsWith("/prompt")
      ? [Reflect.get(entry.frame, "path")]
      : [],
  );
  assert.lengthOf(prompts, 2);
  assert.equal(prompts[0], prompts[1], "both turns must address the same cell project");
  assert.lengthOf(
    transcript.entries.filter(
      (entry) =>
        entry.type === "expect_outbound" &&
        typeof entry.frame === "object" &&
        entry.frame !== null &&
        Reflect.get(entry.frame, "kind") === "ws_connect",
    ),
    1,
    "the socket opened once and stayed open across turns",
  );
}
