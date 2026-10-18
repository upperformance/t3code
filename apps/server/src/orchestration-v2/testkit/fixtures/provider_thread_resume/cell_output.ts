import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "../shared.ts";

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * The idle release closes the first session and its socket. The second
 * message opens a new session that attaches the same cell project, where the
 * conversation lives, so the cell still knows the first answer.
 */
export function assertCellProviderThreadResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sockets = transcript.entries.filter(
    (entry) => entry.type === "expect_outbound" && field(entry.frame, "kind") === "ws_connect",
  );
  assert.lengthOf(sockets, 2, "the idle release must close the first socket");
  const prompts = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" &&
    field(entry.frame, "method") === "POST" &&
    String(field(entry.frame, "path")).endsWith("/prompt")
      ? [String(field(entry.frame, "path"))]
      : [],
  );
  assert.lengthOf(prompts, 2);
  assert.equal(prompts[0], prompts[1], "both sessions must address the same cell project");

  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assert.lengthOf(projection.providerThreads, 1, "resume must keep one provider thread");
  assert.match(
    projection.providerThreads[0]?.nativeThreadRef?.nativeId ?? "",
    /^t3-[0-9a-f]{20}$/,
    "the cell project name is the hashed thread id",
  );
  const answers = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.lengthOf(answers, 2);
  assert.include(answers[1], FIRST_FINAL, "the cell must remember the first answer");
  assert.include(answers[1], SECOND_FINAL);
}
