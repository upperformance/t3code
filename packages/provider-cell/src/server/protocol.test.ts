import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  cellApiUrl,
  cellProjectName,
  cellSocketUrl,
  classifyCellDone,
  readPromptAccepted,
  readPromptRejectedRunId,
} from "./protocol.ts";

describe("cellProjectName", () => {
  it.effect("hashes the whole thread id into a fixed-width name", () =>
    Effect.gen(function* () {
      const threadId =
        "thread:fixture:simple:project:project:fixture:simple:1f0c4ad2-2b8c-4c2e-a1c5-2f3e1e0f9d7a:6a9c1b2e-3d4f-4a5b-8c6d-7e8f9a0b1c2d";
      const name = yield* cellProjectName("t3-", threadId);
      expect(name).toMatch(/^t3-[0-9a-f]{20}$/);
      // Two ids that share a tail must not share a cell.
      const sibling = yield* cellProjectName("t3-", `other:${threadId.slice(-40)}`);
      expect(sibling).not.toBe(name);
    }),
  );

  it.effect("is deterministic and distinct per thread", () =>
    Effect.gen(function* () {
      expect(yield* cellProjectName("t3-", "thread:a")).toBe(
        yield* cellProjectName("t3-", "thread:a"),
      );
      expect(yield* cellProjectName("t3-", "thread:a")).not.toBe(
        yield* cellProjectName("t3-", "thread:b"),
      );
    }),
  );
});

describe("cell urls", () => {
  it("builds the API and socket urls under the project route", () => {
    expect(cellApiUrl("https://acme.vim.app/", "t3-x", "/prompt")).toBe(
      "https://acme.vim.app/cell/t3-x/prompt",
    );
    expect(cellSocketUrl("https://acme.vim.app", "t3-x", "t3-s1")).toBe(
      "wss://acme.vim.app/cell/t3-x/ws?client=t3-s1",
    );
    expect(cellSocketUrl("http://127.0.0.1:8787", "t3-x", "c")).toBe(
      "ws://127.0.0.1:8787/cell/t3-x/ws?client=c",
    );
  });
});

describe("prompt responses", () => {
  it("reads the run id from a 202 and tolerates the empty body of an older tenant", () => {
    expect(readPromptAccepted('{"runId":"run-7","seq":41}')).toEqual({ runId: "run-7", seq: 41 });
    expect(readPromptAccepted("")).toBeUndefined();
    expect(readPromptRejectedRunId('{"runId":"run-3"}')).toBe("run-3");
    expect(readPromptRejectedRunId("An agent run is already in progress")).toBeUndefined();
  });
});

describe("classifyCellDone", () => {
  it("classifies a structured error by its code and keeps the reset time", () => {
    assert.deepEqual(classifyCellDone(undefined), { kind: "completed" });
    assert.deepEqual(classifyCellDone({ code: "cancelled", message: "Cancelled" }), {
      kind: "cancelled",
    });
    const limited = classifyCellDone({
      code: "usage_limit",
      status: 429,
      message: "insufficient_quota",
      resetAt: "2026-10-19T00:00:00.000Z",
    });
    assert.equal(limited.kind, "failed");
    if (limited.kind !== "failed") return;
    assert.equal(limited.failureClass, "usage_limit");
    assert.equal(limited.resetAt, "2026-10-19T00:00:00.000Z");
    assert.include(limited.message, "429");
    const transport = classifyCellDone({ code: "transport_error", message: "fetch failed" });
    assert.isTrue(transport.kind === "failed" && transport.failureClass === "transport_error");
    const tool = classifyCellDone({ code: "tool_error", message: "tsc exited 2" });
    assert.isTrue(tool.kind === "failed" && tool.failureClass === "provider_error");
  });

  it("falls back to the words of a plain-string error", () => {
    assert.deepEqual(classifyCellDone("Cancelled"), { kind: "cancelled" });
    const quota = classifyCellDone("429 insufficient_quota: You exceeded your current quota");
    assert.isTrue(quota.kind === "failed" && quota.failureClass === "usage_limit");
    const other = classifyCellDone("model refused");
    assert.isTrue(other.kind === "failed" && other.failureClass === "provider_error");
  });
});
