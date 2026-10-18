import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  cellApiUrl,
  cellProjectName,
  cellSocketUrl,
  classifyCellDone,
  readCellState,
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

describe("readCellState", () => {
  const state = (over: Record<string, unknown>) =>
    JSON.stringify({
      protocol: 3,
      model: "gpt-6-astra",
      running: false,
      runId: null,
      seq: 12,
      configured: true,
      version: "0.4.0",
      ...over,
    });

  it.effect("reads a level-3 state", () =>
    Effect.gen(function* () {
      const read = yield* readCellState(state({}));
      assert.equal(read.protocol, 3);
      assert.equal(read.seq, 12);
      assert.isNull(read.runId);
    }),
  );

  it.effect("refuses a tenant below level 3 by the level it states", () =>
    Effect.gen(function* () {
      const stated = yield* readCellState(state({ protocol: 2 })).pipe(Effect.flip);
      assert.equal(stated._tag, "CellProtocolError");
      assert.include(stated.message, "cell protocol 2");
      assert.include(stated.message, "needs 3");
      const unstated = yield* readCellState(
        JSON.stringify({ model: "gpt-6-astra", running: false, configured: true }),
      ).pipe(Effect.flip);
      assert.equal(unstated._tag, "CellProtocolError");
      assert.include(unstated.message, "stating no level");
      assert.include(unstated.message, "needs 3");
    }),
  );

  it.effect("tells a body that is not a cell state from an old one", () =>
    Effect.gen(function* () {
      assert.equal((yield* readCellState("<html>").pipe(Effect.flip))._tag, "SchemaError");
      assert.equal(
        (yield* readCellState(state({ model: 7 })).pipe(Effect.flip))._tag,
        "SchemaError",
      );
    }),
  );
});

describe("prompt responses", () => {
  it("reads the run id and opening number from a 202, and the running id from a 409", () => {
    expect(readPromptAccepted('{"runId":"run-7","seq":41}')).toEqual({ runId: "run-7", seq: 41 });
    expect(readPromptAccepted('{"runId":"run-7"}')).toBeUndefined();
    expect(readPromptAccepted("")).toBeUndefined();
    expect(readPromptRejectedRunId('{"runId":"run-3"}')).toBe("run-3");
    expect(readPromptRejectedRunId("The prompt was not started")).toBeUndefined();
  });
});

describe("classifyCellDone", () => {
  it("classifies a done by its error code and keeps the reset time", () => {
    assert.deepEqual(classifyCellDone(undefined), { kind: "completed" });
    assert.deepEqual(classifyCellDone(null), { kind: "completed" });
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
    assert.equal(limited.code, "usage_limit");
    assert.equal(limited.resetAt, "2026-10-19T00:00:00.000Z");
    assert.include(limited.message, "429");
    const transport = classifyCellDone({ code: "transport_error", message: "fetch failed" });
    assert.isTrue(transport.kind === "failed" && transport.failureClass === "transport_error");
    const tool = classifyCellDone({ code: "tool_error", message: "tsc exited 2" });
    assert.isTrue(tool.kind === "failed" && tool.failureClass === "provider_error");
  });
});
