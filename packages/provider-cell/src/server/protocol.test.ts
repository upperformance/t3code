import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  CellEvent,
  cellApiUrl,
  cellProjectName,
  cellSocketUrl,
  classifyCellDone,
  isCellRunEvent,
  isKnownCellEventType,
  readCellRefusal,
  readCellState,
  readPromptAccepted,
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

  it.effect("reads a level-3 state, with the request id of the run in progress", () =>
    Effect.gen(function* () {
      const read = yield* readCellState(state({}));
      assert.equal(read.protocol, 3);
      assert.equal(read.seq, 12);
      assert.isNull(read.runId);
      assert.isUndefined(read.requestId);
      const running = yield* readCellState(
        state({ running: true, runId: "run-2", requestId: "req-2" }),
      );
      assert.equal(running.requestId, "req-2");
      assert.isNull((yield* readCellState(state({ requestId: null }))).requestId);
    }),
  );

  it.effect("refuses a state whose number is not a non-negative integer", () =>
    Effect.gen(function* () {
      assert.equal(
        (yield* readCellState(state({ seq: -1 })).pipe(Effect.flip))._tag,
        "SchemaError",
      );
      assert.equal(
        (yield* readCellState(state({ seq: 2.5 })).pipe(Effect.flip))._tag,
        "SchemaError",
      );
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

describe("prompt and cancel answers", () => {
  it("reads the run id and opening number from a 202, and a refusal's run and reason", () => {
    expect(readPromptAccepted('{"runId":"run-7","seq":41}')).toEqual({ runId: "run-7", seq: 41 });
    expect(readPromptAccepted('{"runId":"run-7"}')).toBeUndefined();
    expect(readPromptAccepted('{"runId":"run-7","seq":-1}')).toBeUndefined();
    expect(readPromptAccepted('{"runId":"run-7","seq":1.5}')).toBeUndefined();
    expect(readPromptAccepted("")).toBeUndefined();
    expect(readCellRefusal('{"runId":"run-3"}')).toEqual({ runId: "run-3", error: null });
    expect(readCellRefusal('{"runId":null,"error":"The prompt is empty"}')).toEqual({
      runId: null,
      error: "The prompt is empty",
    });
    // A proxy's error page is nobody's refusal.
    expect(readCellRefusal("The prompt was not started")).toBeUndefined();
    expect(readCellRefusal("<html>502 Bad Gateway</html>")).toBeUndefined();
  });
});

describe("CellEvent", () => {
  const decode = Schema.decodeUnknownOption(CellEvent);

  it("numbers every event with a non-negative integer", () => {
    assert.isTrue(Option.isSome(decode({ type: "done", runId: "r", seq: 0 })));
    assert.isTrue(Option.isNone(decode({ type: "done", runId: "r", seq: -0.5 })));
    assert.isTrue(Option.isNone(decode({ type: "done", runId: "r", seq: -1 })));
    assert.isTrue(Option.isNone(decode({ type: "assistant", text: "x", runId: "r", seq: 2.5 })));
    assert.isTrue(Option.isNone(decode({ type: "tasks", tasks: [], seq: "3" })));
  });

  it("tells a run's error from a standalone one, carries the opener's request id, and models no replies", () => {
    const scoped = decode({ type: "error", text: "apply_patch failed", runId: "r", seq: 3 });
    assert.isTrue(Option.isSome(scoped) && isCellRunEvent(scoped.value));
    const standalone = decode({ type: "error", text: "Could not report to the parent", seq: 4 });
    assert.isTrue(Option.isSome(standalone) && !isCellRunEvent(standalone.value));
    const opener = decode({
      type: "user",
      text: "Hi",
      runId: "r",
      requestId: "req-1",
      origin: "user",
      seq: 5,
    });
    assert.isTrue(
      Option.isSome(opener) && opener.value.type === "user" && opener.value.requestId === "req-1",
    );
    // What the cell answers a client's own socket message is unnumbered and not an event.
    assert.isTrue(Option.isNone(decode({ type: "error", text: "A run is already in progress" })));
    assert.isFalse(isKnownCellEventType("exec"));
    assert.isTrue(isKnownCellEventType("error"));
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
