import { assert, describe, it } from "@effect/vitest";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";

import { CellSettings } from "../settings.ts";
import {
  startFakeCellServer,
  type FakeCellServer,
  type FakeCellScript,
} from "../testing/fakeCellServer.ts";
import { makeReplayCellTransport } from "../testing/replayTransport.ts";
import { makeCellAdapterV2, type CellAdapterV2Options } from "./adapter.ts";
import { cellProjectName } from "./protocol.ts";
import { checkCellProviderStatus } from "./status.ts";
import { makeHttpCellTransport, type CellTransport } from "./transport.ts";

const INSTANCE_ID = ProviderInstanceId.make("cell_test");
const THREAD_ID = ThreadId.make("thread-cell-test");
const PROJECT = cellProjectName("t3-", THREAD_ID);
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});
const modelSelection = { instanceId: INSTANCE_ID, model: "gpt-6-astra" };
const decodeCellSettings = Schema.decodeUnknownSync(CellSettings);
const settingsFor = (baseUrl: string, token = "secret-token") =>
  decodeCellSettings({ enabled: true, baseUrl, token });

// Provided around each whole test: the undici dispatcher lives in the layer's scope.
const liveLayer = Layer.mergeAll(
  IdAllocator.layer,
  NodeHttpClient.layerUndici,
  NodeSocket.layerWebSocketConstructorWS,
);
const liveTransport = makeHttpCellTransport;

const fakeCell = (script?: FakeCellScript, configured?: boolean) =>
  Effect.acquireRelease(
    Effect.promise(() =>
      startFakeCellServer({
        token: "secret-token",
        ...(script ? { script } : {}),
        ...(configured === undefined ? {} : { configured }),
      }),
    ),
    (fake: FakeCellServer) => Effect.promise(() => fake.close()),
  );

const makeHarness = Effect.fnUntraced(function* (
  transport: CellTransport,
  baseUrl: string,
  overrides: Pick<CellAdapterV2Options, "continuationRequests"> = {},
) {
  const adapter = yield* makeCellAdapterV2({
    instanceId: INSTANCE_ID,
    settings: settingsFor(baseUrl),
    transport,
    requestTimeoutMs: 10_000,
    ...overrides,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: ProviderSessionId.make("session-cell"),
    modelSelection,
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const allEvents: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        allEvents.push(event);
        yield* Queue.offer(emitted, event);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection,
    runtimePolicy,
  });
  const takeEvent = Effect.fnUntraced(function* <
    T extends ProviderAdapter.ProviderAdapterV2Event["type"],
  >(
    type: T,
    predicate: (
      event: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>,
    ) => boolean = () => true,
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (
        event.type === type &&
        predicate(event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>)
      ) {
        return event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>;
      }
    }
  });
  return { runtime, providerThread, takeEvent, allEvents };
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  text: string,
  ordinal = 1,
  origin: { createdBy: "user" | "agent"; creationSource: "web" | "provider" } = {
    createdBy: "user",
    creationSource: "web",
  },
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD_ID,
    projectId: ProjectId.make("project-cell-test"),
    title: "Cell test",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: INSTANCE_ID,
    modelSelection,
    runtimeMode: runtimePolicy.runtimeMode,
    interactionMode: runtimePolicy.interactionMode,
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return {
    appThread,
    threadId: THREAD_ID,
    runId: RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: NodeId.make(`root-${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message-${ordinal}`),
      text,
      attachments: [],
      ...origin,
    },
    modelSelection,
    runtimePolicy,
  };
});

/** The running turn's id was announced before the items the tests wait for. */
const activeTurnId = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) => {
  const turn = events.find((event) => event.type === "provider_turn.updated");
  if (turn?.type !== "provider_turn.updated") throw new Error("no provider turn was announced");
  return turn.providerTurn.id;
};

const itemsOfType = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>, type: string) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === type ? [event.turnItem] : [],
  );

describe("CellAdapterV2 over the fake cell", () => {
  it.live("maps shell, patch, tool and assistant events to turn items", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell(() => [
        { type: "shell", command: "ls", output: "a\nb\n", exitCode: 0 },
        { type: "patch", operation: "Add", path: "src/x.ts" },
        {
          type: "tool",
          name: "task_status",
          arguments: { taskId: "t1" },
          output: '{"taskId":"t1","status":"completed"}',
        },
        { type: "assistant", text: "all done" },
      ]);
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      assert.equal(harness.providerThread.nativeThreadRef?.nativeId, PROJECT);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Do the thing"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(
        terminal.status,
        "completed",
        terminal.status === "failed" ? terminal.failure.message : undefined,
      );
      assert.deepEqual(fake.prompts(PROJECT), ["Do the thing"]);

      const shells = itemsOfType(harness.allEvents, "command_execution");
      assert.deepEqual(
        shells.map((item) => item.status),
        ["running", "completed"],
      );
      const shell = shells.at(-1);
      assert.isTrue(shell?.type === "command_execution");
      if (shell?.type !== "command_execution") return;
      assert.equal(shell.input, "ls");
      assert.equal(shell.output, "a\nb\n");
      assert.equal(shell.exitCode, 0);
      const patch = itemsOfType(harness.allEvents, "file_change").at(-1);
      assert.isTrue(patch?.type === "file_change" && patch.fileName === "src/x.ts");
      const tool = itemsOfType(harness.allEvents, "dynamic_tool").at(-1);
      assert.isTrue(tool?.type === "dynamic_tool");
      if (tool?.type !== "dynamic_tool") return;
      assert.equal(tool.toolName, "task_status");
      assert.deepEqual(tool.output, { taskId: "t1", status: "completed" });
      const assistant = itemsOfType(harness.allEvents, "assistant_message").at(-1);
      assert.isTrue(assistant?.type === "assistant_message" && assistant.text === "all done");
      const ordinals = harness.allEvents.flatMap((event) =>
        event.type === "turn_item.updated" ? [event.turnItem.ordinal] : [],
      );
      assert.deepEqual(
        ordinals,
        [...ordinals].toSorted((a, b) => a - b),
      );
      const thread = harness.allEvents.findLast(
        (event) => event.type === "provider_thread.updated",
      );
      assert.isTrue(
        thread?.type === "provider_thread.updated" && thread.providerThread.status === "idle",
      );
      assert.isTrue(
        thread?.type === "provider_thread.updated" &&
          thread.providerThread.nativeConversationHeadRef?.nativeId === `${PROJECT}:turn:1`,
      );
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("Stop posts cancel and settles only on the cell's done", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell(() => [
        { type: "shell_until_cancel", command: "sleep 30" },
        { type: "assistant", text: "should not finish" },
      ]);
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Run `sleep 30`"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: activeTurnId(harness.allEvents),
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "interrupted");
      const shell = itemsOfType(harness.allEvents, "command_execution").at(-1);
      assert.equal(shell?.status, "interrupted");
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("reconnects when the cell closes the socket and keeps a running turn", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell(() => [{ type: "shell_until_cancel", command: "sleep 30" }]);
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Run `sleep 30`"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      const reconnected = fake.onceConnected(PROJECT);
      fake.dropSockets();
      yield* Effect.promise(() => reconnected);
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: activeTurnId(harness.allEvents),
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("fails a turn whose end it missed while disconnected", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell(() => [{ type: "shell_until_cancel", command: "sleep 30" }]);
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Run `sleep 30`"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      fake.rejectSockets(true);
      fake.dropSockets();
      fake.cancelRun(PROJECT);
      const reconnected = fake.onceConnected(PROJECT);
      fake.rejectSockets(false);
      yield* Effect.promise(() => reconnected);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(terminal.failure?.class, "transport_error");
      const thread = harness.allEvents.findLast(
        (event) => event.type === "provider_thread.updated",
      );
      assert.isTrue(
        thread?.type === "provider_thread.updated" && thread.providerThread.status === "idle",
      );
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("reports the tenant's model and key through the status probe", () =>
    Effect.gen(function* () {
      const transport = yield* liveTransport;
      const ready = yield* fakeCell();
      const snapshot = yield* checkCellProviderStatus(settingsFor(ready.baseUrl), transport);
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.version, "0.2.0");
      assert.deepEqual(
        snapshot.models.map((model) => [model.slug, model.isDefault]),
        [["gpt-6-astra", true]],
      );
      assert.deepEqual(snapshot.supportedRuntimeModes, ["full-access"]);
      const unconfigured = yield* fakeCell(undefined, false);
      const warning = yield* checkCellProviderStatus(settingsFor(unconfigured.baseUrl), transport);
      assert.equal(warning.status, "warning");
      const rejected = yield* checkCellProviderStatus(
        settingsFor(ready.baseUrl, "wrong"),
        transport,
      );
      assert.equal(rejected.status, "error");
      assert.include(rejected.message ?? "", "access token");
      const unreachable = yield* checkCellProviderStatus(
        settingsFor("http://127.0.0.1:9"),
        transport,
      );
      assert.equal(unreachable.status, "error");
      assert.equal(unreachable.installed, false);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );
});

describe("CellAdapterV2 over a scripted socket", () => {
  const entry = (event: Record<string, unknown>): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "ws_event", event },
  });

  it.effect("turns a wake while idle into a continuation that adopts the buffered events", () =>
    Effect.gen(function* () {
      const offered =
        yield* Deferred.make<ProviderContinuationRequests.ProviderContinuationRequest>();
      const controller = makeReplayCellTransport({
        scenario: "wake",
        entries: [
          { type: "expect_outbound", frame: { kind: "ws_connect", path: "/cell/<project>/ws" } },
          entry({ type: "state", running: false, parent: null, depth: 0 }),
          entry({ type: "tasks", tasks: [] }),
          entry({
            type: "task",
            task: {
              taskId: "ab12",
              title: "Review",
              status: "completed",
              summary: "Fine",
              changes: [],
            },
          }),
          entry({ type: "wake", text: '[Delegated task ab12 "Review" completed]\nFine' }),
          entry({ type: "assistant", text: "The review passed; nothing to merge." }),
          entry({ type: "done" }),
        ],
      });
      const harness = yield* makeHarness(controller.transport, "https://cell.test", {
        continuationRequests: {
          offer: (request) => Deferred.succeed(offered, request).pipe(Effect.asVoid),
        },
      });
      // The socket's first frames are processed after ensureThread returns.
      const request = yield* Deferred.await(offered);
      assert.equal(request.threadId, THREAD_ID);
      assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
      yield* harness.runtime.startTurn(
        yield* turnInput(harness.providerThread, "", 1, {
          createdBy: "agent",
          creationSource: "provider",
        }),
      );
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed");
      const assistant = itemsOfType(harness.allEvents, "assistant_message").at(-1);
      assert.isTrue(
        assistant?.type === "assistant_message" &&
          assistant.text === "The review passed; nothing to merge.",
      );
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(IdAllocator.layer))),
  );

  it.effect("rejects attachments as a validation failure instead of dropping them", () =>
    Effect.gen(function* () {
      const controller = makeReplayCellTransport({
        scenario: "attachments",
        entries: [
          { type: "expect_outbound", frame: { kind: "ws_connect", path: "/cell/<project>/ws" } },
          entry({ type: "state", running: false, parent: null, depth: 0 }),
          entry({ type: "tasks", tasks: [] }),
        ],
      });
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const input = yield* turnInput(harness.providerThread, "See the screenshot");
      yield* harness.runtime.startTurn({
        ...input,
        message: {
          ...input.message,
          attachments: [
            {
              id: "att-1",
              name: "shot.png",
              mimeType: "image/png",
              size: 12,
              kind: "image",
            } as never,
          ],
        },
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(terminal.failure?.class, "validation_error");
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
  );
});
