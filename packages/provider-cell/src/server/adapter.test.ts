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
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";

import { CellSettings } from "../settings.ts";
import {
  startFakeCellServer,
  type FakeCellProtocol,
  type FakeCellServer,
  type FakeCellScript,
} from "../testing/fakeCellServer.ts";
import { makeReplayCellTransport } from "../testing/replayTransport.ts";
import { makeCellAdapterV2, type CellAdapterV2Options } from "./adapter.ts";
import { CELL_RUNTIME_MODE_MESSAGE } from "./errors.ts";
import { cellProjectName } from "./protocol.ts";
import { checkCellProviderStatus } from "./status.ts";
import { CellTransportError, makeHttpCellTransport, type CellTransport } from "./transport.ts";

const INSTANCE_ID = ProviderInstanceId.make("cell_test");
const THREAD_ID = ThreadId.make("thread-cell-test");
const SESSION_ID = ProviderSessionId.make("session-cell");
const fullAccess = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});
const supervised = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  ...fullAccess,
  runtimeMode: "approval-required",
});
const defaultModel = { instanceId: INSTANCE_ID, model: "default" };
const decodeCellSettings = Schema.decodeUnknownSync(CellSettings);
const settingsFor = (baseUrl: string, token = "secret-token", enabled = true) =>
  decodeCellSettings({ enabled, baseUrl, token });

// Provided around each whole test: the undici dispatcher lives in the layer's scope.
const liveLayer = Layer.mergeAll(
  IdAllocator.layer,
  NodeHttpClient.layerUndici,
  NodeSocket.layerWebSocketConstructorWS,
);
const liveTransport = makeHttpCellTransport;

const fakeCell = (
  options: {
    readonly script?: FakeCellScript;
    readonly configured?: boolean;
    readonly protocol?: FakeCellProtocol;
    readonly model?: string;
  } = {},
) =>
  Effect.acquireRelease(
    Effect.promise(() =>
      startFakeCellServer({
        token: "secret-token",
        ...(options.script ? { script: options.script } : {}),
        ...(options.configured === undefined ? {} : { configured: options.configured }),
        ...(options.protocol === undefined ? {} : { protocol: options.protocol }),
        ...(options.model === undefined ? {} : { model: options.model }),
      }),
    ),
    (fake: FakeCellServer) => Effect.promise(() => fake.close()),
  );

type Continuation = ProviderContinuationRequests.ProviderContinuationRequest;

interface HarnessOptions {
  readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
  readonly modelSelection?: { readonly instanceId: ProviderInstanceId; readonly model: string };
  readonly providerSessionId?: ProviderSessionId;
  readonly resume?: OrchestrationV2ProviderThread;
  readonly adapterOptions?: Partial<CellAdapterV2Options>;
  /** Where the event collector runs; defaults to the session's scope. */
  readonly collectorScope?: Scope.Scope;
}

const makeAdapter = (
  transport: CellTransport,
  baseUrl: string,
  extra: Partial<CellAdapterV2Options> = {},
) =>
  Effect.gen(function* () {
    const continuations = yield* Queue.unbounded<Continuation>();
    const adapter = yield* makeCellAdapterV2({
      instanceId: INSTANCE_ID,
      settings: settingsFor(baseUrl),
      transport,
      requestTimeoutMs: 10_000,
      continuationRequests: {
        offer: (request) => Queue.offer(continuations, request).pipe(Effect.asVoid),
      },
      ...extra,
    });
    return { adapter, continuations };
  });

const openHarness = Effect.fnUntraced(function* (
  made: Effect.Success<ReturnType<typeof makeAdapter>>,
  options: HarnessOptions = {},
) {
  const runtimePolicy = options.runtimePolicy ?? fullAccess;
  const modelSelection = options.modelSelection ?? defaultModel;
  const runtime = yield* made.adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: options.providerSessionId ?? SESSION_ID,
    modelSelection,
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const allEvents: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
  const collectorScope = options.collectorScope ?? (yield* Effect.scope);
  const collector = yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        allEvents.push(event);
        yield* Queue.offer(emitted, event);
      }),
    ),
    Effect.ignore,
    Effect.forkIn(collectorScope),
  );
  const providerThread = options.resume
    ? yield* runtime.resumeThread({ providerThread: options.resume, modelSelection, runtimePolicy })
    : yield* runtime.ensureThread({ threadId: THREAD_ID, modelSelection, runtimePolicy });
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
  return {
    runtime,
    providerThread,
    takeEvent,
    allEvents,
    collector,
    takeContinuation: Queue.take(made.continuations),
  };
});

const makeHarness = Effect.fnUntraced(function* (
  transport: CellTransport,
  baseUrl: string,
  options: HarnessOptions = {},
) {
  const made = yield* makeAdapter(transport, baseUrl, options.adapterOptions ?? {});
  return yield* openHarness(made, options);
});

type Harness = Effect.Success<ReturnType<typeof makeHarness>>;

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  text: string,
  ordinal = 1,
  origin: {
    readonly createdBy: "user" | "agent";
    readonly creationSource: "web" | "provider";
    readonly messageId?: MessageId;
    readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
    readonly model?: string;
  } = { createdBy: "user", creationSource: "web" },
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  const runtimePolicy = origin.runtimePolicy ?? fullAccess;
  const modelSelection = { instanceId: INSTANCE_ID, model: origin.model ?? "default" };
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
      messageId: origin.messageId ?? MessageId.make(`message-${ordinal}`),
      text,
      attachments: [],
      createdBy: origin.createdBy,
      creationSource: origin.creationSource,
    },
    modelSelection,
    runtimePolicy,
  };
});

/** Starts the continuation turn the adapter offered for a wake and waits for its end. */
const runContinuation = Effect.fnUntraced(function* (
  harness: Harness,
  request: Continuation,
  ordinal: number,
) {
  yield* harness.runtime.startTurn(
    yield* turnInput(harness.providerThread, "", ordinal, {
      createdBy: "agent",
      creationSource: "provider",
      ...(request.messageId === undefined ? {} : { messageId: request.messageId }),
    }),
  );
  return yield* harness.takeEvent("turn.terminal", (event) => event.runOrdinal === ordinal);
});

/** The running turn's id was announced before the items the tests wait for. */
const activeTurnId = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) => {
  const turn = events.findLast((event) => event.type === "provider_turn.updated");
  if (turn?.type !== "provider_turn.updated") throw new Error("no provider turn was announced");
  return turn.providerTurn.id;
};

const itemsOfType = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>, type: string) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === type ? [event.turnItem] : [],
  );

const assistantTexts = (
  events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
  runOrdinal?: number,
) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" &&
    event.turnItem.type === "assistant_message" &&
    (runOrdinal === undefined || event.turnItem.runId === `run-${runOrdinal}`)
      ? [event.turnItem.text]
      : [],
  );

const failureOf = (
  terminal: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }>,
) => (terminal.status === "failed" ? terminal.failure : null);

describe("CellAdapterV2 over the fake cell", () => {
  it.live("maps shell, patch, tool and assistant events to turn items", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [
          { type: "shell", command: "ls", output: "a\nb\n", exitCode: 0 },
          { type: "patch", operation: "Add", path: "src/x.ts" },
          {
            type: "tool",
            name: "task_status",
            arguments: { taskId: "t1" },
            output: '{"taskId":"t1","status":"completed"}',
          },
          { type: "assistant", text: "all done" },
        ],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      assert.equal(harness.providerThread.nativeThreadRef?.nativeId, project);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Do the thing"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(fake.prompts(project), ["Do the thing"]);

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
      assert.deepEqual(assistantTexts(harness.allEvents), ["all done"]);
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
          thread.providerThread.nativeConversationHeadRef?.nativeId === `${project}:turn:1`,
      );
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("Stop posts cancel and settles only on the cell's done", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [
          { type: "shell_until_cancel", command: "sleep 30" },
          { type: "assistant", text: "should not finish" },
        ],
      });
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

  it.live(
    "a Stop pressed right after sending waits for the prompt to register, then cancels it",
    () =>
      Effect.gen(function* () {
        const fake = yield* fakeCell({
          script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
        });
        const transport = yield* liveTransport;
        const harness = yield* makeHarness(transport, fake.baseUrl);
        const project = yield* cellProjectName("t3-", THREAD_ID);
        const input = yield* turnInput(harness.providerThread, "Run `sleep 30`");
        const started = yield* harness.runtime.startTurn(input).pipe(Effect.forkScoped);
        yield* harness.takeEvent("provider_turn.updated");
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: activeTurnId(harness.allEvents),
        });
        yield* Fiber.join(started);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "interrupted");
        assert.deepEqual(fake.prompts(project), ["Run `sleep 30`"]);
        yield* Effect.promise(() => fake.onceIdle(project));
      }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live(
    "reconnects when the cell closes the socket and keeps a turn the run id proves alive",
    () =>
      Effect.gen(function* () {
        const fake = yield* fakeCell({
          script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
        });
        const transport = yield* liveTransport;
        const harness = yield* makeHarness(transport, fake.baseUrl);
        const project = yield* cellProjectName("t3-", THREAD_ID);
        yield* harness.runtime.startTurn(
          yield* turnInput(harness.providerThread, "Run `sleep 30`"),
        );
        yield* harness.takeEvent(
          "turn_item.updated",
          (event) =>
            event.turnItem.type === "command_execution" && event.turnItem.status === "running",
        );
        const reconnected = fake.onceConnected(project);
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

  it.live(
    "without run ids a reconnect during a turn cannot prove the turn survived and fails it",
    () =>
      Effect.gen(function* () {
        const fake = yield* fakeCell({
          protocol: "legacy",
          script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
        });
        const transport = yield* liveTransport;
        const harness = yield* makeHarness(transport, fake.baseUrl);
        const project = yield* cellProjectName("t3-", THREAD_ID);
        yield* harness.runtime.startTurn(
          yield* turnInput(harness.providerThread, "Run `sleep 30`"),
        );
        yield* harness.takeEvent(
          "turn_item.updated",
          (event) =>
            event.turnItem.type === "command_execution" && event.turnItem.status === "running",
        );
        const reconnected = fake.onceConnected(project);
        fake.dropSockets();
        yield* Effect.promise(() => reconnected);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "does not number its runs");
        fake.cancelRun(project);
        yield* Effect.promise(() => fake.onceIdle(project));
      }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("recovers a turn's end from the event log after missing it while disconnected", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Run `sleep 30`"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      fake.rejectSockets(true);
      fake.dropSockets();
      fake.cancelRun(project);
      yield* Effect.promise(() => fake.onceIdle(project));
      const reconnected = fake.onceConnected(project);
      fake.rejectSockets(false);
      yield* Effect.promise(() => reconnected);
      const terminal = yield* harness.takeEvent("turn.terminal");
      // The cell's `done` was read back, so the turn ends the way the cell ended it.
      assert.equal(terminal.status, "cancelled");
      const shell = itemsOfType(harness.allEvents, "command_execution").at(-1);
      assert.equal(shell?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("without an event log a turn whose end it missed fails as a transport error", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        protocol: "legacy",
        script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Run `sleep 30`"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      fake.rejectSockets(true);
      fake.dropSockets();
      fake.cancelRun(project);
      const reconnected = fake.onceConnected(project);
      fake.rejectSockets(false);
      yield* Effect.promise(() => reconnected);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(failureOf(terminal)?.class, "transport_error");
      const thread = harness.allEvents.findLast(
        (event) => event.type === "provider_thread.updated",
      );
      assert.isTrue(
        thread?.type === "provider_thread.updated" && thread.providerThread.status === "idle",
      );
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live(
    "a delegated child that finishes while idle wakes the thread through a continuation",
    () =>
      Effect.gen(function* () {
        const fake = yield* fakeCell({
          script: (prompt) =>
            prompt.startsWith("Delegate")
              ? [
                  {
                    type: "delegate",
                    taskId: "ab12",
                    title: "Review",
                    summary: "Fine",
                    wake: [{ type: "assistant", text: "WAKE_DONE" }],
                  },
                  { type: "assistant", text: "STARTED" },
                ]
              : [{ type: "assistant", text: "USER_REPLY" }],
        });
        const transport = yield* liveTransport;
        const harness = yield* makeHarness(transport, fake.baseUrl);
        yield* harness.runtime.startTurn(
          yield* turnInput(harness.providerThread, "Delegate a review"),
        );
        const first = yield* harness.takeEvent("turn.terminal");
        assert.equal(first.status, "completed");
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["STARTED"]);
        const roster = harness.allEvents.find(
          (event) =>
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
        );
        assert.isTrue(
          roster?.type === "provider_thread.updated" &&
            roster.providerThread.pendingBackgroundTasks?.[0]?.taskId === "ab12",
          "the running child reaches the thread's roster during the first turn",
        );
        const request = yield* harness.takeContinuation;
        assert.equal(request.threadId, THREAD_ID);
        assert.equal(request.delivery, "adapter_buffered");
        assert.include(request.notification?.summary ?? "", "Review");
        assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
        const wake = yield* runContinuation(harness, request, 2);
        assert.equal(wake.status, "completed", failureOf(wake)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE_DONE"]);
        assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
        const subagent = itemsOfType(harness.allEvents, "subagent").at(-1);
        assert.isTrue(subagent?.type === "subagent" && subagent.status === "completed");
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Thanks", 3));
        const third = yield* harness.takeEvent("turn.terminal");
        assert.equal(third.status, "completed");
        assert.deepEqual(assistantTexts(harness.allEvents, 3), ["USER_REPLY"]);
      }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("two children finishing produce two continuations that each carry their own run", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [
          {
            type: "delegate",
            taskId: "one",
            title: "First",
            summary: "1",
            wake: [{ type: "assistant", text: "WAKE_ONE" }],
          },
          {
            type: "delegate",
            taskId: "two",
            title: "Second",
            summary: "2",
            wake: [{ type: "assistant", text: "WAKE_TWO" }],
          },
          { type: "assistant", text: "STARTED" },
        ],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Delegate twice"));
      assert.equal((yield* harness.takeEvent("turn.terminal")).status, "completed");
      const first = yield* harness.takeContinuation;
      const second = yield* harness.takeContinuation;
      assert.notEqual(first.messageId, second.messageId);
      // Dispatch order is the orchestrator's; the message id binds each turn to its wake.
      const late = yield* runContinuation(harness, second, 2);
      assert.equal(late.status, "completed", failureOf(late)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE_TWO"]);
      assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
      const early = yield* runContinuation(harness, first, 3);
      assert.equal(early.status, "completed", failureOf(early)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 3), ["WAKE_ONE"]);
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("a prompt while another client's run is active fails without adopting that run", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [{ type: "assistant", text: "MINE" }],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      assert.isTrue(
        fake.startForeignRun(project, "theirs", [
          { type: "shell_until_cancel", command: "sleep 5" },
          { type: "assistant", text: "THEIRS" },
        ]),
      );
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.include(failureOf(terminal)?.message ?? "", "already running");
      fake.cancelRun(project);
      yield* Effect.promise(() => fake.onceIdle(project));
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
      assert.lengthOf(itemsOfType(harness.allEvents, "command_execution"), 0);
      // The cell is idle again, so the same message goes through.
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine", 2));
      assert.equal((yield* harness.takeEvent("turn.terminal")).status, "completed");
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["MINE"]);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("a new session resumes the thread's cell and continues its conversation", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell();
      const transport = yield* liveTransport;
      const made = yield* makeAdapter(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      const first = yield* openHarness(made).pipe(Effect.scoped);
      const resumed = yield* openHarness(made, {
        providerSessionId: ProviderSessionId.make("session-cell-2"),
        resume: first.providerThread,
      });
      assert.equal(resumed.providerThread.id, first.providerThread.id);
      assert.equal(resumed.providerThread.nativeThreadRef?.nativeId, project);
      yield* resumed.runtime.startTurn(
        yield* turnInput(resumed.providerThread, "Respond with exactly: later", 2),
      );
      assert.equal((yield* resumed.takeEvent("turn.terminal")).status, "completed");
      assert.deepEqual(fake.prompts(project), ["Respond with exactly: later"]);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("closing the session cancels its turn, settles its tasks and stops reconnecting", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [
          {
            type: "delegate",
            taskId: "slow",
            title: "Slow child",
            summary: "never",
            wake: [],
          },
          { type: "shell_until_cancel", command: "sleep 30" },
        ],
      });
      const transport = yield* liveTransport;
      const made = yield* makeAdapter(transport, fake.baseUrl);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      const sessionScope = yield* Scope.make();
      const collectorScope = yield* Effect.scope;
      const harness = yield* openHarness(made, { collectorScope }).pipe(
        Scope.provide(sessionScope),
      );
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Work"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.type === "command_execution" && event.turnItem.status === "running",
      );
      const connected = fake.onceConnected(project);
      yield* Scope.close(sessionScope, Exit.void);
      // The event queue shuts down with the session, which ends the collector.
      yield* Fiber.await(harness.collector);
      const terminal = harness.allEvents.findLast((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal?.type === "turn.terminal",
        `no terminal among ${harness.allEvents.map((event) => event.type).join(", ")}`,
      );
      if (terminal?.type !== "turn.terminal") return;
      assert.equal(terminal.status, "cancelled");
      assert.equal(terminal.threadDisposition, "broken");
      const subagent = itemsOfType(harness.allEvents, "subagent").at(-1);
      assert.equal(subagent?.status, "cancelled");
      fake.cancelRun(project);
      yield* Effect.promise(() => fake.onceIdle(project));
      const raced = yield* Effect.race(
        Effect.promise(() => connected).pipe(Effect.as("reconnected" as const)),
        Effect.sleep("150 millis").pipe(Effect.as("quiet" as const)),
      );
      assert.equal(raced, "quiet");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("refuses every permission mode but Full access before and after the session opens", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell();
      const transport = yield* liveTransport;
      const made = yield* makeAdapter(transport, fake.baseUrl);
      const opened = yield* made.adapter
        .openSession({
          threadId: THREAD_ID,
          providerSessionId: SESSION_ID,
          modelSelection: defaultModel,
          runtimePolicy: supervised,
        })
        .pipe(Effect.result);
      assert.isTrue(opened._tag === "Failure");
      if (opened._tag !== "Failure") return;
      assert.equal(opened.failure._tag, "ProviderAdapterOpenSessionError");
      assert.include(String(Reflect.get(opened.failure, "cause")), CELL_RUNTIME_MODE_MESSAGE);

      const harness = yield* openHarness(made);
      const ensured = yield* harness.runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: defaultModel,
          runtimePolicy: supervised,
        })
        .pipe(Effect.result);
      assert.isTrue(ensured._tag === "Failure");
      if (ensured._tag !== "Failure") return;
      assert.include(String(Reflect.get(ensured.failure, "cause")), CELL_RUNTIME_MODE_MESSAGE);

      yield* harness.runtime.startTurn(
        yield* turnInput(harness.providerThread, "Hello", 1, {
          createdBy: "user",
          creationSource: "web",
          runtimePolicy: supervised,
        }),
      );
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(failureOf(terminal)?.class, "permission_error");
      assert.equal(failureOf(terminal)?.message, CELL_RUNTIME_MODE_MESSAGE);
      const project = yield* cellProjectName("t3-", THREAD_ID);
      assert.deepEqual(fake.prompts(project), []);
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("accepts only Default or the tenant's model", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({ model: "gpt-6-astra" });
      const transport = yield* liveTransport;
      const made = yield* makeAdapter(transport, fake.baseUrl);
      const harness = yield* openHarness(made, {
        modelSelection: { instanceId: INSTANCE_ID, model: "gpt-6-astra" },
      });
      const other = yield* harness.runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: { instanceId: INSTANCE_ID, model: "gpt-5" },
          runtimePolicy: fullAccess,
        })
        .pipe(Effect.result);
      assert.isTrue(other._tag === "Failure");
      if (other._tag !== "Failure") return;
      assert.include(String(Reflect.get(other.failure, "cause")), "cannot switch to gpt-5");
      yield* harness.runtime.startTurn(
        yield* turnInput(harness.providerThread, "Hello", 1, {
          createdBy: "user",
          creationSource: "web",
          model: "gpt-5",
        }),
      );
      const refused = yield* harness.takeEvent("turn.terminal");
      assert.equal(refused.status, "failed");
      assert.equal(failureOf(refused)?.class, "validation_error");
      yield* harness.runtime.startTurn(
        yield* turnInput(harness.providerThread, "Respond with exactly: hi", 2, {
          createdBy: "user",
          creationSource: "web",
          model: "gpt-6-astra",
        }),
      );
      assert.equal((yield* harness.takeEvent("turn.terminal")).status, "completed");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("classifies a quota failure as a usage limit with its reset time", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell({
        script: () => [
          {
            type: "fail",
            error: "insufficient_quota",
            code: "usage_limit",
            status: 429,
            resetAt: "2026-10-19T00:00:00.000Z",
          },
        ],
      });
      const transport = yield* liveTransport;
      const harness = yield* makeHarness(transport, fake.baseUrl);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hello"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(failureOf(terminal)?.class, "usage_limit");
      assert.equal(failureOf(terminal)?.code, "usage_limit");
      assert.equal(failureOf(terminal)?.resetAt, "2026-10-19T00:00:00.000Z");
      const legacy = yield* fakeCell({
        protocol: "legacy",
        script: () => [{ type: "fail", error: "429 insufficient_quota: slow down" }],
      });
      const fallback = yield* makeHarness(transport, legacy.baseUrl, {
        providerSessionId: ProviderSessionId.make("session-legacy"),
      });
      yield* fallback.runtime.startTurn(yield* turnInput(fallback.providerThread, "Hello"));
      const classified = yield* fallback.takeEvent("turn.terminal");
      assert.equal(failureOf(classified)?.class, "usage_limit");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("a disabled instance opens no session", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell();
      const transport = yield* liveTransport;
      const adapter = yield* makeCellAdapterV2({
        instanceId: INSTANCE_ID,
        settings: settingsFor(fake.baseUrl, "secret-token", false),
        transport,
      });
      const opened = yield* adapter
        .openSession({
          threadId: THREAD_ID,
          providerSessionId: SESSION_ID,
          modelSelection: defaultModel,
          runtimePolicy: fullAccess,
        })
        .pipe(Effect.result);
      assert.isTrue(opened._tag === "Failure");
      if (opened._tag !== "Failure") return;
      assert.include(opened.failure.message, "disabled");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("reports the tenant's model and key through the status probe", () =>
    Effect.gen(function* () {
      const transport = yield* liveTransport;
      const ready = yield* fakeCell();
      const snapshot = yield* checkCellProviderStatus(settingsFor(ready.baseUrl), transport);
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.version, "0.3.0");
      assert.deepEqual(
        snapshot.models.map((model) => [model.slug, model.isDefault, model.isCustom]),
        [["gpt-6-astra", true, false]],
      );
      assert.deepEqual(snapshot.supportedRuntimeModes, ["full-access"]);
      const unconfigured = yield* fakeCell({ configured: false });
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
  const ws = (event: Record<string, unknown>): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "ws_event", event },
  });
  const wsClosed = (): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "ws_closed", code: 1001 },
  });
  const expectHttp = (method: string, path: string, body?: string): ProviderReplayEntry => ({
    type: "expect_outbound",
    frame: { kind: "http", method, path, ...(body === undefined ? {} : { body }) },
  });
  const httpResponse = (status: number, body = ""): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "http_response", status, body },
  });
  const httpFailure = (detail: string): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "http_failure", detail },
  });
  const wsConnect = (): ProviderReplayEntry => ({
    type: "expect_outbound",
    frame: { kind: "ws_connect", path: "/cell/<project>/ws" },
  });
  const stateBody = (over: Record<string, unknown> = {}) =>
    JSON.stringify({
      model: "gpt-6-astra",
      running: false,
      configured: true,
      version: "0.3.0",
      runId: null,
      seq: 0,
      ...over,
    });
  /** Session open against a numbered tenant: state over HTTP, then the socket's snapshots. */
  const prelude = (over: Record<string, unknown> = {}, tasks: Array<unknown> = []) => [
    expectHttp("GET", "/cell/<project>/state"),
    httpResponse(200, stateBody(over)),
    wsConnect(),
    ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 0, ...over }),
    ws({ type: "tasks", tasks }),
  ];
  const legacyPrelude = () => [
    expectHttp("GET", "/cell/<project>/state"),
    httpResponse(
      200,
      JSON.stringify({ model: "gpt-6-astra", running: false, configured: true, version: "0.2.0" }),
    ),
    wsConnect(),
    ws({ type: "state", running: false, parent: null, depth: 0 }),
    ws({ type: "tasks", tasks: [] }),
  ];
  const scripted = (scenario: string, entries: ReadonlyArray<ProviderReplayEntry>) =>
    makeReplayCellTransport({ scenario, entries });
  const scriptedLayer = IdAllocator.layer;

  it.effect("buffers another run's events during the prompt and drops them on 409", () =>
    Effect.gen(function* () {
      const controller = scripted("foreign-409", [
        ...prelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Mine"),
        ws({ type: "assistant", text: "THEIRS", runId: "run-9", seq: 1 }),
        ws({ type: "done", runId: "run-9", seq: 2 }),
        httpResponse(409, '{"runId":"run-9"}'),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(failureOf(terminal)?.class, "provider_error");
      assert.include(failureOf(terminal)?.message ?? "", "already running");
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("without run ids, the prompt's echo separates another run's tail from T3's run", () =>
    Effect.gen(function* () {
      const controller = scripted("legacy-echo", [
        ...legacyPrelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Mine"),
        ws({ type: "assistant", text: "THEIRS" }),
        ws({ type: "done" }),
        ws({ type: "user", text: "Mine" }),
        ws({ type: "assistant", text: "MINE" }),
        httpResponse(202),
        ws({ type: "done" }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents), ["MINE"]);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("queues every wake and lets each continuation adopt the one its message id names", () =>
    Effect.gen(function* () {
      const controller = scripted("two-wakes", [
        ...prelude(),
        ws({
          type: "wake",
          text: "[Delegated task a done]",
          runId: "run-1",
          seq: 1,
          origin: "wake",
        }),
        ws({ type: "assistant", text: "WAKE_ONE", runId: "run-1", seq: 2 }),
        ws({ type: "done", runId: "run-1", seq: 3 }),
        ws({
          type: "wake",
          text: "[Delegated task b done]",
          runId: "run-2",
          seq: 4,
          origin: "wake",
        }),
        ws({ type: "assistant", text: "WAKE_TWO", runId: "run-2", seq: 5 }),
        ws({ type: "done", runId: "run-2", seq: 6 }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const first = yield* harness.takeContinuation;
      const second = yield* harness.takeContinuation;
      assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
      const late = yield* runContinuation(harness, second, 1);
      assert.equal(late.status, "completed", failureOf(late)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 1), ["WAKE_TWO"]);
      const early = yield* runContinuation(harness, first, 2);
      assert.equal(early.status, "completed", failureOf(early)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE_ONE"]);
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("without run ids, wakes queue in arrival order with the events that follow each", () =>
    Effect.gen(function* () {
      const controller = scripted("legacy-two-wakes", [
        ...legacyPrelude(),
        ws({ type: "wake", text: "[Delegated task a done]" }),
        ws({ type: "assistant", text: "WAKE_ONE" }),
        ws({ type: "done" }),
        ws({ type: "wake", text: "[Delegated task b done]" }),
        ws({ type: "assistant", text: "WAKE_TWO" }),
        ws({ type: "done" }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const first = yield* harness.takeContinuation;
      const second = yield* harness.takeContinuation;
      assert.notEqual(first.messageId, second.messageId);
      assert.equal((yield* runContinuation(harness, first, 1)).status, "completed");
      assert.deepEqual(assistantTexts(harness.allEvents, 1), ["WAKE_ONE"]);
      assert.equal((yield* runContinuation(harness, second, 2)).status, "completed");
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE_TWO"]);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("a cleared continuation drops its wake instead of pinning the session", () =>
    Effect.gen(function* () {
      const controller = scripted("wake-cleared", [
        ...prelude(),
        ws({
          type: "wake",
          text: "[Delegated task a done]",
          runId: "run-1",
          seq: 1,
          origin: "wake",
        }),
        ws({ type: "assistant", text: "WAKE_ONE", runId: "run-1", seq: 2 }),
        ws({ type: "done", runId: "run-1", seq: 3 }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const request = yield* harness.takeContinuation;
      assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
      yield* request.clearIfCurrent!();
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      const dispatched = yield* request.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.isTrue(dispatched._tag === "None");
      const terminal = yield* runContinuation(harness, request, 1);
      assert.equal(terminal.status, "completed");
      assert.lengthOf(itemsOfType(harness.allEvents, "assistant_message"), 0);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("a done that arrives with the cancel's response settles the turn as completed", () =>
    Effect.gen(function* () {
      const controller = scripted("cancel-races-done", [
        ...prelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Hi"),
        ws({ type: "user", text: "Hi", runId: "run-1", seq: 1, origin: "user" }),
        httpResponse(202, '{"runId":"run-1","seq":1}'),
        ws({ type: "assistant", text: "DONE_ANYWAY", runId: "run-1", seq: 2 }),
        expectHttp("POST", "/cell/<project>/cancel"),
        ws({ type: "done", runId: "run-1", seq: 3 }),
        httpResponse(204),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) => event.turnItem.type === "assistant_message",
      );
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: activeTurnId(harness.allEvents),
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed");
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("a lost prompt response is recovered from the cell's state and event log", () =>
    Effect.gen(function* () {
      const controller = scripted("prompt-lost-recovered", [
        ...prelude({ seq: 4 }),
        expectHttp("POST", "/cell/<project>/prompt", "Hi"),
        httpFailure("socket hang up"),
        expectHttp("GET", "/cell/<project>/state"),
        httpResponse(200, stateBody({ running: true, runId: "run-1", seq: 6 })),
        expectHttp("GET", "/cell/<project>/events?after=4&limit=1000"),
        httpResponse(
          200,
          JSON.stringify([
            { type: "user", text: "Hi", runId: "run-1", seq: 5, origin: "user" },
            { type: "assistant", text: "RECOVERED", runId: "run-1", seq: 6 },
          ]),
        ),
        ws({ type: "done", runId: "run-1", seq: 7 }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents), ["RECOVERED"]);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("a lost prompt response against an idle cell fails the turn as never started", () =>
    Effect.gen(function* () {
      const controller = scripted("prompt-lost-idle", [
        ...prelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Hi"),
        httpFailure("socket hang up"),
        expectHttp("GET", "/cell/<project>/state"),
        httpResponse(200, stateBody()),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "failed");
      assert.equal(failureOf(terminal)?.class, "transport_error");
      assert.include(failureOf(terminal)?.message ?? "", "never started");
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect(
    "without run ids, a lost prompt response waits for the echo before adopting the run",
    () =>
      Effect.gen(function* () {
        const controller = scripted("legacy-prompt-lost", [
          ...legacyPrelude(),
          expectHttp("POST", "/cell/<project>/prompt", "Hi"),
          httpFailure("socket hang up"),
          expectHttp("GET", "/cell/<project>/state"),
          httpResponse(
            200,
            JSON.stringify({
              model: "gpt-6-astra",
              running: true,
              configured: true,
              version: "0.2.0",
            }),
          ),
          ws({ type: "user", text: "Hi" }),
          ws({ type: "assistant", text: "ECHOED" }),
          ws({ type: "done" }),
        ]);
        const harness = yield* makeHarness(controller.transport, "https://cell.test");
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["ECHOED"]);
        controller.assertComplete();
      }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("rejects attachments as a validation failure instead of dropping them", () =>
    Effect.gen(function* () {
      const controller = scripted("attachments", prelude());
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
      assert.equal(failureOf(terminal)?.class, "validation_error");
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.effect("refuses a second request to another project in the same recording", () =>
    Effect.gen(function* () {
      const controller = scripted("project-binding", [
        ...prelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Hi"),
        httpResponse(202, '{"runId":"run-1","seq":1}'),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const elsewhere = yield* controller.transport
        .request({
          method: "POST",
          url: "https://cell.test/cell/t3-somewhere-else/prompt",
          headers: {},
          body: "Hi",
        })
        .pipe(Effect.result);
      assert.isTrue(elsewhere._tag === "Failure");
      assert.throws(() => controller.assertComplete(), /mismatch/);
      assert.isDefined(harness);
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  // Reconnects back off on the real clock, so these scenarios run live.
  it.live("processes the frames of a connection before the close that followed them", () =>
    Effect.gen(function* () {
      const controller = scripted("fifo-drain", [
        ...prelude(),
        expectHttp("POST", "/cell/<project>/prompt", "Hi"),
        httpResponse(202, '{"runId":"run-1","seq":1}'),
        ws({ type: "user", text: "Hi", runId: "run-1", seq: 1, origin: "user" }),
        ws({ type: "assistant", text: "BEFORE_CLOSE", runId: "run-1", seq: 2 }),
        ws({ type: "done", runId: "run-1", seq: 3 }),
        wsClosed(),
        wsConnect(),
        ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 3 }),
        ws({ type: "tasks", tasks: [] }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents), ["BEFORE_CLOSE"]);
      // The reconnect follows the first short-lived connection's 500 ms backoff.
      yield* Effect.sleep("700 millis");
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.live("reconciles the task roster from the snapshot after a reconnect", () =>
    Effect.gen(function* () {
      const running = { taskId: "t1", title: "Child", status: "running" };
      const controller = scripted("tasks-reconcile", [
        ...prelude({}, [running]),
        wsClosed(),
        wsConnect(),
        ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 0 }),
        ws({ type: "tasks", tasks: [] }),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      yield* harness.takeEvent(
        "provider_thread.updated",
        (event) => event.providerThread.pendingBackgroundTasks?.[0]?.taskId === "t1",
      );
      assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
      yield* harness.takeEvent(
        "provider_thread.updated",
        (event) => (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 0,
      );
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.live(
    "a wake whose run ended while disconnected fails its continuation as a transport error",
    () =>
      Effect.gen(function* () {
        const controller = scripted("wake-lost", [
          ...prelude(),
          ws({
            type: "wake",
            text: "[Delegated task a done]",
            runId: "run-1",
            seq: 1,
            origin: "wake",
          }),
          wsClosed(),
          wsConnect(),
          ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 1 }),
          ws({ type: "tasks", tasks: [{ taskId: "marker", title: "Marker", status: "running" }] }),
        ]);
        const harness = yield* makeHarness(controller.transport, "https://cell.test");
        const request = yield* harness.takeContinuation;
        yield* harness.takeEvent(
          "provider_thread.updated",
          (event) => event.providerThread.pendingBackgroundTasks?.[0]?.taskId === "marker",
        );
        const terminal = yield* runContinuation(harness, request, 1);
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "while T3 was disconnected");
        controller.assertComplete();
      }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );

  it.live("a wake's missed events are read back from the event log on reconnect", () =>
    Effect.gen(function* () {
      const controller = scripted("wake-recovered", [
        ...prelude(),
        ws({
          type: "wake",
          text: "[Delegated task a done]",
          runId: "run-1",
          seq: 1,
          origin: "wake",
        }),
        wsClosed(),
        wsConnect(),
        ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 3 }),
        ws({ type: "tasks", tasks: [{ taskId: "marker", title: "Marker", status: "running" }] }),
        expectHttp("GET", "/cell/<project>/events?after=1&limit=1000"),
        httpResponse(
          200,
          JSON.stringify([
            { type: "assistant", text: "WAKE_DONE", runId: "run-1", seq: 2 },
            { type: "done", runId: "run-1", seq: 3 },
          ]),
        ),
      ]);
      const harness = yield* makeHarness(controller.transport, "https://cell.test");
      const request = yield* harness.takeContinuation;
      yield* harness.takeEvent(
        "provider_thread.updated",
        (event) => event.providerThread.pendingBackgroundTasks?.[0]?.taskId === "marker",
      );
      const terminal = yield* runContinuation(harness, request, 1);
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 1), ["WAKE_DONE"]);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(scriptedLayer)),
  );
});

describe("CellAdapterV2 reconnect backoff", () => {
  /** Every connection sends the snapshots and closes at once, as a proxy that drops sockets would. */
  const flappingTransport = (connects: Array<number>): CellTransport => ({
    request: () =>
      Effect.succeed({
        status: 200,
        body: JSON.stringify({ model: "m", running: false, configured: true, runId: null, seq: 0 }),
      }),
    connect: ({ url }) =>
      Effect.sync(() => {
        connects.push(connects.length + 1);
        let delivered = false;
        const pull: Effect.Effect<ReadonlyArray<string>, CellTransportError> = Effect.suspend(
          () => {
            if (!delivered) {
              delivered = true;
              return Effect.succeed([
                JSON.stringify({ type: "state", running: false, runId: null, seq: 0 }),
                JSON.stringify({ type: "tasks", tasks: [] }),
              ]);
            }
            return Effect.fail(
              new CellTransportError({ operation: `read ${url}`, detail: "closed", closed: true }),
            );
          },
        );
        return { pull };
      }),
  });

  it.effect("backs off between connections that die young and resets after a healthy one", () =>
    Effect.gen(function* () {
      const connects: Array<number> = [];
      yield* makeHarness(flappingTransport(connects), "https://cell.test");
      const settle = Effect.gen(function* () {
        for (let index = 0; index < 20; index++) yield* Effect.yieldNow;
      });
      yield* settle;
      assert.deepEqual(connects, [1]);
      yield* TestClock.adjust("499 millis");
      yield* settle;
      assert.deepEqual(connects, [1], "the first short-lived connection waits 500 ms");
      yield* TestClock.adjust("1 millis");
      yield* settle;
      assert.deepEqual(connects, [1, 2]);
      yield* TestClock.adjust("999 millis");
      yield* settle;
      assert.deepEqual(connects, [1, 2], "the second waits a full second");
      yield* TestClock.adjust("1 millis");
      yield* settle;
      assert.deepEqual(connects, [1, 2, 3]);
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
  );

  it.effect("a connection that stayed healthy makes the next drop reconnect at once", () =>
    Effect.gen(function* () {
      const connects: Array<number> = [];
      const gate = yield* Deferred.make<void>();
      let opened = 0;
      const transport: CellTransport = {
        ...flappingTransport(connects),
        connect: ({ url }) =>
          Effect.sync(() => {
            connects.push(connects.length + 1);
            opened += 1;
            const mine = opened;
            let delivered = false;
            const pull: Effect.Effect<ReadonlyArray<string>, CellTransportError> = Effect.suspend(
              () => {
                if (!delivered) {
                  delivered = true;
                  return Effect.succeed([
                    JSON.stringify({ type: "state", running: false, runId: null, seq: 0 }),
                    JSON.stringify({ type: "tasks", tasks: [] }),
                  ]);
                }
                const closed = Effect.fail(
                  new CellTransportError({
                    operation: `read ${url}`,
                    detail: "closed",
                    closed: true,
                  }),
                );
                // The first connection lives until the test releases it; the rest die young.
                return mine === 1 ? Deferred.await(gate).pipe(Effect.andThen(closed)) : closed;
              },
            );
            return { pull };
          }),
      };
      yield* makeHarness(transport, "https://cell.test");
      const settle = Effect.gen(function* () {
        for (let index = 0; index < 20; index++) yield* Effect.yieldNow;
      });
      yield* settle;
      assert.deepEqual(connects, [1]);
      yield* TestClock.adjust("30 seconds");
      yield* Deferred.succeed(gate, undefined);
      yield* settle;
      assert.deepEqual(connects, [1, 2], "a healthy connection's drop reconnects without delay");
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
  );
});
