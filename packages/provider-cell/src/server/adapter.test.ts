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
import * as Option from "effect/Option";
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
  type FakeCellServer,
  type FakeCellScript,
} from "../testing/fakeCellServer.ts";
import { makeReplayCellTransport } from "../testing/replayTransport.ts";
import { makeCellAdapterV2, type CellAdapterV2Options } from "./adapter.ts";
import { CELL_RUNTIME_MODE_MESSAGE } from "./errors.ts";
import { CELL_PROVIDER, cellProjectName } from "./protocol.ts";
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
    readonly model?: string;
  } = {},
) =>
  Effect.acquireRelease(
    Effect.promise(() =>
      startFakeCellServer({
        token: "secret-token",
        ...(options.script ? { script: options.script } : {}),
        ...(options.configured === undefined ? {} : { configured: options.configured }),
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
    readonly creationSource: "web" | "provider" | "server";
    readonly messageId?: MessageId;
    readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
    readonly model?: string;
    /** The run T3's own restart continuation resumes, as the orchestrator names it. */
    readonly restartContinuationOfRunId?: RunId;
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
    ...(origin.restartContinuationOfRunId === undefined
      ? {}
      : { restartContinuationOfRunId: origin.restartContinuationOfRunId }),
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
      // An error outside any run is the cell's diagnostic: logged, never an item of the turn.
      fake.raiseError(project, "Could not report to the parent cell");
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Do the thing"));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(fake.prompts(project), ["Do the thing"]);
      assert.lengthOf(itemsOfType(harness.allEvents, "system_notice"), 0);

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
      // The cancel names the run it is for; the fake refuses any other.
      assert.deepEqual(fake.cancels(yield* cellProjectName("t3-", THREAD_ID)), ["run-1"]);
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

  /**
   * A session that owns a run in progress and ends with it still running, as
   * on a server stop: the cell holds the run until `release`, and `persisted`
   * is the provider thread as the orchestrator last stored it, with the head
   * naming the run at the number the session had applied.
   */
  const interruptedSession = Effect.fnUntraced(function* () {
    let release = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const fake = yield* fakeCell({
      script: () => [
        { type: "assistant", text: "FIRST_HALF" },
        { type: "hold", until: held },
        { type: "assistant", text: "SECOND_HALF" },
      ],
    });
    const transport = yield* liveTransport;
    const made = yield* makeAdapter(transport, fake.baseUrl);
    const project = yield* cellProjectName("t3-", THREAD_ID);
    const sessionScope = yield* Scope.make();
    const collectorScope = yield* Effect.scope;
    const first = yield* openHarness(made, { collectorScope }).pipe(Scope.provide(sessionScope));
    yield* first.runtime.startTurn(yield* turnInput(first.providerThread, "Work"));
    // The head follows the run: after the first answer it names the run at that number.
    const persisted = yield* first.takeEvent(
      "provider_thread.updated",
      (event) =>
        event.providerThread.nativeConversationHeadRef?.nativeId === "run-1" &&
        (event.providerThread.nativeConversationHeadRef.ordinal ?? 0) >= 2,
    );
    yield* Scope.close(sessionScope, Exit.void);
    yield* Fiber.await(first.collector);
    const closing = first.allEvents.findLast((event) => event.type === "provider_thread.updated");
    assert.equal(
      closing?.type === "provider_thread.updated"
        ? closing.providerThread.nativeConversationHeadRef?.strength
        : undefined,
      "strong",
      "closing the session must leave the head naming the run the cell still runs",
    );
    const resume = () =>
      openHarness(made, {
        providerSessionId: ProviderSessionId.make("session-cell-2"),
        resume: persisted.providerThread,
      });
    return { fake, project, release, resume };
  });

  it.live(
    "a restart continuation takes back the run the last session owned and carries its rest",
    () =>
      Effect.gen(function* () {
        const { fake, project, release, resume } = yield* interruptedSession();
        const resumed = yield* resume();
        yield* resumed.runtime.startTurn(
          yield* turnInput(resumed.providerThread, "Continue where you left off.", 2, {
            createdBy: "agent",
            creationSource: "server",
            restartContinuationOfRunId: RunId.make("run-1"),
          }),
        );
        // The turn owned the surviving run inside startTurn, before anything could be sent; the rest of the run is now its.
        release();
        const terminal = yield* resumed.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(resumed.allEvents, 2), ["SECOND_HALF"]);
        assert.deepEqual(fake.prompts(project), ["Work"], "the continuation posts no prompt");
        const settled = resumed.allEvents.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(
          settled?.type === "provider_thread.updated"
            ? settled.providerThread.nativeConversationHeadRef?.strength
            : undefined,
          "weak",
        );
      }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("a run that ended while T3 was down settles its restart continuation from the log", () =>
    Effect.gen(function* () {
      const { fake, project, release, resume } = yield* interruptedSession();
      release();
      yield* Effect.promise(() => fake.onceIdle(project));
      const resumed = yield* resume();
      yield* resumed.runtime.startTurn(
        yield* turnInput(resumed.providerThread, "Continue where you left off.", 2, {
          createdBy: "agent",
          creationSource: "server",
          restartContinuationOfRunId: RunId.make("run-1"),
        }),
      );
      const terminal = yield* resumed.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(resumed.allEvents, 2), ["SECOND_HALF"]);
      assert.deepEqual(fake.prompts(project), ["Work"], "the continuation posts no prompt");
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live(
    "a message sent while the last session's run still runs is refused naming it, and a continuation carries it",
    () =>
      Effect.gen(function* () {
        const { fake, project, release, resume } = yield* interruptedSession();
        const resumed = yield* resume();
        yield* resumed.runtime.startTurn(
          yield* turnInput(resumed.providerThread, "Something new", 2),
        );
        const refused = yield* resumed.takeEvent("turn.terminal");
        assert.equal(refused.status, "failed");
        assert.include(failureOf(refused)?.message ?? "", "when T3 Code last stopped");
        assert.deepEqual(fake.prompts(project), ["Work"]);
        const offered = yield* resumed.takeContinuation;
        yield* resumed.runtime.startTurn(
          yield* turnInput(resumed.providerThread, "", 3, {
            createdBy: "agent",
            creationSource: "provider",
            ...(offered.messageId === undefined ? {} : { messageId: offered.messageId }),
          }),
        );
        release();
        const terminal = yield* resumed.takeEvent(
          "turn.terminal",
          (event) => event.runOrdinal === 3,
        );
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(resumed.allEvents, 3), ["SECOND_HALF"]);
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

  it.live(
    "the fake cell keeps a prompt's request id on its run and cancels only the run named",
    () =>
      Effect.gen(function* () {
        const fake = yield* fakeCell({
          script: () => [{ type: "shell_until_cancel", command: "sleep 30" }],
        });
        const transport = yield* liveTransport;
        const project = "t3-direct";
        const auth = { authorization: "Bearer secret-token" };
        const post = (path: string, headers: Record<string, string>, body?: string) =>
          transport.request({
            method: "POST",
            url: `${fake.baseUrl}/cell/${project}${path}`,
            headers: { ...auth, ...headers },
            ...(body === undefined ? {} : { body }),
          });
        const first = yield* post("/prompt", { "x-request-id": "req-1" }, "Run `sleep 30`");
        assert.equal(first.status, 202);
        assert.equal(first.body, '{"runId":"run-1","seq":1}');
        const repeated = yield* post("/prompt", { "x-request-id": "req-1" }, "Run `sleep 30`");
        assert.equal(repeated.status, 202, "a repeat with the run's request id is that run again");
        assert.equal(repeated.body, '{"runId":"run-1","seq":1}');
        const other = yield* post("/prompt", { "x-request-id": "req-2" }, "Something else");
        assert.equal(other.status, 409);
        assert.equal(other.body, '{"runId":"run-1"}');
        const state = yield* transport.request({
          method: "GET",
          url: `${fake.baseUrl}/cell/${project}/state`,
          headers: auth,
        });
        assert.include(state.body, '"requestId":"req-1"');
        const wrongRun = yield* post("/cancel", { "x-run-id": "run-9" });
        assert.equal(wrongRun.status, 409, "a cancel for another run cancels nothing");
        assert.equal(wrongRun.body, '{"runId":"run-1"}');
        const cancelled = yield* post("/cancel", { "x-run-id": "run-1" });
        assert.equal(cancelled.status, 204);
        yield* Effect.promise(() => fake.onceIdle(project));
        const afterEnd = yield* post("/prompt", { "x-request-id": "req-1" }, "Run `sleep 30`");
        assert.equal(afterEnd.status, 202, "the run that ended last still answers its request id");
        assert.equal(afterEnd.body, '{"runId":"run-1","seq":1}');
        assert.equal(
          (yield* post("/cancel", { "x-run-id": "run-1" })).status,
          204,
          "no run: no-op",
        );
        assert.deepEqual(fake.cancels(project), ["run-9", "run-1", "run-1"]);
        const empty = yield* post("/prompt", { "x-request-id": "req-3" }, "   ");
        assert.equal(empty.status, 409);
        assert.equal(empty.body, '{"runId":null,"error":"The prompt is empty"}');
        assert.deepEqual(fake.prompts(project), ["Run `sleep 30`"]);
        const unconfigured = yield* fakeCell({ configured: false });
        const refused = yield* transport.request({
          method: "POST",
          url: `${unconfigured.baseUrl}/cell/${project}/prompt`,
          headers: auth,
          body: "Hi",
        });
        assert.equal(refused.status, 409);
        assert.equal(
          refused.body,
          '{"runId":null,"error":"OPENAI_API_KEY is not configured for this cell"}',
        );
      }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );

  it.live("reports the tenant's model and key through the status probe", () =>
    Effect.gen(function* () {
      const transport = yield* liveTransport;
      const ready = yield* fakeCell();
      const snapshot = yield* checkCellProviderStatus(settingsFor(ready.baseUrl), transport);
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.version, "0.4.0");
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

describe("CellAdapterV2 against a tenant below cell protocol 3", () => {
  /** Answers `GET state` as a tenant of the stated level would, and must never be asked for a socket. */
  const olderTenant = (state: Record<string, unknown>): CellTransport => ({
    request: () => Effect.succeed({ status: 200, body: JSON.stringify(state) }),
    connect: () => Effect.die(new Error("an older tenant must never be attached")),
  });
  const level2 = {
    protocol: 2,
    model: "gpt-6-astra",
    running: false,
    configured: true,
    version: "0.3.0",
    runId: null,
    seq: 0,
  };
  const unstated = { model: "gpt-6-astra", running: false, configured: true, version: "0.2.0" };

  it.effect(
    "the status probe names the level the tenant speaks and the one this provider needs",
    () =>
      Effect.gen(function* () {
        const stated = yield* checkCellProviderStatus(
          settingsFor("https://old.test"),
          olderTenant(level2),
        );
        assert.equal(stated.status, "error");
        assert.equal(stated.installed, true);
        assert.include(stated.message ?? "", "cell protocol 2");
        assert.include(stated.message ?? "", "needs 3");
        assert.deepEqual(stated.models, []);
        const silent = yield* checkCellProviderStatus(
          settingsFor("https://old.test"),
          olderTenant(unstated),
        );
        assert.equal(silent.status, "error");
        assert.include(silent.message ?? "", "stating no level");
        assert.include(silent.message ?? "", "needs 3");
      }),
  );

  it.effect("attaching a thread fails with the typed error before any socket or prompt", () =>
    Effect.gen(function* () {
      const made = yield* makeAdapter(olderTenant(level2), "https://old.test");
      const runtime = yield* made.adapter.openSession({
        threadId: THREAD_ID,
        providerSessionId: SESSION_ID,
        modelSelection: defaultModel,
        runtimePolicy: fullAccess,
      });
      const ensured = yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: defaultModel,
          runtimePolicy: fullAccess,
        })
        .pipe(Effect.result);
      assert.isTrue(ensured._tag === "Failure");
      if (ensured._tag !== "Failure") return;
      const cause = Reflect.get(ensured.failure, "cause");
      assert.equal(Reflect.get(cause as object, "_tag"), "CellProtocolError");
      assert.include(String(cause), "cell protocol 2");
      assert.include(String(cause), "needs 3");
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
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
      protocol: 3,
      model: "gpt-6-astra",
      running: false,
      configured: true,
      version: "0.4.0",
      runId: null,
      seq: 0,
      ...over,
    });
  /** One complete page of the event log, as `GET events` answers it. */
  const eventsPage = (events: ReadonlyArray<Record<string, unknown>>) =>
    JSON.stringify({ events, nextAfter: events.at(-1)?.seq ?? 0, complete: true });
  /** Session open: state over HTTP, then the socket's `state` and the roster at its number. */
  const prelude = (over: Record<string, unknown> = {}, tasks: Array<unknown> = []) => [
    expectHttp("GET", "/cell/<project>/state"),
    httpResponse(200, stateBody(over)),
    wsConnect(),
    ws({ type: "state", running: false, parent: null, depth: 0, runId: null, seq: 0, ...over }),
    ws({ type: "tasks", tasks, seq: typeof over.seq === "number" ? over.seq : 0 }),
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

  it.effect(
    "a lost prompt response is recovered by its request id on the opener in the event log",
    () =>
      Effect.gen(function* () {
        const controller = scripted("prompt-lost-recovered", [
          ...prelude({ seq: 4 }),
          expectHttp("POST", "/cell/<project>/prompt", "Hi"),
          httpFailure("socket hang up"),
          expectHttp("GET", "/cell/<project>/state"),
          httpResponse(
            200,
            stateBody({ running: true, runId: "run-1", requestId: "<request-id:1>", seq: 6 }),
          ),
          expectHttp("GET", "/cell/<project>/events?after=4&limit=1000"),
          httpResponse(
            200,
            eventsPage([
              {
                type: "user",
                text: "Hi",
                runId: "run-1",
                requestId: "<request-id:1>",
                seq: 5,
                origin: "user",
              },
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

  it.effect(
    "a server error without the cell's refusal is a lost answer; one carrying it is a refusal",
    () =>
      Effect.gen(function* () {
        const controller = scripted("prompt-5xx", [
          ...prelude(),
          expectHttp("POST", "/cell/<project>/prompt", "Hi"),
          httpResponse(502, "<html>Bad Gateway</html>"),
          expectHttp("GET", "/cell/<project>/state"),
          httpResponse(200, stateBody()),
          expectHttp("POST", "/cell/<project>/prompt", "Again"),
          httpResponse(500, '{"runId":null,"error":"The body did not arrive"}'),
        ]);
        const harness = yield* makeHarness(controller.transport, "https://cell.test");
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Hi"));
        const lost = yield* harness.takeEvent("turn.terminal");
        assert.equal(lost.status, "failed");
        assert.equal(failureOf(lost)?.class, "transport_error");
        assert.include(failureOf(lost)?.message ?? "", "answered 502");
        assert.include(failureOf(lost)?.message ?? "", "never started");
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Again", 2));
        const refused = yield* harness.takeEvent("turn.terminal");
        assert.equal(refused.status, "failed");
        assert.equal(failureOf(refused)?.class, "provider_error");
        assert.include(failureOf(refused)?.message ?? "", "The body did not arrive");
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
        ws({ type: "tasks", tasks: [], seq: 3 }),
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
        ws({ type: "tasks", tasks: [], seq: 0 }),
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
          ws({
            type: "tasks",
            tasks: [{ taskId: "marker", title: "Marker", status: "running" }],
            seq: 1,
          }),
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
        ws({
          type: "tasks",
          tasks: [{ taskId: "marker", title: "Marker", status: "running" }],
          seq: 3,
        }),
        expectHttp("GET", "/cell/<project>/events?after=1&limit=1000"),
        httpResponse(
          200,
          eventsPage([
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
        body: JSON.stringify({
          protocol: 3,
          model: "m",
          running: false,
          configured: true,
          runId: null,
          seq: 0,
        }),
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
                JSON.stringify({ type: "tasks", tasks: [], seq: 0 }),
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
                    JSON.stringify({ type: "tasks", tasks: [], seq: 0 }),
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

describe("CellAdapterV2 over a hand-driven socket", () => {
  interface DrivenCell {
    readonly transport: CellTransport;
    /** Delivers frames on the current connection, behind whatever was sent before. */
    readonly send: (frames: ReadonlyArray<Record<string, unknown>>) => Effect.Effect<void>;
    /** Ends the current connection as a dropped socket; the next one opens with `opensWith`. */
    readonly close: Effect.Effect<void>;
    readonly opensWith: (frames: ReadonlyArray<Record<string, unknown>>) => void;
    /**
     * What `GET events` answers from now on: the whole log in one complete
     * page, or a page per request, which may be an effect the test releases.
     */
    readonly replays: (
      log:
        | ReadonlyArray<Record<string, unknown>>
        | ((after: number) => Record<string, unknown> | Effect.Effect<Record<string, unknown>>),
    ) => void;
    /** What `GET state` answers from now on, over the idle default. */
    readonly states: (over: Record<string, unknown>) => void;
    /** Answers the next `POST prompt`; given before the prompt, it answers at once. */
    readonly answerPrompt: (status: number, body?: string) => Effect.Effect<void>;
    /** Fails the next `POST prompt` at the transport, as a dropped socket would. */
    readonly failPrompt: (detail: string) => Effect.Effect<void>;
    /** Answers the next `POST cancel`; without one queued, a cancel answers 204. */
    readonly answerCancel: (status: number, body?: string) => Effect.Effect<void>;
    readonly replayRequests: Array<string>;
    /** The `x-request-id` of every prompt posted, in order. */
    readonly promptRequestIds: Array<string | undefined>;
    /** The `x-run-id` of every cancel posted, in order. */
    readonly cancels: Array<string | undefined>;
    /** How many sockets the adapter has opened. */
    readonly connects: () => number;
  }
  /** The whole log as one complete page of the events after `after`. */
  const wholeLog = (events: ReadonlyArray<Record<string, unknown>>) => (after: number) => {
    const page = events.filter((event) => Number(event.seq) > after);
    return { events: page, nextAfter: page.at(-1)?.seq ?? after, complete: true };
  };
  interface Answer {
    readonly status: number;
    readonly body: string;
  }
  /**
   * A transport the test drives by hand, for races the transcript walk cannot
   * express: the prompt's answer is released when the test says, and frames
   * can follow it. Reconnects back off on the clock, so these run live or
   * under the test clock.
   */
  const driven = () =>
    Effect.gen(function* () {
      const incoming = yield* Queue.unbounded<ReadonlyArray<string> | "close">();
      const answers = yield* Queue.unbounded<Answer>();
      const cancelAnswers = yield* Queue.unbounded<Answer>();
      let opening: ReadonlyArray<Record<string, unknown>> = [];
      let replay: (
        after: number,
      ) => Record<string, unknown> | Effect.Effect<Record<string, unknown>> = wholeLog([]);
      const replayRequests: Array<string> = [];
      const promptRequestIds: Array<string | undefined> = [];
      const cancels: Array<string | undefined> = [];
      let connects = 0;
      let state: Record<string, unknown> = {
        protocol: 3,
        model: "gpt-6-astra",
        running: false,
        configured: true,
        runId: null,
        requestId: null,
        seq: 0,
        version: "0.4.0",
      };
      const send = (frames: ReadonlyArray<Record<string, unknown>>) =>
        Queue.offer(
          incoming,
          frames.map((frame) => JSON.stringify(frame)),
        ).pipe(Effect.asVoid);
      const transport: CellTransport = {
        request: (request) => {
          const url = new URL(request.url);
          if (request.method === "POST" && url.pathname.endsWith("/prompt")) {
            promptRequestIds.push(request.headers["x-request-id"]);
            // Status 0 stands for no answer at all.
            return Queue.take(answers).pipe(
              Effect.filterOrFail(
                (answer) => answer.status !== 0,
                (answer) =>
                  new CellTransportError({ operation: "POST prompt", detail: answer.body }),
              ),
            );
          }
          if (request.method === "POST" && url.pathname.endsWith("/cancel")) {
            cancels.push(request.headers["x-run-id"]);
            return Queue.poll(cancelAnswers).pipe(
              Effect.map(Option.getOrElse((): Answer => ({ status: 204, body: "" }))),
            );
          }
          if (url.pathname.endsWith("/events")) {
            replayRequests.push(`/events${url.search}`);
            return Effect.suspend(() => {
              const page = replay(Number(url.searchParams.get("after") ?? "0"));
              return (Effect.isEffect(page) ? page : Effect.succeed(page)).pipe(
                Effect.map((body) => ({ status: 200, body: JSON.stringify(body) })),
              );
            });
          }
          return Effect.sync(() => ({ status: 200, body: JSON.stringify(state) }));
        },
        connect: () =>
          Effect.gen(function* () {
            connects += 1;
            yield* send(opening);
            const pull: Effect.Effect<ReadonlyArray<string>, CellTransportError> = Effect.gen(
              function* () {
                const batch = yield* Queue.take(incoming);
                if (batch === "close") {
                  return yield* new CellTransportError({
                    operation: "read",
                    detail: "closed",
                    closed: true,
                  });
                }
                return batch;
              },
            );
            return { pull };
          }),
      };
      const cell: DrivenCell = {
        transport,
        send,
        close: Queue.offer(incoming, "close").pipe(Effect.asVoid),
        opensWith: (frames) => {
          opening = frames;
        },
        replays: (log) => {
          replay = typeof log === "function" ? log : wholeLog(log);
        },
        states: (over) => {
          state = { ...state, ...over };
        },
        answerPrompt: (status, body = "") =>
          Queue.offer(answers, { status, body }).pipe(Effect.asVoid),
        failPrompt: (detail) =>
          Queue.offer(answers, { status: 0, body: detail }).pipe(Effect.asVoid),
        answerCancel: (status, body = "") =>
          Queue.offer(cancelAnswers, { status, body }).pipe(Effect.asVoid),
        replayRequests,
        promptRequestIds,
        cancels,
        connects: () => connects,
      };
      return cell;
    });
  /** A connection's opening: `state` and the roster numbered at the same point. */
  const prelude = (state: Record<string, unknown> = {}, tasks: ReadonlyArray<unknown> = []) => {
    const seq = typeof state.seq === "number" ? state.seq : 0;
    return [
      { type: "state", running: false, parent: null, depth: 0, runId: null, ...state, seq },
      { type: "tasks", tasks, seq },
    ];
  };
  /** A task row sent last on a connection: once it reaches the roster, everything before it was processed. */
  const marker = (seq: number, taskId = "marker") => ({
    type: "task",
    task: { taskId, title: "Marker", status: "running" },
    seq,
  });
  /** Lets the fibers behind the queues and the test clock run on. */
  const settle = Effect.gen(function* () {
    for (let index = 0; index < 100; index++) yield* Effect.yieldNow;
  });
  const awaitMarker = (harness: Harness, taskId = "marker") =>
    harness.takeEvent(
      "provider_thread.updated",
      (event) =>
        event.providerThread.pendingBackgroundTasks?.some((task) => task.taskId === taskId) ===
        true,
    );
  /** Waits on the live clock until the cell has seen as many requests of a kind. */
  const awaitRequests = (count: () => number, expected: number) =>
    Effect.gen(function* () {
      while (count() < expected) yield* Effect.sleep("5 millis");
    });
  /** Starts a turn and leaves it waiting on its unanswered prompt. */
  const startParked = Effect.fnUntraced(function* (harness: Harness, text: string, ordinal = 1) {
    const started = yield* harness.runtime
      .startTurn(yield* turnInput(harness.providerThread, text, ordinal))
      .pipe(Effect.forkScoped);
    yield* harness.takeEvent(
      "provider_turn.updated",
      (event) => event.providerTurn.ordinal === ordinal,
    );
    return started;
  });
  const subagentStatuses = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "subagent.updated"
        ? [`${event.subagent.nativeTaskRef?.nativeId}:${event.subagent.status}`]
        : [],
    );
  const drivenLayer = IdAllocator.layer;

  it.live(
    "a gateway's JSON error without the cell's refusal is a lost answer, owned by the opener already buffered",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* startParked(harness, "Hi");
        yield* awaitRequests(() => cell.promptRequestIds.length, 1);
        const requestId = cell.promptRequestIds[0]!;
        yield* cell.send([
          { type: "user", text: "Hi", runId: "run-1", requestId, seq: 1, origin: "user" },
          { type: "assistant", text: "BUFFERED", runId: "run-1", seq: 2 },
          { type: "done", runId: "run-1", seq: 3 },
          marker(4),
        ]);
        yield* awaitMarker(harness);
        // The same body as HTML would have gone through recovery; JSON without the cell's `runId` does too.
        yield* cell.answerPrompt(502, '{"error":"upstream reset"}');
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["BUFFERED"]);
        assert.deepEqual(cell.replayRequests, [], "the opener among the candidates needs no log");
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a 202 that overtakes the previous run's tail keeps the turn; a stranger after the run began still ends it",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude({ running: true, runId: "OLD", seq: 1 }));
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"MINE","seq":4}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([
          { type: "done", runId: "OLD", seq: 3 },
          { type: "user", text: "Mine", runId: "MINE", seq: 4, origin: "user" },
          { type: "assistant", text: "MINE", runId: "MINE", seq: 5 },
          { type: "done", runId: "MINE", seq: 6 },
        ]);
        const first = yield* harness.takeEvent("turn.terminal");
        assert.equal(first.status, "completed", failureOf(first)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["MINE"]);

        yield* cell.answerPrompt(202, '{"runId":"MINE_2","seq":8}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Again", 2));
        yield* cell.send([
          { type: "assistant", text: "OLD_TAIL", runId: "OLD", seq: 7 },
          { type: "user", text: "Again", runId: "MINE_2", seq: 8, origin: "user" },
          { type: "assistant", text: "THEIRS", runId: "OTHER", seq: 9 },
        ]);
        const second = yield* harness.takeEvent("turn.terminal");
        assert.equal(second.status, "failed");
        assert.equal(failureOf(second)?.class, "transport_error");
        assert.deepEqual(assistantTexts(harness.allEvents, 2), []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a reconnect before the 202 is judged once it names the run: the log supplies the end",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Mine");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.replays([
          { type: "assistant", text: "MINE", runId: "A", seq: 2 },
          { type: "done", runId: "A", seq: 3 },
        ]);
        cell.opensWith([...prelude({ seq: 3 }), marker(4)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* Fiber.join(started);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["MINE"]);
        assert.deepEqual(cell.replayRequests, ["/events?after=1&limit=1000"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a reconnect before the 202 fails the turn when the cell is idle and its log has no end",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Mine");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.replays([
          { type: "fs", changes: [], seq: 2 },
          { type: "fs", changes: [], seq: 3 },
        ]);
        cell.opensWith([...prelude({ seq: 3 }), marker(4)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* Fiber.join(started);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "its result stayed in the cell");
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live("a reconnect before the run began hides nothing of it", () =>
    Effect.gen(function* () {
      const cell = yield* driven();
      cell.opensWith(prelude());
      const harness = yield* makeHarness(cell.transport, "https://cell.test");
      const started = yield* startParked(harness, "Mine");
      cell.opensWith([...prelude(), marker(1)]);
      yield* cell.close;
      yield* awaitMarker(harness);
      yield* cell.answerPrompt(202, '{"runId":"A","seq":2}');
      yield* Fiber.join(started);
      yield* cell.send([
        { type: "user", text: "Mine", runId: "A", seq: 2, origin: "user" },
        { type: "assistant", text: "MINE", runId: "A", seq: 3 },
        { type: "done", runId: "A", seq: 4 },
      ]);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents), ["MINE"]);
    }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live("a wake buffered behind the accepted run's done still becomes a continuation", () =>
    Effect.gen(function* () {
      const cell = yield* driven();
      cell.opensWith(prelude());
      const harness = yield* makeHarness(cell.transport, "https://cell.test");
      const started = yield* startParked(harness, "Mine");
      yield* cell.send([
        { type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" },
        { type: "assistant", text: "MINE", runId: "A", seq: 2 },
        { type: "done", runId: "A", seq: 3 },
        { type: "wake", text: "[Delegated task a done]", runId: "B", seq: 4, origin: "wake" },
        { type: "assistant", text: "WAKE", runId: "B", seq: 5 },
        { type: "done", runId: "B", seq: 6 },
        marker(7),
      ]);
      yield* awaitMarker(harness);
      yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
      yield* Fiber.join(started);
      const first = yield* harness.takeEvent("turn.terminal");
      assert.equal(first.status, "completed", failureOf(first)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 1), ["MINE"]);
      const request = yield* harness.takeContinuation;
      const wake = yield* runContinuation(harness, request, 2);
      assert.equal(wake.status, "completed", failureOf(wake)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE"]);
    }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "a continuation whose wake was cleared ends empty and leaves the next wake its own",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.send([
          { type: "wake", text: "[Delegated task a done]", runId: "A", seq: 1, origin: "wake" },
          { type: "assistant", text: "FIRST", runId: "A", seq: 2 },
          { type: "done", runId: "A", seq: 3 },
          { type: "wake", text: "[Delegated task b done]", runId: "B", seq: 4, origin: "wake" },
          { type: "assistant", text: "SECOND", runId: "B", seq: 5 },
          { type: "done", runId: "B", seq: 6 },
        ]);
        const first = yield* harness.takeContinuation;
        const second = yield* harness.takeContinuation;
        yield* first.clearIfCurrent!();
        const stale = yield* runContinuation(harness, first, 1);
        assert.equal(stale.status, "completed");
        assert.deepEqual(assistantTexts(harness.allEvents, 1), []);
        const live = yield* runContinuation(harness, second, 2);
        assert.equal(live.status, "completed", failureOf(live)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["SECOND"]);
        assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect("a dispatched wake that a reset dropped fails its continuation and takes nothing", () =>
    Effect.gen(function* () {
      const cell = yield* driven();
      cell.opensWith(prelude());
      const harness = yield* makeHarness(cell.transport, "https://cell.test");
      yield* cell.send([
        { type: "wake", text: "[Delegated task a done]", runId: "A", seq: 1, origin: "wake" },
        { type: "assistant", text: "FIRST", runId: "A", seq: 2 },
        { type: "done", runId: "A", seq: 3 },
      ]);
      const first = yield* harness.takeContinuation;
      const dispatched = yield* first.dispatchIfCurrent!(Effect.succeed("persisted"));
      assert.isTrue(dispatched._tag === "Some");
      yield* cell.send([
        { type: "reset", seq: 4 },
        { type: "wake", text: "[Delegated task b done]", runId: "B", seq: 5, origin: "wake" },
        { type: "assistant", text: "SECOND", runId: "B", seq: 6 },
        { type: "done", runId: "B", seq: 7 },
      ]);
      const second = yield* harness.takeContinuation;
      const stale = yield* runContinuation(harness, first, 1);
      assert.equal(stale.status, "failed");
      assert.include(failureOf(stale)?.message ?? "", "reset");
      assert.deepEqual(assistantTexts(harness.allEvents, 1), []);
      const live = yield* runContinuation(harness, second, 2);
      assert.equal(live.status, "completed", failureOf(live)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents, 2), ["SECOND"]);
      assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a reconnect reads the gap back up to its snapshot; the roster at that number settles a task it dropped and later events arrive live",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([
          { type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" },
          { type: "task", task: { taskId: "old-child", title: "Old", status: "running" }, seq: 2 },
        ]);
        yield* harness.takeEvent(
          "provider_thread.updated",
          (event) =>
            event.providerThread.pendingBackgroundTasks?.some(
              (task) => task.taskId === "old-child",
            ) === true,
        );
        const live = { type: "assistant", text: "LIVE", runId: "A", seq: 4 };
        // The log already runs past the snapshot; what lies beyond it is the socket's to deliver.
        cell.replays([{ type: "assistant", text: "MISSED", runId: "A", seq: 3 }, live]);
        cell.opensWith([
          ...prelude({ running: true, runId: "A", seq: 3 }),
          live,
          { type: "done", runId: "A", seq: 5 },
        ]);
        yield* cell.close;
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["MISSED", "LIVE"]);
        assert.deepEqual(subagentStatuses(harness.allEvents), [
          "old-child:running",
          "old-child:cancelled",
        ]);
        assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
        assert.deepEqual(cell.replayRequests, ["/events?after=2&limit=1000"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a reconnect between the prompt's opening event and its 202 changes nothing: the 202 names the run and its events are the turn's",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Mine");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.opensWith([...prelude({ running: true, runId: "A", seq: 1 }), marker(2)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* Fiber.join(started);
        yield* cell.send([
          { type: "assistant", text: "MINE", runId: "A", seq: 3 },
          { type: "done", runId: "A", seq: 4 },
        ]);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["MINE"]);
        assert.deepEqual(cell.replayRequests, []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "another run's output read back from the gap before the 202 is never the turn's: a run after T3's began ends it",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Mine");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.replays([
          { type: "user", text: "Theirs", runId: "B", seq: 2, origin: "user" },
          { type: "assistant", text: "FOREIGN", runId: "B", seq: 3 },
        ]);
        cell.opensWith([...prelude({ running: true, runId: "B", seq: 3 }), marker(4)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* Fiber.join(started);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.deepEqual(assistantTexts(harness.allEvents), []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "candidates are judged in sequence order: a wake's run numbered after T3's began ends the turn and is queued, not adopted",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Mine");
        yield* cell.send([
          { type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" },
          { type: "assistant", text: "MINE", runId: "A", seq: 2 },
          { type: "wake", text: "[Delegated task a done]", runId: "B", seq: 3, origin: "wake" },
          { type: "assistant", text: "WAKE", runId: "B", seq: 4 },
          { type: "done", runId: "B", seq: 5 },
          marker(6),
        ]);
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* Fiber.join(started);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["MINE"]);
        const request = yield* harness.takeContinuation;
        const wake = yield* runContinuation(harness, request, 2);
        assert.equal(wake.status, "completed", failureOf(wake)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a task row read back from the gap belongs to the turn that was running when the cell sent it",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        const child = { taskId: "child", title: "Child", status: "running" };
        cell.replays([
          { type: "task", task: child, seq: 2 },
          { type: "done", runId: "A", seq: 3 },
        ]);
        cell.opensWith(prelude({ seq: 3 }, [child]));
        yield* cell.close;
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        const rows = harness.allEvents.flatMap((event) =>
          event.type === "subagent.updated"
            ? [
                `${event.subagent.runId}:${event.subagent.nativeTaskRef?.nativeId}:${event.subagent.status}`,
              ]
            : [],
        );
        assert.isAbove(rows.length, 0, "the child reaches the run that delegated it");
        assert.deepEqual([...new Set(rows)], ["run-1:child:running"]);
        assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
        assert.deepEqual(cell.replayRequests, ["/events?after=1&limit=1000"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "a connection whose roster does not follow its state within ten seconds is dropped and reopened",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        // The next connection sends its state and then nothing.
        cell.opensWith([prelude()[0]!]);
        yield* cell.close;
        yield* TestClock.adjust("500 millis");
        yield* settle;
        assert.equal(cell.connects(), 2);
        yield* TestClock.adjust("9999 millis");
        yield* settle;
        assert.equal(cell.connects(), 2, "the roster has ten seconds from the state");
        cell.opensWith([...prelude(), marker(1)]);
        yield* TestClock.adjust("1 millis");
        yield* settle;
        yield* TestClock.adjust("1 seconds");
        yield* settle;
        assert.equal(cell.connects(), 3);
        yield* awaitMarker(harness);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "a run event sent before the roster is not applied; the connection is dropped and the next one reads the gap",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.opensWith([
          prelude({ running: true, runId: "A", seq: 2 })[0]!,
          { type: "assistant", text: "LEAKED", runId: "A", seq: 2 },
        ]);
        yield* cell.close;
        yield* TestClock.adjust("500 millis");
        yield* settle;
        assert.equal(cell.connects(), 2);
        cell.replays([{ type: "assistant", text: "EARLY", runId: "A", seq: 2 }]);
        cell.opensWith([
          ...prelude({ running: true, runId: "A", seq: 2 }),
          { type: "done", runId: "A", seq: 3 },
        ]);
        yield* TestClock.adjust("1 seconds");
        yield* settle;
        assert.equal(cell.connects(), 3);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["EARLY"]);
        assert.deepEqual(cell.replayRequests, ["/events?after=1&limit=1000"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a reset keeps a marker only for a dispatched continuation; an undispatched or cleared one ends empty",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.send([
          { type: "wake", text: "[Delegated task a done]", runId: "A", seq: 1, origin: "wake" },
          { type: "assistant", text: "FIRST", runId: "A", seq: 2 },
          { type: "done", runId: "A", seq: 3 },
          { type: "wake", text: "[Delegated task b done]", runId: "B", seq: 4, origin: "wake" },
          { type: "assistant", text: "SECOND", runId: "B", seq: 5 },
          { type: "done", runId: "B", seq: 6 },
        ]);
        const first = yield* harness.takeContinuation;
        const second = yield* harness.takeContinuation;
        const dispatched = yield* second.dispatchIfCurrent!(Effect.succeed("persisted"));
        assert.isTrue(dispatched._tag === "Some");
        yield* cell.send([{ type: "reset", seq: 7 }, marker(8)]);
        yield* awaitMarker(harness);
        yield* second.clearIfCurrent!();
        const undispatched = yield* runContinuation(harness, first, 1);
        assert.equal(undispatched.status, "completed", failureOf(undispatched)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 1), []);
        const cleared = yield* runContinuation(harness, second, 2);
        assert.equal(cleared.status, "completed", failureOf(cleared)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live("a gap is read back a page at a time, each applied before the next is fetched", () =>
    Effect.gen(function* () {
      const cell = yield* driven();
      cell.opensWith(prelude());
      const harness = yield* makeHarness(cell.transport, "https://cell.test");
      yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
      yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
      const log = [
        { type: "assistant", text: "ONE", runId: "A", seq: 2 },
        { type: "assistant", text: "TWO", runId: "A", seq: 3 },
        { type: "assistant", text: "THREE", runId: "A", seq: 4 },
        { type: "done", runId: "A", seq: 5 },
      ];
      cell.replays((after) => {
        const page = log.filter((event) => event.seq > after).slice(0, 2);
        const nextAfter = page.at(-1)?.seq ?? after;
        return { events: page, nextAfter, complete: nextAfter >= 5 };
      });
      cell.opensWith(prelude({ seq: 5 }));
      yield* cell.close;
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      assert.deepEqual(assistantTexts(harness.allEvents), ["ONE", "TWO", "THREE"]);
      assert.deepEqual(cell.replayRequests, [
        "/events?after=1&limit=1000",
        "/events?after=3&limit=1000",
      ]);
    }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a gap the cell no longer retains fails the turn naming it, and marks a queued wake lost",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        cell.replays((after) => ({
          events: [],
          nextAfter: after,
          complete: false,
          truncated: true,
          earliest: 90,
        }));
        cell.opensWith(prelude({ seq: 100 }));
        yield* cell.close;
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "events numbered 2 to 100");

        yield* cell.send([
          { type: "wake", text: "[Delegated task a done]", runId: "W", seq: 101, origin: "wake" },
          { type: "assistant", text: "PARTIAL", runId: "W", seq: 102 },
        ]);
        const request = yield* harness.takeContinuation;
        cell.opensWith([...prelude({ seq: 300 }), marker(301)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        const continuation = yield* runContinuation(harness, request, 2);
        assert.equal(continuation.status, "failed");
        assert.equal(failureOf(continuation)?.class, "transport_error");
        assert.include(failureOf(continuation)?.message ?? "", "while T3 was disconnected");
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["PARTIAL"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a lost 409 never adopts another client's identical prompt: without T3's request id on a run, the turn fails",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        // Another client's prompt with the same words ran and ended; the 409 to T3's own was lost.
        const started = yield* startParked(harness, "Continue");
        yield* cell.send([
          { type: "user", text: "Continue", runId: "F", requestId: null, seq: 1, origin: "user" },
          { type: "assistant", text: "FOREIGN ANSWER", runId: "F", seq: 2 },
          { type: "done", runId: "F", seq: 3 },
          marker(4),
        ]);
        yield* awaitMarker(harness);
        cell.states({ seq: 4 });
        yield* cell.failPrompt("socket hang up");
        yield* Fiber.join(started);
        const idle = yield* harness.takeEvent("turn.terminal");
        assert.equal(idle.status, "failed");
        assert.equal(failureOf(idle)?.class, "transport_error");
        assert.include(failureOf(idle)?.message ?? "", "never started");
        assert.deepEqual(assistantTexts(harness.allEvents), []);
        assert.match(cell.promptRequestIds[0] ?? "", /^[0-9a-f-]{36}$/);

        // The same words again, while the other client's run is still going: T3 cannot tell, and says so.
        const again = yield* startParked(harness, "Continue", 2);
        yield* cell.send([
          { type: "user", text: "Continue", runId: "G", requestId: null, seq: 5, origin: "user" },
          { type: "assistant", text: "FOREIGN AGAIN", runId: "G", seq: 6 },
          marker(7, "marker-2"),
        ]);
        yield* awaitMarker(harness, "marker-2");
        cell.states({ running: true, runId: "G", requestId: null, seq: 7 });
        yield* cell.failPrompt("socket hang up");
        yield* Fiber.join(again);
        const running = yield* harness.takeEvent("turn.terminal");
        assert.equal(running.status, "failed");
        assert.equal(failureOf(running)?.class, "transport_error");
        assert.include(failureOf(running)?.message ?? "", "cannot tell whether this message ran");
        assert.deepEqual(assistantTexts(harness.allEvents), []);
        assert.notEqual(cell.promptRequestIds[1], cell.promptRequestIds[0]);
        assert.deepEqual(cell.replayRequests, []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a lost prompt answer is recovered by T3's request id on the run's opener, or on the run the state names",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        const started = yield* startParked(harness, "Hi");
        yield* awaitRequests(() => cell.promptRequestIds.length, 1);
        const requestId = cell.promptRequestIds[0]!;
        yield* cell.send([
          { type: "user", text: "Hi", runId: "A", requestId, seq: 1, origin: "user" },
          { type: "assistant", text: "BY_OPENER", runId: "A", seq: 2 },
          marker(3),
        ]);
        yield* awaitMarker(harness);
        yield* cell.failPrompt("socket hang up");
        yield* Fiber.join(started);
        yield* cell.send([{ type: "done", runId: "A", seq: 4 }]);
        const first = yield* harness.takeEvent("turn.terminal");
        assert.equal(first.status, "completed", failureOf(first)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["BY_OPENER"]);

        // No opener among what T3 holds, but the state names T3's id on the run in progress.
        const again = yield* startParked(harness, "Again", 2);
        yield* awaitRequests(() => cell.promptRequestIds.length, 2);
        const second = cell.promptRequestIds[1]!;
        assert.notEqual(second, requestId);
        cell.states({ running: true, runId: "B", requestId: second, seq: 4 });
        yield* cell.failPrompt("socket hang up");
        yield* Fiber.join(again);
        yield* cell.send([
          { type: "assistant", text: "BY_STATE", runId: "B", seq: 5 },
          { type: "done", runId: "B", seq: 6 },
        ]);
        const terminal = yield* harness.takeEvent(
          "turn.terminal",
          (event) => event.runOrdinal === 2,
        );
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["BY_STATE"]);
        assert.deepEqual(cell.replayRequests, []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "Stop decided behind a replay that ends the turn sends no cancel, so the run that followed is left alone",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* harness.takeEvent("provider_turn.updated");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        // A ended and a child woke B while T3 was disconnected; the page saying so is held until released.
        const replayStarted = yield* Deferred.make<void>();
        const releaseReplay = yield* Deferred.make<void>();
        cell.replays(() =>
          Deferred.succeed(replayStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseReplay)),
            Effect.as({
              events: [
                { type: "done", runId: "A", seq: 2 },
                {
                  type: "wake",
                  text: "[Delegated task a done]",
                  runId: "B",
                  seq: 3,
                  origin: "wake",
                },
                { type: "assistant", text: "WAKE", runId: "B", seq: 4 },
              ],
              nextAfter: 4,
              complete: true,
            }),
          ),
        );
        cell.opensWith(prelude({ running: true, runId: "B", seq: 4 }));
        yield* cell.close;
        yield* Deferred.await(replayStarted);
        const stop = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: activeTurnId(harness.allEvents),
          })
          .pipe(Effect.forkScoped);
        yield* Effect.sleep("50 millis");
        assert.deepEqual(cell.cancels, [], "the cancel waits behind the replay");
        yield* Deferred.succeed(releaseReplay, undefined);
        yield* Fiber.join(stop);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(cell.cancels, [], "a turn the replay ended has nothing left to stop");
        const request = yield* harness.takeContinuation;
        yield* cell.send([{ type: "done", runId: "B", seq: 5 }]);
        const wake = yield* runContinuation(harness, request, 2);
        assert.equal(wake.status, "completed", failureOf(wake)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a cancel the cell refuses because another run is active means the run ended: the replayed done settles the turn",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* harness.takeEvent("provider_turn.updated");
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        // By the time the cancel reaches the cell, A has ended and B runs: the cell names B and cancels nothing.
        yield* cell.answerCancel(409, '{"runId":"B"}');
        const stop = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: activeTurnId(harness.allEvents),
          })
          .pipe(Effect.forkScoped);
        yield* awaitRequests(() => cell.cancels.length, 1);
        assert.deepEqual(cell.cancels, ["A"]);
        cell.replays([
          { type: "assistant", text: "MINE", runId: "A", seq: 2 },
          { type: "done", runId: "A", seq: 3 },
          { type: "wake", text: "[Delegated task a done]", runId: "B", seq: 4, origin: "wake" },
          { type: "assistant", text: "WAKE", runId: "B", seq: 5 },
          { type: "done", runId: "B", seq: 6 },
        ]);
        cell.opensWith(prelude({ seq: 6 }));
        yield* cell.close;
        yield* Fiber.join(stop);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["MINE"]);
        const request = yield* harness.takeContinuation;
        const wake = yield* runContinuation(harness, request, 2);
        assert.equal(wake.status, "completed", failureOf(wake)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["WAKE"]);
        assert.deepEqual(cell.cancels, ["A"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a gap the cell no longer retains marks a wake it still runs lost, and the loss outranks the wake's done",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.send([
          { type: "wake", text: "[Delegated task a done]", runId: "W", seq: 1, origin: "wake" },
          { type: "assistant", text: "BEFORE GAP", runId: "W", seq: 2 },
        ]);
        const request = yield* harness.takeContinuation;
        cell.replays((after) => ({
          events: [],
          nextAfter: after,
          complete: false,
          truncated: true,
          earliest: 50,
        }));
        cell.opensWith([...prelude({ running: true, runId: "W", seq: 60 }), marker(61)]);
        yield* cell.close;
        yield* awaitMarker(harness);
        yield* cell.send([
          { type: "assistant", text: "AFTER GAP", runId: "W", seq: 62 },
          { type: "done", runId: "W", seq: 63 },
          marker(64, "marker-2"),
        ]);
        yield* awaitMarker(harness, "marker-2");
        const continuation = yield* runContinuation(harness, request, 1);
        assert.equal(continuation.status, "failed");
        assert.equal(failureOf(continuation)?.class, "transport_error");
        assert.include(failureOf(continuation)?.message ?? "", "while T3 was disconnected");
        assert.deepEqual(assistantTexts(harness.allEvents, 1), ["BEFORE GAP", "AFTER GAP"]);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "an error outside any run is the cell's diagnostic, live or read back; a run's error is a notice; replies and unknown frames are ignored",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([
          { type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" },
          { type: "error", text: "Could not report to the parent cell", seq: 2 },
          { type: "exec", id: "x", stdout: "", stderr: "", exitCode: 0, timedOut: false },
          { type: "error", text: "apply_patch failed", runId: "A", seq: 3 },
          { type: "assistant", text: "MINE", runId: "A", seq: 4 },
          { type: "done", runId: "A", seq: 5 },
        ]);
        const first = yield* harness.takeEvent("turn.terminal");
        assert.equal(first.status, "completed", failureOf(first)?.message);
        assert.deepEqual(
          itemsOfType(harness.allEvents, "system_notice").map((item) =>
            item.type === "system_notice" ? item.message : "",
          ),
          ["apply_patch failed"],
        );
        yield* cell.answerPrompt(202, '{"runId":"B","seq":6}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Again", 2));
        yield* cell.send([{ type: "user", text: "Again", runId: "B", seq: 6, origin: "user" }]);
        cell.replays([
          { type: "error", text: "Repaired /project: made a directory again", seq: 7 },
          { type: "assistant", text: "LATE", runId: "B", seq: 8 },
          { type: "done", runId: "B", seq: 9 },
        ]);
        cell.opensWith(prelude({ seq: 9 }));
        yield* cell.close;
        const second = yield* harness.takeEvent("turn.terminal", (event) => event.runOrdinal === 2);
        assert.equal(second.status, "completed", failureOf(second)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["LATE"]);
        assert.lengthOf(itemsOfType(harness.allEvents, "system_notice"), 1);
        assert.equal(cell.connects(), 2);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "a replay page that makes no progress, or ends the log before the snapshot, fails the recovery and keeps the cursor",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        const log = [
          { type: "assistant", text: "LATE", runId: "A", seq: 2 },
          { type: "done", runId: "A", seq: 3 },
        ];
        let pages = 0;
        cell.replays((after) => {
          pages += 1;
          // The first page stalls; the second claims the log ends at 2 though the snapshot said 3; the third is honest.
          if (pages === 1) return { events: [], nextAfter: after, complete: false };
          if (pages === 2) return { events: [log[0]!], nextAfter: 2, complete: true };
          return wholeLog(log)(after);
        });
        cell.opensWith(prelude({ seq: 3 }));
        yield* cell.close;
        for (const backoff of ["500 millis", "1 seconds", "2 seconds"] as const) {
          yield* TestClock.adjust(backoff);
          yield* settle;
        }
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents), ["LATE"]);
        assert.deepEqual(cell.replayRequests, [
          "/events?after=1&limit=1000",
          "/events?after=1&limit=1000",
          "/events?after=2&limit=1000",
        ]);
        assert.equal(cell.connects(), 4);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "three connections in a row without a roster fail the active turn naming the reason; reconnecting goes on",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 1, origin: "user" }]);
        // Every connection from now on sends its state and nothing else.
        cell.opensWith([prelude({ running: true, runId: "A", seq: 1 })[0]!]);
        yield* cell.close;
        for (const backoff of ["500 millis", "1 seconds", "2 seconds"] as const) {
          yield* TestClock.adjust(backoff);
          yield* settle;
          yield* TestClock.adjust("10 seconds");
          yield* settle;
        }
        assert.equal(cell.connects(), 4);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "no tasks roster");
        assert.include(failureOf(terminal)?.message ?? "", "3 connections in a row");
        yield* TestClock.adjust("5 seconds");
        yield* settle;
        assert.equal(cell.connects(), 5, "the socket keeps reconnecting while the thread is idle");
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a snapshot numbered below the last applied fails the turn as a transport error and attaches afresh",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude({ seq: 100 }));
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.answerPrompt(202, '{"runId":"A","seq":101}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Mine"));
        yield* cell.send([{ type: "user", text: "Mine", runId: "A", seq: 101, origin: "user" }]);
        cell.opensWith([...prelude({ seq: 0 }), marker(1)]);
        yield* cell.close;
        const first = yield* harness.takeEvent("turn.terminal");
        assert.equal(first.status, "failed");
        assert.equal(failureOf(first)?.class, "transport_error");
        assert.include(failureOf(first)?.message ?? "", "went backwards");
        yield* awaitMarker(harness);
        // The new numbering is followed from its snapshot: a run numbered 2 is not dropped as old.
        yield* cell.answerPrompt(202, '{"runId":"B","seq":2}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Again", 2));
        yield* cell.send([
          { type: "user", text: "Again", runId: "B", seq: 2, origin: "user" },
          { type: "assistant", text: "NEW", runId: "B", seq: 3 },
          { type: "done", runId: "B", seq: 4 },
        ]);
        const second = yield* harness.takeEvent("turn.terminal", (event) => event.runOrdinal === 2);
        assert.equal(second.status, "completed", failureOf(second)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["NEW"]);
        assert.deepEqual(cell.replayRequests, []);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.effect(
    "a roster numbered elsewhere than its state, and an event with a fractional number, each drop the connection",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        cell.opensWith([
          { type: "state", running: false, runId: null, seq: 0 },
          { type: "tasks", tasks: [], seq: 5 },
        ]);
        yield* cell.close;
        yield* TestClock.adjust("500 millis");
        yield* settle;
        assert.equal(cell.connects(), 2);
        cell.opensWith([...prelude(), { type: "fs", changes: [], seq: 1.5 }]);
        yield* TestClock.adjust("1 seconds");
        yield* settle;
        assert.equal(cell.connects(), 3);
        cell.opensWith([...prelude(), marker(1)]);
        yield* TestClock.adjust("2 seconds");
        yield* settle;
        assert.equal(cell.connects(), 4);
        yield* awaitMarker(harness);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );
  it.live(
    "ownership is persisted before the output buffered while the prompt was unanswered, and a session restored from that head shows all of it",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* startParked(harness, "Mine");
        yield* awaitRequests(() => cell.promptRequestIds.length, 1);
        const requestId = cell.promptRequestIds[0]!;
        // The cell broadcasts the run's first events before it answers the prompt.
        const log = [
          { type: "user", text: "Mine", runId: "A", requestId, seq: 1, origin: "user" },
          { type: "assistant", text: "ONE", runId: "A", seq: 2 },
          { type: "assistant", text: "TWO", runId: "A", seq: 3 },
          marker(4),
        ];
        yield* cell.send(log);
        yield* awaitMarker(harness);
        yield* cell.answerPrompt(202, '{"runId":"A","seq":1}');
        const owned = yield* harness.takeEvent(
          "provider_thread.updated",
          (event) => event.providerThread.nativeConversationHeadRef?.strength === "strong",
        );
        assert.equal(owned.providerThread.nativeConversationHeadRef?.nativeId, "A");
        assert.equal(
          owned.providerThread.nativeConversationHeadRef?.ordinal,
          0,
          "the first head names the number before the buffered opener, not the last number received",
        );
        yield* harness.takeEvent(
          "provider_thread.updated",
          (event) => event.providerThread.nativeConversationHeadRef?.ordinal === 3,
        );
        // The opener, applied without output, moves the head to 1; each answer's head then follows the answer in the stream the orchestrator persists in order.
        const trail = harness.allEvents.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [`answer:${event.turnItem.text}`]
            : event.type === "provider_thread.updated" &&
                event.providerThread.nativeConversationHeadRef?.strength === "strong"
              ? [`head:${event.providerThread.nativeConversationHeadRef.ordinal}`]
              : [],
        );
        assert.deepEqual(trail, [
          "head:0",
          "head:1",
          "answer:ONE",
          "head:2",
          "answer:TWO",
          "head:3",
        ]);

        // A session that ended right after the first head was persisted, as
        // the orchestrator stored it, reads the whole buffer back from there.
        const later = yield* driven();
        later.states({ running: true, runId: "A", seq: 4 });
        later.opensWith(
          prelude({ running: true, runId: "A", seq: 4 }, [
            { taskId: "marker", title: "Marker", status: "running" },
          ]),
        );
        later.replays(log);
        const resumed = yield* makeHarness(later.transport, "https://cell.test", {
          providerSessionId: ProviderSessionId.make("session-cell-2"),
          resume: owned.providerThread,
        });
        assert.deepEqual(later.replayRequests, ["/events?after=0&limit=1000"]);
        yield* resumed.runtime.startTurn(
          yield* turnInput(resumed.providerThread, "Continue where you left off.", 2, {
            createdBy: "agent",
            creationSource: "server",
            restartContinuationOfRunId: RunId.make("run-1"),
          }),
        );
        yield* later.send([{ type: "done", runId: "A", seq: 5 }]);
        const terminal = yield* resumed.takeEvent(
          "turn.terminal",
          (event) => event.runOrdinal === 2,
        );
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
        assert.deepEqual(assistantTexts(resumed.allEvents, 2), ["ONE", "TWO"]);
        assert.deepEqual(later.promptRequestIds, [], "the continuation posts nothing");
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "a wake adopted from its buffer persists the head behind each event applied, never ahead of it",
    () =>
      Effect.gen(function* () {
        const cell = yield* driven();
        cell.opensWith(prelude());
        const harness = yield* makeHarness(cell.transport, "https://cell.test");
        yield* cell.send([
          { type: "wake", text: "[Delegated task a done]", runId: "A", seq: 1, origin: "wake" },
          { type: "assistant", text: "FIRST", runId: "A", seq: 2 },
          { type: "assistant", text: "SECOND", runId: "A", seq: 3 },
          marker(4),
        ]);
        yield* awaitMarker(harness);
        const request = yield* harness.takeContinuation;
        yield* harness.runtime.startTurn(
          yield* turnInput(harness.providerThread, "", 1, {
            createdBy: "agent",
            creationSource: "provider",
            ...(request.messageId === undefined ? {} : { messageId: request.messageId }),
          }),
        );
        yield* harness.takeEvent(
          "provider_thread.updated",
          (event) => event.providerThread.nativeConversationHeadRef?.ordinal === 3,
        );
        const trail = harness.allEvents.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [`answer:${event.turnItem.text}`]
            : event.type === "provider_thread.updated" &&
                event.providerThread.nativeConversationHeadRef?.strength === "strong"
              ? [`head:${event.providerThread.nativeConversationHeadRef.ordinal}`]
              : [],
        );
        // The opener is consumed when the wake is queued, so the head starts at it, not at the marker beyond the buffer.
        assert.deepEqual(trail, ["head:1", "answer:FIRST", "head:2", "answer:SECOND", "head:3"]);
        yield* cell.send([{ type: "done", runId: "A", seq: 5 }]);
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "completed", failureOf(terminal)?.message);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  /** The thread as a session that owned run A up to number 2 left it, for a session to resume. */
  const threadOwningRun = (providerThread: OrchestrationV2ProviderThread) =>
    ({
      ...providerThread,
      nativeConversationHeadRef: {
        driver: CELL_PROVIDER,
        nativeId: "A",
        strength: "strong",
        fingerprint: "request-of-the-last-session",
        ordinal: 2,
      },
    }) satisfies OrchestrationV2ProviderThread;

  it.live(
    "a reset read back behind the last session's run fails its restart continuation naming the reset, without posting a prompt",
    () =>
      Effect.gen(function* () {
        const earlier = yield* driven();
        earlier.opensWith(prelude());
        const before = yield* makeHarness(earlier.transport, "https://cell.test");
        const cell = yield* driven();
        cell.states({ seq: 3 });
        cell.opensWith(prelude({ seq: 3 }));
        cell.replays([{ type: "reset", seq: 3 }]);
        const harness = yield* makeHarness(cell.transport, "https://cell.test", {
          providerSessionId: ProviderSessionId.make("session-cell-2"),
          resume: threadOwningRun(before.providerThread),
        });
        assert.deepEqual(cell.replayRequests, ["/events?after=2&limit=1000"]);
        assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
        yield* harness.runtime.startTurn(
          yield* turnInput(harness.providerThread, "Continue where you left off.", 2, {
            createdBy: "agent",
            creationSource: "server",
            restartContinuationOfRunId: RunId.make("run-1"),
          }),
        );
        const terminal = yield* harness.takeEvent("turn.terminal");
        assert.equal(terminal.status, "failed");
        assert.equal(failureOf(terminal)?.class, "transport_error");
        assert.include(failureOf(terminal)?.message ?? "", "reset");
        assert.include(failureOf(terminal)?.message ?? "", "T3 Code last stopped");
        assert.deepEqual(cell.promptRequestIds, [], "nothing is posted to the reset cell");
        assert.deepEqual(assistantTexts(harness.allEvents, 2), []);
        assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
        const settled = harness.allEvents.findLast(
          (event) => event.type === "provider_thread.updated",
        );
        assert.equal(
          settled?.type === "provider_thread.updated"
            ? settled.providerThread.nativeConversationHeadRef?.strength
            : undefined,
          "weak",
          "the reset run is no longer what a later session would take back",
        );
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );

  it.live(
    "with no restart continuation, a message after the reset posts as a prompt and the offered continuation reports the reset",
    () =>
      Effect.gen(function* () {
        const earlier = yield* driven();
        earlier.opensWith(prelude());
        const before = yield* makeHarness(earlier.transport, "https://cell.test");
        const cell = yield* driven();
        cell.states({ seq: 3 });
        cell.opensWith(prelude({ seq: 3 }));
        cell.replays([{ type: "reset", seq: 3 }]);
        const harness = yield* makeHarness(cell.transport, "https://cell.test", {
          providerSessionId: ProviderSessionId.make("session-cell-2"),
          resume: threadOwningRun(before.providerThread),
        });
        yield* cell.answerPrompt(202, '{"runId":"B","seq":4}');
        yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, "Again", 2));
        yield* cell.send([
          { type: "user", text: "Again", runId: "B", seq: 4, origin: "user" },
          { type: "assistant", text: "NEW", runId: "B", seq: 5 },
          { type: "done", runId: "B", seq: 6 },
        ]);
        const prompted = yield* harness.takeEvent(
          "turn.terminal",
          (event) => event.runOrdinal === 2,
        );
        assert.equal(prompted.status, "completed", failureOf(prompted)?.message);
        assert.deepEqual(assistantTexts(harness.allEvents, 2), ["NEW"]);
        const offered = yield* harness.takeContinuation;
        const reported = yield* runContinuation(harness, offered, 3);
        assert.equal(reported.status, "failed");
        assert.include(failureOf(reported)?.message ?? "", "reset");
        assert.deepEqual(assistantTexts(harness.allEvents, 3), []);
        assert.lengthOf(cell.promptRequestIds, 1, "only the typed message reaches the cell");
        assert.isFalse(yield* harness.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(drivenLayer)),
  );
});
