/**
 * One T3 thread is one cell project. The adapter posts prompts over HTTP and
 * reads the cell's broadcast events over its WebSocket, which it keeps open
 * for the life of the session and reopens whenever celld closes it. The cell
 * broadcasts the same events to every client, so nothing is attributed to a
 * T3 turn until the cell has confirmed which run is T3's: by run id where the
 * tenant sends one, by the echo of the prompt where it does not.
 * docs/internals/provider-cell.md records the rules.
 */
import {
  type MessageId,
  type NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItemStatus,
  type ProviderInstanceId,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunId,
  type ThreadId,
  type TurnItemId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  backgroundWorkNotification,
  type BackgroundWorkReport,
} from "@t3tools/provider-core/server/notification";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";

import type { CellSettings } from "../settings.ts";
import { CellModelError, CellRuntimeModeError } from "./errors.ts";
import {
  CELL_PROVIDER,
  CellEvent,
  CellState,
  cellApiUrl,
  cellAuthorizationHeaders,
  cellProjectName,
  cellSocketUrl,
  classifyCellDone,
  isCellRunEvent,
  isCellWakeEvent,
  isKnownCellEventType,
  readPromptAccepted,
  type CellRunEvent,
  type CellTask,
} from "./protocol.ts";
import type { CellTransport } from "./transport.ts";

const CellProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: false,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: true,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "none",
    nativeRequestIds: "none",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface CellAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: CellSettings;
  readonly transport: CellTransport;
  readonly requestTimeoutMs?: number;
  readonly nativeEventLogger?: ProviderEventLoggers.EventNdjsonLogger;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

/**
 * A drop after a healthy connection reconnects at once, since celld closes
 * sockets whenever a cell moves. Connections that keep dying young back off,
 * so a proxy that accepts and closes cannot drive a tight loop.
 */
const RECONNECT_DELAYS_MS = [0, 500, 1_000, 2_000, 5_000, 10_000] as const;
const HEALTHY_CONNECTION_MS = 30_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const EVENTS_PAGE_LIMIT = 1_000;
/** How long a lost prompt response waits for the prompt's echo before giving up on an unnumbered tenant. */
const LEGACY_ECHO_GRACE_MS = 2_000;
const FULL_ACCESS = "full-access";
const DEFAULT_MODEL_SENTINEL = "default";

const UNOBSERVED_END_MESSAGE =
  "The connection to the cell dropped while the turn ran; its result stayed in the cell. Send another message to continue the conversation there.";
const UNPROVABLE_TURN_MESSAGE =
  "The connection to the cell dropped while the turn ran, and this tenant does not number its runs, so T3 cannot tell whether the turn it sees is still this one. Its result stays in the cell; send another message to continue there.";
const WAKE_LOST_MESSAGE =
  "The cell finished this delegated work while T3 was disconnected; its result stayed in the cell. Send another message to continue the conversation there.";
const ALREADY_RUNNING_MESSAGE =
  "The cell is already running a turn started from another client. Wait for it to finish, then send the message again.";

interface TurnScope {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly rootNodeId: NodeId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly nativeId: string;
}

interface OpenItem {
  readonly ordinal: number;
  readonly startedAt: DateTime.Utc;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly scope: TurnScope;
  providerTurn: OrchestrationV2ProviderTurn;
  nextOrdinal: number;
  openShell: (OpenItem & { readonly command: string }) | undefined;
  readonly openTools: Array<OpenItem & { readonly name: string; readonly input: unknown }>;
  readonly done: Deferred.Deferred<void>;
  interruptRequested: boolean;
  /** `starting` while `POST prompt` is unanswered: run events are candidates, not yet T3's. */
  phase: "starting" | "running";
  /** The cell's id for this turn's run, once a 202 or an adopted wake named it. */
  nativeRunId: string | undefined;
  /** Without run ids, the echo of the prompt is where this turn's run begins on the socket. */
  echoed: boolean;
  /** Resolved when the echo shows up among the candidates. */
  readonly echo: Deferred.Deferred<void>;
  readonly candidates: Array<CellRunEvent>;
  /** Last sequence number seen before the prompt was posted, for recovering a lost response. */
  seqBeforePrompt: number | undefined;
}

/** A delegated task, remembered where it was first seen so later reports land on the same rows. */
interface TaskRecord {
  task: CellTask;
  readonly owner:
    | (TurnScope & OpenItem & { readonly nodeId: NodeId; readonly itemId: TurnItemId })
    | undefined;
  endedAt: DateTime.Utc | undefined;
}

/**
 * A run the cell started on its own after a child finished. Each one is
 * offered to the orchestrator as a continuation with its own message id, so
 * the continuation turn that starts for it adopts exactly its output.
 */
interface PendingWake {
  readonly messageId: MessageId;
  readonly runId: string | undefined;
  readonly events: Array<CellRunEvent>;
  ended: boolean;
  /** The run ended while T3 was disconnected, so the buffer is incomplete. */
  lost: boolean;
}

type Inbox =
  | { readonly type: "connected" }
  | { readonly type: "frame"; readonly text: string }
  | { readonly type: "disconnected"; readonly detail: string };

const decodeFrame = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeEvent = Schema.decodeUnknownEffect(CellEvent);
const decodeState = Schema.decodeUnknownEffect(Schema.fromJsonString(CellState));
const decodeEventList = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.Unknown)),
);

const taskStatus = (status: string): OrchestrationV2TurnItemStatus =>
  status === "completed"
    ? "completed"
    : status === "failed"
      ? "failed"
      : status === "cancelled"
        ? "cancelled"
        : "running";

const isSettled = (status: OrchestrationV2TurnItemStatus) =>
  status !== "running" && status !== "pending" && status !== "waiting";

const eventType = (raw: unknown) =>
  typeof raw === "object" && raw !== null && "type" in raw ? raw.type : undefined;

export const makeCellAdapterV2 = Effect.fn("makeCellAdapterV2")(function* (
  options: CellAdapterV2Options,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const { baseUrl, token, projectPrefix } = options.settings;
  const headers = cellAuthorizationHeaders(token);
  const requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: CELL_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });
  const nativeRef = (
    nativeId: string | null,
    strength: OrchestrationV2ProviderRef["strength"],
    ordinal?: number,
  ): OrchestrationV2ProviderRef => ({
    driver: CELL_PROVIDER,
    nativeId,
    strength,
    ...(ordinal === undefined ? {} : { ordinal }),
  });
  const checkRuntimeMode = (
    policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  ): Effect.Effect<void, CellRuntimeModeError> =>
    policy.runtimeMode === FULL_ACCESS
      ? Effect.void
      : Effect.fail(new CellRuntimeModeError({ runtimeMode: policy.runtimeMode }));

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: CELL_PROVIDER,
    getCapabilities: () => Effect.succeed(CellProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("CellAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      if (!options.settings.enabled) return yield* protocolError("Cell is disabled");
      if (!baseUrl) return yield* protocolError("Cell instance has no base URL");
      if (input.runtimePolicy.interactionMode === "plan")
        return yield* protocolError("Cell does not support a separate Plan mode");
      yield* checkRuntimeMode(input.runtimePolicy).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: CELL_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const scope = yield* Effect.scope;
      const now = yield* DateTime.now;
      let session: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: CELL_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd: input.runtimePolicy.cwd ?? "/",
        model:
          input.modelSelection.model === DEFAULT_MODEL_SENTINEL ? null : input.modelSelection.model,
        capabilities: CellProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const inbox = yield* Queue.unbounded<Inbox>();
      const commands = yield* Semaphore.make(1);
      const eventPermit = yield* Semaphore.make(1);
      let closed = false;
      let project: string | undefined;
      let tenantModel: string | undefined;
      let thread: OrchestrationV2ProviderThread | undefined;
      let active: ActiveTurn | undefined;
      let lastProviderTurn: OrchestrationV2ProviderTurn | undefined;
      let socketFiber: Fiber.Fiber<void> | undefined;
      let connections = 0;
      let awaitingState = false;
      /** Highest sequence number seen; undefined until the tenant sends one. */
      let lastSeq: number | undefined;
      let wakeCounter = 0;
      let ready: Deferred.Deferred<void, ProviderAdapter.ProviderAdapterProtocolError> | undefined;
      const tasks = new Map<string, TaskRecord>();
      const finishedBackground: Array<BackgroundWorkReport> = [];
      const wakes: Array<PendingWake> = [];
      /**
       * Rows published while no run subscribed. The session manager drops such
       * events, so they are published again when the next turn starts.
       */
      const republish = new Set<TaskRecord>();

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);
      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = null,
      ) {
        session = { ...session, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_session.updated",
          driver: CELL_PROVIDER,
          providerSession: session,
        });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (!thread) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_thread.updated",
          driver: CELL_PROVIDER,
          providerThread: thread,
        });
      });
      const request = (method: "GET" | "POST", path: string, body?: string) =>
        options.transport
          .request({
            method,
            url: cellApiUrl(baseUrl, project ?? "", path),
            headers: {
              ...headers,
              "x-client": `t3-${input.providerSessionId}`,
            },
            ...(body === undefined ? {} : { body }),
          })
          .pipe(
            Effect.timeout(requestTimeoutMs),
            Effect.mapError((cause) =>
              protocolError(
                cause._tag === "TimeoutError"
                  ? `Cell ${method} ${path} timed out`
                  : `Cell ${method} ${path} failed: ${cause.detail}`,
                cause,
              ),
            ),
          );
      const readState = Effect.fnUntraced(function* () {
        const response = yield* request("GET", "/state");
        if (response.status === 401 || response.status === 403)
          return yield* protocolError("The cell tenant rejected the access token");
        if (response.status !== 200)
          return yield* protocolError(`The cell answered ${response.status} to GET state`);
        return yield* decodeState(response.body).pipe(
          Effect.mapError((cause) =>
            protocolError(`${baseUrl} answered GET state without a cell state`, cause),
          ),
        );
      });

      const itemIdentity = (scopeOf: TurnScope, ordinal: number) =>
        `${options.instanceId}:${scopeOf.providerThreadId}:${scopeOf.nativeId}:${ordinal}`;
      const itemIds = (scopeOf: TurnScope, ordinal: number) => {
        const identity = itemIdentity(scopeOf, ordinal);
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver: CELL_PROVIDER,
            nativeItemId: identity,
          }),
          nodeId: idAllocator.derive.nodeFromProviderItem({
            driver: CELL_PROVIDER,
            nativeItemId: identity,
          }),
          messageId: idAllocator.derive.messageFromProviderItem({
            driver: CELL_PROVIDER,
            nativeItemId: identity,
          }),
        };
      };
      const baseItem = (
        scopeOf: TurnScope,
        ordinal: number,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        ...itemIds(scopeOf, ordinal),
        threadId: scopeOf.threadId,
        runId: scopeOf.runId,
        providerThreadId: scopeOf.providerThreadId,
        providerTurnId: scopeOf.providerTurnId,
        nativeItemRef: nativeRef(null, "none", ordinal),
        parentItemId: null,
        ordinal,
        startedAt,
        updatedAt,
      });
      const emitNode = (
        scopeOf: TurnScope,
        base: ReturnType<typeof baseItem>,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2TurnItemStatus,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: CELL_PROVIDER,
          node: {
            id: base.nodeId,
            threadId: scopeOf.threadId,
            runId: scopeOf.runId,
            parentNodeId: scopeOf.rootNodeId,
            rootNodeId: scopeOf.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: scopeOf.providerThreadId,
            providerTurnId: scopeOf.providerTurnId,
            nativeItemRef: base.nativeItemRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: base.startedAt,
            completedAt,
          },
        });
      const allocate = (turn: ActiveTurn) => turn.nextOrdinal++;

      const publishAssistant = Effect.fnUntraced(function* (turn: ActiveTurn, text: string) {
        const time = yield* DateTime.now;
        const base = baseItem(turn.scope, allocate(turn), time, time);
        const message: OrchestrationV2ConversationMessage = {
          id: base.messageId,
          threadId: turn.scope.threadId,
          runId: turn.scope.runId,
          nodeId: base.nodeId,
          role: "assistant",
          text,
          attachments: [],
          streaming: false,
          createdBy: "agent",
          creationSource: "provider",
          createdAt: time,
          updatedAt: time,
        };
        yield* emit({ type: "message.updated", driver: CELL_PROVIDER, message });
        yield* emitNode(turn.scope, base, "assistant_message", "completed", time);
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            ...base,
            type: "assistant_message",
            status: "completed",
            title: null,
            completedAt: time,
            messageId: base.messageId,
            text,
            streaming: false,
          },
        });
      });
      const publishShell = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        item: OpenItem & { readonly command: string },
        status: OrchestrationV2TurnItemStatus,
        output?: { readonly text: string; readonly exitCode: number },
      ) {
        const time = yield* DateTime.now;
        const base = baseItem(turn.scope, item.ordinal, item.startedAt, time);
        const completedAt = isSettled(status) ? time : null;
        yield* emitNode(turn.scope, base, "tool_call", status, completedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            ...base,
            type: "command_execution",
            status,
            title: "shell",
            completedAt,
            input: item.command,
            ...(output === undefined
              ? {}
              : {
                  output: output.text,
                  exitCode: output.exitCode,
                  outputIndicatesFailure: output.exitCode !== 0,
                }),
          },
        });
      });
      const publishTool = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        item: OpenItem & { readonly name: string; readonly input: unknown },
        status: OrchestrationV2TurnItemStatus,
        output?: unknown,
      ) {
        const time = yield* DateTime.now;
        const base = baseItem(turn.scope, item.ordinal, item.startedAt, time);
        const completedAt = isSettled(status) ? time : null;
        yield* emitNode(turn.scope, base, "tool_call", status, completedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            ...base,
            type: "dynamic_tool",
            status,
            title: item.name,
            completedAt,
            toolName: item.name,
            input: item.input,
            ...(output === undefined ? {} : { output }),
          },
        });
      });
      const publishPatch = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        operation: string,
        path: string,
      ) {
        const time = yield* DateTime.now;
        const base = baseItem(turn.scope, allocate(turn), time, time);
        yield* emitNode(turn.scope, base, "tool_call", "completed", time);
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            ...base,
            type: "file_change",
            status: "completed",
            title: operation,
            completedAt: time,
            fileName: path,
            changes: [{ operation, path }],
          },
        });
      });
      const publishNotice = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        title: string,
        message: string,
      ) {
        const time = yield* DateTime.now;
        const base = baseItem(turn.scope, allocate(turn), time, time);
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            ...base,
            type: "system_notice",
            status: "completed",
            title,
            completedAt: time,
            message,
          },
        });
      });
      const roster = () =>
        [...tasks.values()].flatMap((record) =>
          taskStatus(record.task.status) === "running"
            ? [
                {
                  taskId: record.task.taskId,
                  kind: "subagent" as const,
                  ...(record.task.title ? { description: record.task.title } : {}),
                },
              ]
            : [],
        );
      const syncBackground = () => updateThread({ pendingBackgroundTasks: roster() });
      const publishTask = Effect.fnUntraced(function* (record: TaskRecord) {
        const owner = record.owner;
        if (!owner) return;
        if (!active) republish.add(record);
        const time = yield* DateTime.now;
        const status = taskStatus(record.task.status);
        if (isSettled(status) && record.endedAt === undefined) record.endedAt = time;
        const completedAt = isSettled(status) ? (record.endedAt ?? time) : null;
        const title = record.task.title?.trim() || `Task ${record.task.taskId}`;
        const result = record.task.summary?.trim() || null;
        const taskRef = nativeRef(record.task.taskId, "strong");
        const subagent: OrchestrationV2Subagent = {
          id: owner.nodeId,
          threadId: owner.threadId,
          runId: owner.runId,
          parentNodeId: owner.rootNodeId,
          origin: "provider_native",
          createdBy: "agent",
          driver: CELL_PROVIDER,
          providerInstanceId: options.instanceId,
          providerThreadId: owner.providerThreadId,
          childThreadId: null,
          nativeTaskRef: taskRef,
          prompt: title,
          title,
          model: null,
          status,
          result,
          startedAt: owner.startedAt,
          completedAt,
          updatedAt: time,
        };
        yield* emit({ type: "subagent.updated", driver: CELL_PROVIDER, subagent });
        yield* emit({
          type: "node.updated",
          driver: CELL_PROVIDER,
          node: {
            id: owner.nodeId,
            threadId: owner.threadId,
            runId: owner.runId,
            parentNodeId: owner.rootNodeId,
            rootNodeId: owner.rootNodeId,
            kind: "subagent",
            status,
            countsForRun: false,
            providerThreadId: owner.providerThreadId,
            providerTurnId: owner.providerTurnId,
            nativeItemRef: taskRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: owner.startedAt,
            completedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: CELL_PROVIDER,
          turnItem: {
            id: owner.itemId,
            nodeId: owner.nodeId,
            threadId: owner.threadId,
            runId: owner.runId,
            providerThreadId: owner.providerThreadId,
            providerTurnId: owner.providerTurnId,
            nativeItemRef: taskRef,
            parentItemId: null,
            ordinal: owner.ordinal,
            status,
            title,
            startedAt: owner.startedAt,
            completedAt,
            updatedAt: time,
            type: "subagent",
            subagentId: owner.nodeId,
            origin: "provider_native",
            driver: CELL_PROVIDER,
            providerInstanceId: options.instanceId,
            childThreadId: null,
            prompt: title,
            result,
          },
        });
      });
      const observeTask = Effect.fnUntraced(function* (task: CellTask) {
        const previous = tasks.get(task.taskId);
        const previousStatus = previous ? taskStatus(previous.task.status) : undefined;
        let record = previous;
        if (!record) {
          let owner: TaskRecord["owner"];
          if (active) {
            const startedAt = yield* DateTime.now;
            const ordinal = allocate(active);
            const ids = itemIds(active.scope, ordinal);
            owner = {
              ...active.scope,
              ordinal,
              startedAt,
              nodeId: ids.nodeId,
              itemId: ids.id,
            };
          }
          record = { task, owner, endedAt: undefined };
          tasks.set(task.taskId, record);
        } else {
          record.task = task;
        }
        const status = taskStatus(task.status);
        yield* publishTask(record);
        if (isSettled(status) && previousStatus === "running" && record.owner && !active) {
          finishedBackground.push({
            kind: "subagent",
            label: task.title,
            outcome:
              status === "completed" ? "completed" : status === "failed" ? "failed" : "cancelled",
          });
        }
        if (status !== previousStatus) yield* syncBackground();
      });
      /** The snapshot is the whole roster: a task it no longer lists ended while nobody listened. */
      const reconcileTasks = Effect.fnUntraced(function* (snapshot: ReadonlyArray<CellTask>) {
        const listed = new Set(snapshot.map((task) => task.taskId));
        let changed = false;
        for (const task of snapshot) {
          const record = tasks.get(task.taskId);
          if (record) {
            changed ||= record.task.status !== task.status;
            record.task = task;
            yield* publishTask(record);
          } else {
            changed = true;
            tasks.set(task.taskId, { task, owner: undefined, endedAt: undefined });
          }
        }
        for (const [taskId, record] of tasks) {
          if (listed.has(taskId)) continue;
          if (taskStatus(record.task.status) === "running") {
            changed = true;
            record.task = { ...record.task, status: "cancelled" };
            yield* publishTask(record);
          }
          tasks.delete(taskId);
        }
        if (changed) yield* syncBackground();
      });
      const settleTasks = Effect.fnUntraced(function* (status: "cancelled" | "failed") {
        for (const record of tasks.values()) {
          if (taskStatus(record.task.status) !== "running") continue;
          record.task = { ...record.task, status };
          yield* publishTask(record);
        }
        if (thread?.pendingBackgroundTasks?.length) yield* syncBackground();
      });
      const closeOpenItems = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: OrchestrationV2TurnItemStatus,
      ) {
        if (turn.openShell) {
          yield* publishShell(turn, turn.openShell, status);
          turn.openShell = undefined;
        }
        for (const tool of turn.openTools.splice(0)) yield* publishTool(turn, tool, status);
      });
      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "cancelled" | "interrupted" | "failed",
        detail?: string,
        failureClass:
          | "usage_limit"
          | "provider_error"
          | "transport_error"
          | "validation_error"
          | "permission_error" = "provider_error",
        disposition: "reusable" | "broken" = "reusable",
        failureDetail: { readonly code?: string | null; readonly resetAt?: string | null } = {},
      ) {
        if (active !== turn) return;
        const completedAt = yield* DateTime.now;
        yield* closeOpenItems(turn, status === "completed" ? "failed" : status);
        turn.providerTurn = { ...turn.providerTurn, status, completedAt };
        lastProviderTurn = turn.providerTurn;
        yield* emit({
          type: "provider_turn.updated",
          driver: CELL_PROVIDER,
          threadId: turn.scope.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({
          status: disposition === "broken" ? "error" : "idle",
          ...(status === "completed"
            ? { nativeConversationHeadRef: nativeRef(turn.scope.nativeId, "weak") }
            : {}),
        });
        yield* updateSession(disposition === "broken" ? "error" : "ready", detail ?? null);
        active = undefined;
        if (status === "failed") {
          const failure = makeProviderFailure({
            class: failureClass,
            message: detail ?? "Cell turn failed.",
            code: failureDetail.code ?? null,
            resetAt: failureDetail.resetAt ?? null,
          });
          const base = baseItem(turn.scope, allocate(turn), completedAt, completedAt);
          yield* emit({
            type: "turn_item.updated",
            driver: CELL_PROVIDER,
            turnItem: {
              ...base,
              type: "error",
              status: "failed",
              title: failureClass === "usage_limit" ? "Usage limit reached" : null,
              completedAt,
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: CELL_PROVIDER,
            providerThreadId: turn.scope.providerThreadId,
            providerTurnId: turn.scope.providerTurnId,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure,
            failureItemOrdinal: base.ordinal,
            threadDisposition: disposition,
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: CELL_PROVIDER,
            providerThreadId: turn.scope.providerThreadId,
            providerTurnId: turn.scope.providerTurnId,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure: null,
            threadDisposition: disposition,
          });
        }
        yield* Deferred.succeed(turn.done, undefined);
      });

      const dropWake = (wake: PendingWake) =>
        Effect.sync(() => {
          const index = wakes.indexOf(wake);
          if (index !== -1) wakes.splice(index, 1);
        });
      const enqueueWake = Effect.fnUntraced(function* (runId: string | undefined) {
        const key =
          runId === undefined
            ? `${input.providerSessionId}:wake:${++wakeCounter}`
            : `${project}:run:${runId}`;
        const wake: PendingWake = {
          messageId: idAllocator.derive.messageFromProviderItem({
            driver: CELL_PROVIDER,
            nativeItemId: `${options.instanceId}:${key}`,
          }),
          runId,
          events: [],
          ended: false,
          lost: false,
        };
        wakes.push(wake);
        if (!thread?.appThreadId || !options.continuationRequests) return wake;
        const notification = backgroundWorkNotification(finishedBackground.splice(0));
        yield* options.continuationRequests.offer({
          threadId: thread.appThreadId,
          providerThreadId: thread.id,
          driver: CELL_PROVIDER,
          detail: null,
          delivery: "adapter_buffered",
          messageId: wake.messageId,
          ...(notification ? { notification } : {}),
          dispatchIfCurrent: (dispatch) =>
            eventPermit.withPermits(1)(
              Effect.suspend(() =>
                wakes.includes(wake) && !closed
                  ? dispatch.pipe(
                      Effect.asSome,
                      Effect.tapError(() => dropWake(wake)),
                    )
                  : Effect.succeedNone,
              ),
            ),
          clearIfCurrent: () => eventPermit.withPermits(1)(dropWake(wake)),
        });
        return wake;
      });
      /** Run events with no T3 owner: a wake starts a queued run, its followers fill it, the rest is noise. */
      const routeUnowned = Effect.fnUntraced(function* (event: CellRunEvent) {
        if (isCellWakeEvent(event)) {
          yield* enqueueWake(event.runId);
          return;
        }
        const wake =
          event.runId === undefined
            ? wakes.findLast((candidate) => candidate.runId === undefined && !candidate.ended)
            : wakes.find((candidate) => candidate.runId === event.runId);
        if (wake === undefined || wake.ended) {
          return yield* Effect.logDebug(`Ignored a cell ${event.type} event with no owner`);
        }
        wake.events.push(event);
        if (event.type === "done") wake.ended = true;
      });
      const applyToTurn = Effect.fnUntraced(function* (turn: ActiveTurn, event: CellRunEvent) {
        switch (event.type) {
          case "user":
            return;
          case "wake":
            return yield* publishNotice(turn, "Delegated task", event.text);
          case "assistant":
            return yield* publishAssistant(turn, event.text);
          case "shell": {
            if (turn.openShell) yield* publishShell(turn, turn.openShell, "completed");
            const item = {
              ordinal: allocate(turn),
              startedAt: yield* DateTime.now,
              command: event.command,
            };
            turn.openShell = item;
            return yield* publishShell(turn, item, "running");
          }
          case "shell_output": {
            const item = turn.openShell ?? {
              ordinal: allocate(turn),
              startedAt: yield* DateTime.now,
              command: event.command,
            };
            turn.openShell = undefined;
            const text = [event.stdout, event.stderr, event.timedOut ? "[timed out]" : ""]
              .filter((part) => part.length > 0)
              .join(event.stdout.endsWith("\n") || !event.stdout ? "" : "\n");
            // After Stop the cell aborts the command; its output is the abort, not a result.
            return yield* publishShell(
              turn,
              item,
              turn.interruptRequested ? "interrupted" : "completed",
              { text, exitCode: event.exitCode },
            );
          }
          case "tool": {
            const item = {
              ordinal: allocate(turn),
              startedAt: yield* DateTime.now,
              name: event.name,
              input: event.arguments,
            };
            turn.openTools.push(item);
            return yield* publishTool(turn, item, "running");
          }
          case "tool_output": {
            const index = turn.openTools.findIndex((tool) => tool.name === event.name);
            const item =
              index === -1
                ? {
                    ordinal: allocate(turn),
                    startedAt: yield* DateTime.now,
                    name: event.name,
                    input: null,
                  }
                : turn.openTools.splice(index, 1)[0]!;
            return yield* publishTool(turn, item, "completed", parseToolOutput(event.output));
          }
          case "patch":
            return yield* publishPatch(turn, event.operation, event.path);
          case "error":
            return yield* publishNotice(turn, "Cell error", event.text);
          case "done": {
            const outcome = classifyCellDone(event.error);
            if (outcome.kind === "completed") return yield* finish(turn, "completed");
            if (outcome.kind === "cancelled")
              return yield* finish(turn, turn.interruptRequested ? "interrupted" : "cancelled");
            return yield* finish(
              turn,
              "failed",
              outcome.message,
              outcome.failureClass,
              "reusable",
              { code: outcome.code, resetAt: outcome.resetAt },
            );
          }
        }
      });
      const isEcho = (turn: ActiveTurn, event: CellRunEvent): boolean =>
        event.type === "user" &&
        event.origin !== "wake" &&
        event.text.trim() === turn.input.message.text.trim();
      const handleRunEvent = Effect.fnUntraced(function* (event: CellRunEvent) {
        const turn = active;
        if (turn !== undefined) {
          if (turn.phase === "starting") {
            turn.candidates.push(event);
            if (isEcho(turn, event)) yield* Deferred.succeed(turn.echo, undefined);
            return;
          }
          if (event.runId !== undefined && turn.nativeRunId !== undefined) {
            if (event.runId === turn.nativeRunId) return yield* applyToTurn(turn, event);
            // The cell runs one agent loop at a time, so another run's event
            // means T3's run ended while its `done` never arrived.
            yield* finish(turn, "failed", UNOBSERVED_END_MESSAGE, "transport_error");
            return yield* routeUnowned(event);
          }
          if (event.runId === undefined && turn.nativeRunId === undefined) {
            if (turn.echoed) return yield* applyToTurn(turn, event);
            if (isEcho(turn, event)) {
              turn.echoed = true;
              return;
            }
            // Before the echo, the socket is still delivering an earlier run's tail.
            return yield* routeUnowned(event);
          }
          return yield* Effect.logWarning(
            `Ignored a cell ${event.type} event whose run identification does not match the active turn`,
          );
        }
        return yield* routeUnowned(event);
      });
      /** Candidates the 202 or 409 proved foreign still matter when they are a wake's run. */
      const releaseCandidates = Effect.fnUntraced(function* (events: ReadonlyArray<CellRunEvent>) {
        for (const event of events) yield* routeUnowned(event);
      });
      const accept = Effect.fnUntraced(function* (turn: ActiveTurn, runId: string | undefined) {
        const candidates = turn.candidates.splice(0);
        turn.phase = "running";
        turn.nativeRunId = runId;
        if (runId !== undefined) {
          yield* releaseCandidates(candidates.filter((event) => event.runId !== runId));
          for (const event of candidates) {
            if (event.runId === runId && active === turn) yield* applyToTurn(turn, event);
          }
          return;
        }
        const echo = candidates.findIndex((event) => isEcho(turn, event));
        yield* releaseCandidates(echo === -1 ? candidates : candidates.slice(0, echo));
        if (echo === -1) return;
        turn.echoed = true;
        for (const event of candidates.slice(echo + 1)) {
          if (active === turn) yield* applyToTurn(turn, event);
        }
      });
      const handleEvent: (event: CellEvent) => Effect.Effect<void> = Effect.fnUntraced(function* (
        event: CellEvent,
      ) {
        if (event.type === "state") return yield* handleState(event);
        if (event.seq !== undefined) {
          if (lastSeq !== undefined && event.seq <= lastSeq) return;
          lastSeq = event.seq;
        }
        switch (event.type) {
          case "tasks":
            return yield* reconcileTasks(event.tasks);
          case "task": {
            if ("removed" in event.task) {
              const record = tasks.get(event.task.taskId);
              if (!record) return;
              if (taskStatus(record.task.status) === "running") {
                yield* observeTask({ ...record.task, status: "cancelled" });
              }
              tasks.delete(event.task.taskId);
              yield* syncBackground();
              return;
            }
            return yield* observeTask(event.task);
          }
          case "reset":
            wakes.splice(0);
            if (active) {
              active.candidates.splice(0);
              yield* finish(
                active,
                "failed",
                "The cell conversation was reset from another client while the turn ran.",
              );
            }
            return;
          case "fs":
          case "exec":
            return;
          default:
            if (isCellRunEvent(event)) return yield* handleRunEvent(event);
            return;
        }
      });
      const handleRaw: (
        raw: unknown,
      ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterProtocolError> = Effect.fnUntraced(
        function* (raw: unknown) {
          const type = eventType(raw);
          if (!isKnownCellEventType(type)) return;
          if (options.nativeEventLogger)
            yield* options.nativeEventLogger.write(
              { provider: "cell", event: raw },
              input.threadId,
            );
          const event = yield* decodeEvent(raw).pipe(
            Effect.mapError((cause) =>
              protocolError(`The cell sent an unreadable ${type} event`, cause),
            ),
          );
          yield* handleEvent(event);
        },
      );
      /** Fetches the persisted events T3 missed; false when the tenant could not supply them. */
      const catchUp = Effect.fnUntraced(function* (after: number, target: number) {
        let cursor = after;
        while (cursor < target) {
          const response = yield* request(
            "GET",
            `/events?after=${cursor}&limit=${EVENTS_PAGE_LIMIT}`,
          ).pipe(Effect.result);
          if (Result.isFailure(response) || response.success.status !== 200) return false;
          const page = yield* decodeEventList(response.success.body).pipe(Effect.result);
          if (Result.isFailure(page) || page.success.length === 0) return false;
          const before = cursor;
          for (const raw of page.success) {
            if (eventType(raw) === "state") continue;
            yield* handleRaw(raw).pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Skipped an unreadable replayed cell event", cause),
              ),
            );
            const seq = typeof raw === "object" && raw !== null ? Reflect.get(raw, "seq") : null;
            if (typeof seq === "number" && seq > cursor) cursor = seq;
          }
          if (cursor === before) return false;
        }
        return true;
      });
      const handleState: (
        event: Extract<CellEvent, { readonly type: "state" }>,
      ) => Effect.Effect<void> = Effect.fnUntraced(function* (
        event: Extract<CellEvent, { readonly type: "state" }>,
      ) {
        const reconnected = awaitingState && connections > 1;
        awaitingState = false;
        if (ready) yield* Deferred.succeed(ready, undefined);
        if (!reconnected) {
          if (event.seq !== undefined && lastSeq === undefined) lastSeq = event.seq;
          return;
        }
        // Events sent while the socket was down are gone unless the tenant
        // numbers them, in which case they are read back before judging.
        let caughtUp = false;
        if (event.seq !== undefined && lastSeq !== undefined) {
          caughtUp = event.seq <= lastSeq || (yield* catchUp(lastSeq, event.seq));
          if (lastSeq < event.seq) lastSeq = event.seq;
        }
        const numbered = event.runId !== undefined;
        const turn = active;
        if (turn !== undefined && turn.phase === "running") {
          const alive = event.running && numbered && event.runId === turn.nativeRunId;
          if (!alive) {
            yield* finish(
              turn,
              "failed",
              numbered ? UNOBSERVED_END_MESSAGE : UNPROVABLE_TURN_MESSAGE,
              "transport_error",
            );
          } else if (!caughtUp) {
            yield* publishNotice(
              turn,
              "Connection dropped",
              "The connection to the cell dropped and came back while this turn ran; output from the gap may be missing here. The cell's own transcript has all of it.",
            );
          }
        }
        for (const wake of wakes) {
          if (wake.ended) continue;
          if (!(event.running && numbered && event.runId === wake.runId)) wake.lost = true;
        }
      });
      const handleFrame = Effect.fnUntraced(function* (text: string) {
        const raw = yield* decodeFrame(text).pipe(
          Effect.mapError((cause) =>
            protocolError("The cell sent a frame that is not JSON", cause),
          ),
        );
        yield* handleRaw(raw);
      });

      // Connection boundaries and frames are processed in the order the socket
      // produced them, so a `done` read just before a close is never skipped
      // because the producer has already moved on to the next connection.
      yield* Stream.fromQueue(inbox).pipe(
        Stream.runForEach((entry) =>
          Effect.gen(function* () {
            if (closed) return;
            switch (entry.type) {
              case "connected":
                connections += 1;
                awaitingState = true;
                return;
              case "disconnected":
                awaitingState = false;
                // Failing `ready` here, after the connection's frames, means a
                // `state` that did arrive before the close still opens the thread.
                if (ready) {
                  yield* Deferred.fail(
                    ready,
                    protocolError(`Could not connect to the cell: ${entry.detail}`),
                  );
                }
                return yield* Effect.logDebug(`Cell socket closed: ${entry.detail}`);
              case "frame":
                return yield* handleFrame(entry.text).pipe(
                  Effect.catch((cause) =>
                    Effect.logWarning("Skipped an unreadable cell event", cause),
                  ),
                );
            }
          }).pipe(eventPermit.withPermits(1)),
        ),
        Effect.forkIn(scope),
      );

      const runSocket = Effect.fnUntraced(function* (projectName: string) {
        const url = cellSocketUrl(baseUrl, projectName, `t3-${input.providerSessionId}`);
        let failures = 0;
        for (;;) {
          if (closed) return;
          let connectedAt: number | undefined;
          const outcome = yield* Effect.scoped(
            Effect.gen(function* () {
              const connection = yield* options.transport.connect({ url, headers });
              connectedAt = yield* Clock.currentTimeMillis;
              yield* Queue.offer(inbox, { type: "connected" });
              while (true) {
                const frames = yield* connection.pull;
                for (const text of frames) yield* Queue.offer(inbox, { type: "frame", text });
              }
            }),
          ).pipe(Effect.result);
          if (closed) return;
          const detail = Result.isFailure(outcome)
            ? `${outcome.failure.detail} (${url})`
            : `socket ended (${url})`;
          yield* Queue.offer(inbox, { type: "disconnected", detail });
          const now = yield* Clock.currentTimeMillis;
          const healthy = connectedAt !== undefined && now - connectedAt >= HEALTHY_CONNECTION_MS;
          failures = healthy ? 0 : failures + 1;
          const delay =
            RECONNECT_DELAYS_MS[Math.min(failures, RECONNECT_DELAYS_MS.length - 1)] ?? 0;
          if (delay > 0) yield* Effect.sleep(delay);
        }
      });

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          wakes.splice(0);
          if (active) {
            active.candidates.splice(0);
            yield* finish(active, "cancelled", "Cell session closed", "provider_error", "broken");
          }
          yield* settleTasks("cancelled");
          yield* Queue.shutdown(inbox);
          // `end`, not `shutdown`: the terminal just emitted must still reach the consumer.
          yield* Queue.end(events);
        }).pipe(eventPermit.withPermits(1)),
      );

      const checkModel = (model: string): Effect.Effect<void, CellModelError> =>
        model === DEFAULT_MODEL_SENTINEL || tenantModel === undefined || model === tenantModel
          ? Effect.void
          : Effect.fail(new CellModelError({ requested: model, available: tenantModel }));
      const register = Effect.fnUntraced(function* (
        args: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (closed) return yield* protocolError("Cell session is closed; reopen it");
        if (active) return yield* protocolError("Cannot change cell threads during an active turn");
        yield* checkRuntimeMode(args.runtimePolicy);
        const existing = args.existingProviderThread;
        if (
          existing &&
          (existing.driver !== CELL_PROVIDER || existing.providerInstanceId !== options.instanceId)
        )
          return yield* protocolError("Cell thread belongs to another provider instance");
        const requested =
          existing?.nativeThreadRef?.nativeId ??
          input.initialNativeThreadId ??
          (yield* cellProjectName(projectPrefix, args.threadId));
        if (thread && project === requested && thread.appThreadId === args.threadId) {
          yield* checkModel(args.modelSelection.model);
          return thread;
        }
        if (thread) return yield* protocolError("Open a new session to attach another cell");
        project = requested;
        const state = yield* readState().pipe(
          Effect.onError(() =>
            Effect.sync(() => {
              project = undefined;
            }),
          ),
        );
        tenantModel = state.model;
        yield* checkModel(args.modelSelection.model).pipe(
          Effect.onError(() =>
            Effect.sync(() => {
              project = undefined;
            }),
          ),
        );
        const createdAt = yield* DateTime.now;
        thread = existing
          ? {
              ...existing,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: nativeRef(requested, "strong"),
              status: "idle",
              updatedAt: createdAt,
            }
          : {
              id: idAllocator.derive.providerThread({
                driver: CELL_PROVIDER,
                providerInstanceId: options.instanceId,
                nativeThreadId: requested,
              }),
              driver: CELL_PROVIDER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: args.threadId,
              ownerNodeId: null,
              nativeThreadRef: nativeRef(requested, "strong"),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
              updatedAt: createdAt,
            };
        const connected = yield* Deferred.make<
          void,
          ProviderAdapter.ProviderAdapterProtocolError
        >();
        ready = connected;
        socketFiber = yield* runSocket(requested).pipe(Effect.forkIn(scope));
        yield* Deferred.await(connected).pipe(
          Effect.timeout(requestTimeoutMs),
          Effect.catchTags({
            TimeoutError: (cause) =>
              protocolError(`The cell at ${baseUrl} did not answer the socket in time`, cause),
          }),
          Effect.onError(() =>
            Effect.gen(function* () {
              ready = undefined;
              thread = undefined;
              project = undefined;
              const fiber = socketFiber;
              socketFiber = undefined;
              closed = true;
              if (fiber) yield* Fiber.interrupt(fiber);
              closed = false;
            }),
          ),
        );
        ready = undefined;
        yield* emit({
          type: "provider_thread.updated",
          driver: CELL_PROVIDER,
          providerThread: thread,
        });
        return thread;
      });
      const isActive = (turn: ActiveTurn) => active === turn;
      const validateThread = (candidate: OrchestrationV2ProviderThread) =>
        thread !== undefined &&
        candidate.driver === CELL_PROVIDER &&
        candidate.providerInstanceId === options.instanceId &&
        candidate.appThreadId === thread.appThreadId &&
        candidate.nativeThreadRef?.nativeId === project;
      const UNPROVABLE_PROMPT_MESSAGE = (detail: string) =>
        `The cell did not confirm the prompt and is running a turn T3 cannot prove is this one: ${detail}. Its result stays in the cell; send another message once it is idle.`;
      const acceptEchoed = Effect.fnUntraced(function* (turn: ActiveTurn, detail: string) {
        const echoed = turn.candidates.find((event) => isEcho(turn, event));
        if (echoed !== undefined) return yield* accept(turn, echoed.runId);
        yield* releaseCandidates(turn.candidates.splice(0));
        return yield* finish(turn, "failed", UNPROVABLE_PROMPT_MESSAGE(detail), "transport_error");
      });
      /**
       * The prompt's answer never came. The cell may still have started the
       * run: its echo among the candidates, or its id in the cell's state and
       * event log, proves it. An unnumbered tenant that is running gets a short
       * grace for the echo to arrive; anything less fails the turn.
       */
      const resolveLostPrompt = Effect.fnUntraced(function* (turn: ActiveTurn, detail: string) {
        const echoed = turn.candidates.find((event) => isEcho(turn, event));
        if (echoed !== undefined) {
          yield* accept(turn, echoed.runId);
          return "settled" as const;
        }
        const state = yield* readState().pipe(Effect.result);
        if (Result.isFailure(state)) {
          yield* releaseCandidates(turn.candidates.splice(0));
          yield* finish(turn, "failed", detail, "transport_error");
          return "settled" as const;
        }
        if (!state.success.running) {
          yield* releaseCandidates(turn.candidates.splice(0));
          yield* finish(
            turn,
            "failed",
            `The cell did not confirm the prompt and is idle, so the message most likely never started: ${detail}. Send it again.`,
            "transport_error",
          );
          return "settled" as const;
        }
        const runId = state.success.runId ?? undefined;
        if (runId === undefined) return "awaiting_echo" as const;
        if (turn.seqBeforePrompt !== undefined) {
          const target = state.success.seq ?? turn.seqBeforePrompt;
          if (target > turn.seqBeforePrompt) yield* catchUp(turn.seqBeforePrompt, target);
        }
        const replayed = turn.candidates.find(
          (event) => isEcho(turn, event) && event.runId === runId,
        );
        if (replayed !== undefined && isActive(turn)) {
          yield* accept(turn, runId);
        } else if (isActive(turn)) {
          yield* releaseCandidates(turn.candidates.splice(0));
          yield* finish(turn, "failed", UNPROVABLE_PROMPT_MESSAGE(detail), "transport_error");
        }
        return "settled" as const;
      });
      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        if (!validateThread(turnInput.providerThread) || closed || project === undefined)
          return yield* protocolError("Cell thread is not attached to this session");
        if (active)
          return yield* protocolError(
            "The cell already has an active turn; queue through the orchestrator",
          );
        if (turnInput.runtimePolicy.interactionMode === "plan")
          return yield* protocolError("Cell does not support a separate Plan mode");
        const continuation =
          turnInput.message.createdBy === "agent" &&
          turnInput.message.creationSource === "provider";
        const nativeId = `${project}:turn:${turnInput.providerTurnOrdinal}`;
        const startedAt = yield* DateTime.now;
        const providerTurn: OrchestrationV2ProviderTurn = {
          id: idAllocator.derive.providerTurn({
            driver: CELL_PROVIDER,
            nativeTurnId: `${options.instanceId}:${nativeId}`,
          }),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: nativeRef(nativeId, "weak", turnInput.providerTurnOrdinal),
          ordinal: turnInput.providerTurnOrdinal,
          status: "running",
          startedAt,
          completedAt: null,
        };
        const turn: ActiveTurn = {
          input: turnInput,
          scope: {
            threadId: turnInput.threadId,
            runId: turnInput.runId,
            rootNodeId: turnInput.rootNodeId,
            providerThreadId: turnInput.providerThread.id,
            providerTurnId: providerTurn.id,
            nativeId,
          },
          providerTurn,
          nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
          openShell: undefined,
          openTools: [],
          done: yield* Deferred.make<void>(),
          interruptRequested: false,
          phase: "starting",
          nativeRunId: undefined,
          echoed: false,
          echo: yield* Deferred.make<void>(),
          candidates: [],
          seqBeforePrompt: undefined,
        };
        yield* Effect.gen(function* () {
          thread = turnInput.providerThread;
          active = turn;
          yield* emit({
            type: "provider_turn.updated",
            driver: CELL_PROVIDER,
            threadId: turnInput.threadId,
            providerTurn,
          });
          // The orchestrator's copy of the thread predates anything that
          // happened while idle; the roster comes from the adapter's own table.
          yield* updateThread({
            status: "active",
            firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
            pendingBackgroundTasks: roster(),
          });
          yield* updateSession("running");
          for (const record of republish) yield* publishTask(record);
          republish.clear();
          if (turnInput.runtimePolicy.runtimeMode !== FULL_ACCESS) {
            return yield* finish(
              turn,
              "failed",
              new CellRuntimeModeError({ runtimeMode: turnInput.runtimePolicy.runtimeMode })
                .message,
              "permission_error",
            );
          }
          const model = turnInput.modelSelection.model;
          if (
            model !== DEFAULT_MODEL_SENTINEL &&
            tenantModel !== undefined &&
            model !== tenantModel
          ) {
            return yield* finish(
              turn,
              "failed",
              new CellModelError({ requested: model, available: tenantModel }).message,
              "validation_error",
            );
          }
          if (turnInput.message.attachments.length > 0) {
            return yield* finish(
              turn,
              "failed",
              "Cell threads cannot receive attachments yet. Send the message without them.",
              "validation_error",
            );
          }
          if (continuation) {
            const index = wakes.findIndex((wake) => wake.messageId === turnInput.message.messageId);
            const wake = index === -1 ? wakes.shift() : wakes.splice(index, 1)[0];
            if (wake === undefined) return yield* finish(turn, "completed");
            turn.phase = "running";
            turn.nativeRunId = wake.runId;
            turn.echoed = true;
            for (const event of wake.events) {
              if (active === turn) yield* applyToTurn(turn, event);
            }
            if (active === turn && wake.lost) {
              yield* finish(turn, "failed", WAKE_LOST_MESSAGE, "transport_error");
            }
            return;
          }
          turn.seqBeforePrompt = lastSeq;
        }).pipe(eventPermit.withPermits(1));
        // `active` is read through a call: the guard above narrowed the captured
        // variable to undefined for the rest of this function, which is stale.
        if (continuation || !isActive(turn)) return;
        const outcome = yield* request("POST", "/prompt", turnInput.message.text).pipe(
          Effect.result,
        );
        const settled = yield* Effect.gen(function* () {
          if (!isActive(turn)) return "settled" as const;
          if (Result.isFailure(outcome)) {
            return yield* resolveLostPrompt(turn, outcome.failure.detail);
          }
          const response = outcome.success;
          if (response.status === 202) {
            return yield* accept(turn, readPromptAccepted(response.body)?.runId);
          }
          // Nothing broadcast before the refusal was T3's run.
          yield* releaseCandidates(turn.candidates.splice(0));
          if (response.status === 409) {
            const body = response.body.trim();
            const foreignRun =
              body.startsWith("{") || body === "An agent run is already in progress";
            return yield* finish(
              turn,
              "failed",
              foreignRun
                ? ALREADY_RUNNING_MESSAGE
                : `The cell did not start the turn: ${body || "the prompt was not started"}. Check the tenant's model API key.`,
            );
          }
          yield* finish(
            turn,
            "failed",
            `The cell answered ${response.status} to the prompt${response.body.trim() ? `: ${response.body.trim().slice(0, 500)}` : ""}`,
            response.status === 401 || response.status === 403
              ? "provider_error"
              : "transport_error",
          );
          return "settled" as const;
        }).pipe(eventPermit.withPermits(1));
        if (settled === "awaiting_echo") {
          yield* Deferred.await(turn.echo).pipe(
            Effect.timeout(LEGACY_ECHO_GRACE_MS),
            Effect.result,
          );
          yield* Effect.gen(function* () {
            if (!isActive(turn)) return;
            yield* acceptEchoed(turn, Result.isFailure(outcome) ? outcome.failure.detail : "");
          }).pipe(eventPermit.withPermits(1));
        }
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: CELL_PROVIDER,
        providerSessionId: input.providerSessionId,
        providerSession: session,
        events: Stream.fromQueue(events),
        hasPendingBackgroundWork: Effect.sync(
          () =>
            wakes.length > 0 ||
            [...tasks.values()].some((record) => taskStatus(record.task.status) === "running"),
        ),
        ensureThread: (args) =>
          register(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: CELL_PROVIDER,
                  threadId: args.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (args) =>
          register({
            threadId: args.threadId ?? args.providerThread.appThreadId ?? input.threadId,
            modelSelection: args.modelSelection ?? input.modelSelection,
            runtimePolicy: args.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: args.providerThread,
          }).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: CELL_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: args.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (args) =>
          start(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: CELL_PROVIDER,
                  threadId: args.threadId,
                  providerThreadId: args.providerThread.id,
                  runId: args.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
              driver: CELL_PROVIDER,
              providerThreadId: args.providerThread.id,
            }),
          ),
        interruptTurn: (args) =>
          Effect.gen(function* () {
            const turn = active;
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            )
              return yield* protocolError("This cell turn is no longer active");
            turn.interruptRequested = true;
            const response = yield* request("POST", "/cancel");
            if (response.status >= 400)
              return yield* protocolError(`The cell answered ${response.status} to cancel`);
            yield* Deferred.await(turn.done).pipe(
              Effect.timeout(requestTimeoutMs),
              Effect.catchTags({
                TimeoutError: (cause) =>
                  eventPermit
                    .withPermits(1)(
                      finish(
                        turn,
                        "failed",
                        "The cell did not confirm the interruption in time.",
                        "transport_error",
                      ),
                    )
                    .pipe(
                      Effect.andThen(
                        protocolError("The cell did not confirm the interruption", cause),
                      ),
                    ),
              }),
            );
          }).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: CELL_PROVIDER,
                  providerThreadId: args.providerThread.id,
                  providerTurnId: args.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: CELL_PROVIDER,
              requestId: args.requestId,
              cause: protocolError("Cell threads have no runtime requests"),
            }),
          ),
        readThreadSnapshot: (args) =>
          validateThread(args.providerThread)
            ? Effect.succeed({
                providerThread: args.providerThread,
                providerTurns: lastProviderTurn ? [lastProviderTurn] : [],
                messages: [],
                runtimeRequests: [],
              })
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: CELL_PROVIDER,
                  providerThreadId: args.providerThread.id,
                  cause: protocolError("Snapshot requested for a different cell thread"),
                }),
              ),
        rollbackThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: CELL_PROVIDER,
              providerThreadId: args.providerThread.id,
              checkpointId: args.target.checkpointId,
              cause: "A cell keeps one transcript; it cannot be rolled back.",
            }),
          ),
        forkThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: CELL_PROVIDER,
              providerThreadId: args.sourceProviderThread.id,
              cause: "A cell cannot fork its conversation; T3 uses a context handoff instead.",
            }),
          ),
      };
      return runtime;
    }),
  });
});

const decodeJsonOutput = Schema.decodeOption(Schema.fromJsonString(Schema.Unknown));

/** Orchestration tools answer JSON; keeping it structured lets the row render it as such. */
function parseToolOutput(output: string): unknown {
  const trimmed = output.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return output;
  return Option.getOrElse(decodeJsonOutput(trimmed), (): unknown => output);
}
