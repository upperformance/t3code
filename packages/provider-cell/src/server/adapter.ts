/**
 * One T3 thread is one cell project. The adapter posts prompts over HTTP and
 * reads the cell's broadcast events over its WebSocket, which it keeps open
 * for the life of the session and reopens whenever celld closes it. The cell
 * broadcasts the same events to every client, so nothing is attributed to a
 * T3 turn until the cell has said which run is T3's: the 202 to its prompt,
 * or, when that answer was lost, the request id T3 sent with the prompt found
 * on the run in the cell's state or on its opening event. From then on an
 * event is the turn's exactly when it carries that run id. The run T3 owns
 * and the number up to which its output is applied are kept on the provider
 * thread's head ref, which the orchestrator persists and hands back when a
 * later session resumes the thread, so a run that outlives T3 is taken back
 * or read to its end before anything else is sent. The tenant must speak
 * cell protocol 3: every event numbered, every connection opening with
 * `state` and the `tasks` roster, and a paged event log to read a gap back.
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
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
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
  CellEventsPage,
  cellApiUrl,
  cellAuthorizationHeaders,
  cellProjectName,
  cellSocketUrl,
  classifyCellDone,
  isCellRunEvent,
  isCellWakeEvent,
  isKnownCellEventType,
  readCellRefusal,
  readCellState,
  readPromptAccepted,
  type CellRunEvent,
  type CellTask,
} from "./protocol.ts";
import { CellTransportError, type CellTransport } from "./transport.ts";

const CellProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
    // The cell runs on without T3: a turn in progress and the tasks it
    // delegated outlive a server restart, and the next session takes the run
    // back through the thread's persisted head.
    backgroundWorkSurvivesRestart: true,
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
/** How long a connection may go without its `tasks` roster after `state` before it is dropped. */
const ROSTER_TIMEOUT_MS = 10_000;
/**
 * How many connections in a row may fail to deliver a usable snapshot before
 * the active turn stops waiting on them. Reconnecting goes on regardless.
 */
const MAX_CONSECUTIVE_BROKEN_CONNECTIONS = 3;
const FULL_ACCESS = "full-access";
const DEFAULT_MODEL_SENTINEL = "default";

const UNOBSERVED_END_MESSAGE =
  "The cell's run for this turn ended without saying how; its result stayed in the cell. Send another message to continue the conversation there.";
const WAKE_LOST_MESSAGE =
  "The cell finished this delegated work while T3 was disconnected; its result stayed in the cell. Send another message to continue the conversation there.";
const ALREADY_RUNNING_MESSAGE =
  "The cell is already running a turn started from another client. Wait for it to finish, then send the message again.";
/** The run in progress is the one T3 itself started before it last stopped. */
const OWN_RUN_RUNNING_MESSAGE =
  "The cell is still running the turn that was in progress when T3 Code last stopped; its output will follow in this thread. Wait for it to finish, then send the message again.";
/** The run T3 owned when it stopped ended meanwhile, and the log no longer says how. */
const RESTORED_LOST_MESSAGE =
  "The cell's run for the turn in progress when T3 Code last stopped ended while T3 Code was down, and how it ended could not be read back; its result stayed in the cell. Send another message to continue the conversation there.";
const WAKE_RESET_MESSAGE =
  "The cell conversation was reset from another client before this delegated result reached T3, and the reset discarded it. Send another message to continue there.";
/** The run T3 owned when it stopped was discarded by a reset before the next session could take it back. */
const RESTORED_RESET_MESSAGE =
  "The cell conversation was reset from another client after T3 Code last stopped, which discarded the run for the turn then in progress; whatever that run produced before the reset is shown above, and nothing more will come from it. Send another message to continue in the reset conversation.";
const UNNAMED_RUN_MESSAGE =
  "The cell accepted the prompt without naming its run and the number of its first event, so T3 cannot tell which output is this turn's. Its result stays in the cell; send another message to continue there.";
/** The prompt's answer was lost and nothing in the cell names the request id T3 sent, while the cell runs something. */
const unconfirmedPromptMessage = (detail: string) =>
  `The cell did not answer the prompt (${detail}), and neither its state nor its log shows a run started with the request id T3 sent, so T3 cannot tell whether this message ran. If it did, its result stays in the cell; wait for the cell to be idle, then send another message.`;
/** The prompt's answer was lost, the cell is idle, and no run carries the request id T3 sent. */
const neverStartedMessage = (detail: string) =>
  `The cell did not answer the prompt (${detail}) and is idle with no run started with the request id T3 sent, so the message most likely never started. Send it again.`;
/** Three connections in a row failed before their snapshot could be used; the turn stops waiting on the socket. */
const reconnectGivenUpMessage = (reason: string) =>
  `The connection to the cell could not be re-established: ${MAX_CONSECUTIVE_BROKEN_CONNECTIONS} connections in a row were dropped because ${reason}. The turn's result stays in the cell; send another message once the tenant is healthy.`;
/** The cell's numbering went backwards, so what T3 was following is not this cell's history any more. */
const seqRegressedMessage = (from: number, to: number) =>
  `The cell's event numbering went backwards, from ${from} to ${to}, so the history T3 was following is gone. The turn's result, if any, stays in the cell; T3 has attached afresh at the new numbering, so send another message to continue there.`;
/** The cell's log no longer reaches back to the gap, so what the turn produced in it cannot be read. */
const gapLostMessage = (after: number, upTo: number) =>
  `The connection to the cell dropped while the turn ran, and the cell no longer retains the events numbered ${after + 1} to ${upTo} that T3 missed, so part of this turn's output cannot be read back. The cell's own transcript has all of it; send another message to continue there.`;

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
  /**
   * The `x-request-id` the prompt is posted with, fresh for every turn. The
   * cell keeps it on the run it starts, so a lost answer can be recovered by
   * finding it on the run in progress or on a run's opening event, and never
   * by the prompt's text, which another client may have sent too.
   */
  readonly requestId: string;
  /**
   * The cell's run this turn owns, once the 202 named it, the request id
   * recovered it, or a wake was adopted. Until then `POST prompt` is
   * unanswered and run events are candidates, not yet T3's.
   */
  run: CellRun | undefined;
  readonly candidates: Array<CellRunEvent>;
  /** The cell's state at the latest reconnect while the prompt was unanswered, judged once the 202 names the run. */
  snapshot: CellSnapshot | undefined;
  /**
   * The number the thread's head names this turn's run at: the boundary
   * before anything of the run still to be applied when the run was named,
   * then the number of each event of the run applied. Undefined until the
   * head names the run; from then on the turn's end writes the head that
   * follows it.
   */
  head: number | undefined;
}

/** A run of the cell's agent loop: its id and the number of its opening event. */
interface CellRun {
  readonly id: string;
  /** An event of another run numbered below this predates the run and says nothing about it. */
  readonly firstSeq: number;
}

/** What a connection's `state` said: the cell at one sequence number. */
interface CellSnapshot {
  readonly seq: number;
  readonly running: boolean;
  readonly runId: string | null;
}

/** How reading a gap back from the event log ended. */
type CatchUp =
  | { readonly kind: "complete" }
  | { readonly kind: "truncated"; readonly after: number; readonly upTo: number }
  | { readonly kind: "failed"; readonly detail: string };

/** The rows a task is published to: its subagent node and item, and its place among the turn's items. */
interface TaskRows extends OpenItem {
  readonly nodeId: NodeId;
  readonly itemId: TurnItemId;
}

/** A delegated task, remembered where it was first seen so later reports land on the same rows. */
interface TaskRecord {
  task: CellTask;
  /** The turn the task is published in and its rows there; undefined until a turn gives it a place. */
  owner: (TurnScope & TaskRows) | undefined;
  /**
   * The rows an earlier session started for the task, handed back with the
   * thread. The first turn of this session to publish the task takes them
   * over, so the task's end lands on the rows that showed it start rather
   * than on new ones.
   */
  restored: TaskRows | undefined;
  endedAt: DateTime.Utc | undefined;
}

/**
 * A run no turn owns yet whose output is waiting for one. A `wake` is a run
 * the cell started on its own after a child finished, offered to the
 * orchestrator as a continuation with its own message id, so the continuation
 * turn that starts for it adopts exactly its output. A `restored` run is the
 * one the last session owned when it ended, read from the thread's persisted
 * head: T3's own restart continuation adopts it, and failing that it is
 * offered as a continuation when the next turn starts.
 */
interface PendingWake {
  readonly kind: "wake" | "restored";
  readonly messageId: MessageId;
  readonly runId: string;
  /** The number of the event that opened the run, or, for a restored run, the last one the earlier session applied. */
  readonly firstSeq: number;
  readonly events: Array<CellRunEvent>;
  ended: boolean;
  /** The run ended while T3 was disconnected and could not be read back, so the buffer is incomplete. */
  lost: boolean;
  /**
   * A `reset` discarded the run after it was queued. Only the run the last
   * session owned is kept past a reset, so that the turn which comes to take
   * it back learns of the reset instead of posting its text as a prompt.
   */
  reset: boolean;
  /** The run has been offered to the orchestrator as a continuation. */
  offered: boolean;
  /** The orchestrator persisted the continuation, so a turn for it will come. */
  dispatched: boolean;
}

/** The run the last session owned, as the thread's persisted head names it. */
interface RestoredRun {
  readonly runId: string;
  /** The last number that session applied; the log is read back from here. */
  readonly cursor: number;
}

/**
 * One socket connection. Every connection opens with `state`, then the
 * `tasks` roster at that number, then events. Nothing is applied before the
 * roster; a connection that breaks that order, or sends no roster within
 * `ROSTER_TIMEOUT_MS`, is dropped and reopened.
 */
interface Connection {
  readonly ordinal: number;
  /** Completed with a reason to make the socket loop drop this connection. */
  readonly broken: Deferred.Deferred<string>;
  phase:
    | { readonly kind: "state" }
    | {
        readonly kind: "roster";
        readonly snapshot: CellSnapshot;
        readonly timer: Fiber.Fiber<void>;
      }
    | { readonly kind: "events" }
    | { readonly kind: "broken" };
}

type Inbox =
  | { readonly type: "connected"; readonly broken: Deferred.Deferred<string> }
  | { readonly type: "frame"; readonly text: string }
  | { readonly type: "roster_timeout"; readonly connection: number }
  | { readonly type: "disconnected"; readonly detail: string };

const decodeFrame = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeEvent = Schema.decodeUnknownEffect(CellEvent);
const decodeEventsPage = Schema.decodeUnknownEffect(Schema.fromJsonString(CellEventsPage));

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

/**
 * A strong head is written only while a run is T3's, naming the run and the
 * number applied up to; a weak one names a finished turn and restores nothing.
 */
function readRestoredRun(
  head: OrchestrationV2ProviderRef | null | undefined,
): RestoredRun | undefined {
  if (
    !head ||
    head.driver !== CELL_PROVIDER ||
    head.strength !== "strong" ||
    head.nativeId === null ||
    head.ordinal === undefined
  )
    return undefined;
  return { runId: head.nativeId, cursor: head.ordinal };
}

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
      let connection: Connection | undefined;
      /** Connections dropped in a row before their snapshot could be used; a usable one resets it. */
      let consecutiveBreaks = 0;
      /** The highest sequence number applied; the first connection's `state` sets it, unless a restored run says where the last session left off. */
      let lastSeq = 0;
      /** Where the last session left off, when it owned a run: the first connection reads the log back from here. */
      let resumeCursor: number | undefined;
      /**
       * Message ids of dispatched continuations whose wake a `reset` dropped,
       * so the turn that comes for each can say why it carries nothing. An
       * entry leaves when that turn starts or the orchestrator clears it.
       */
      const resetWakes = new Set<MessageId>();
      let ready: Deferred.Deferred<void, ProviderAdapter.ProviderAdapterProtocolError> | undefined;
      const tasks = new Map<string, TaskRecord>();
      const finishedBackground: Array<BackgroundWorkReport> = [];
      const wakes: Array<PendingWake> = [];
      /**
       * Rows published while no run subscribed. The session manager drops such
       * events, so they are published again when the next turn starts.
       */
      const republish = new Set<TaskRecord>();
      /**
       * Rows an earlier session started for tasks still open when the thread
       * was handed back, by task id. The first roster says which of them the
       * cell still has; each is taken by the record of its task, or settled
       * as cancelled when the cell no longer lists the task.
       */
      const restorable = new Map<string, TaskRows & { readonly title: string | undefined }>();
      const takeRestored = (taskId: string) => {
        const rows = restorable.get(taskId);
        if (rows !== undefined) restorable.delete(taskId);
        return rows;
      };
      /**
       * A misnaming is the cell's diagnostic, reported where a standalone
       * error goes: once per session, by its text, since every connection's
       * snapshot carries it again and the error the cell recorded for it
       * may be read back.
       */
      const reportedMisnamings = new Set<string>();
      const reportMisnamed = Effect.fnUntraced(function* (misnamed: string | null | undefined) {
        if (misnamed == null || reportedMisnamings.has(misnamed)) return;
        reportedMisnamings.add(misnamed);
        yield* Effect.logWarning(
          `The cell ${project ?? "?"} of thread ${input.threadId} is misnamed: ${misnamed}`,
        );
      });

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
      /**
       * The thread's persisted head while a run is T3's: the run, the request
       * id its prompt was posted with, and the number up to which the run's
       * output is applied. The orchestrator keeps it with the provider thread
       * and hands it back when a later session resumes the thread, so that
       * session can take the run back if the cell still runs it, or read its
       * end from the log. The number is never the last one received, which
       * runs ahead of the output while events wait for the prompt's answer:
       * a session resuming from the head must find everything below it
       * already applied, and read everything above it back.
       */
      const publishHead = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        runId: string,
        seq: number,
      ) {
        turn.head = seq;
        yield* updateThread({
          nativeConversationHeadRef: {
            driver: CELL_PROVIDER,
            nativeId: runId,
            strength: "strong",
            fingerprint: turn.requestId,
            ordinal: seq,
          },
        });
      });
      const request = (
        method: "GET" | "POST",
        path: string,
        body?: string,
        extraHeaders: Readonly<Record<string, string>> = {},
      ) =>
        options.transport
          .request({
            method,
            url: cellApiUrl(baseUrl, project ?? "", path),
            headers: {
              ...headers,
              "x-client": `t3-${input.providerSessionId}`,
              ...extraHeaders,
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
        const state = yield* readCellState(response.body).pipe(
          Effect.mapError((cause) =>
            cause._tag === "CellProtocolError"
              ? cause
              : protocolError(`${baseUrl} answered GET state without a cell state`, cause),
          ),
        );
        yield* reportMisnamed(state.misnamed);
        return state;
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
      /** The rows a task gets in a turn: the ones an earlier session started for it, or new ones allocated here. */
      const placeIn = Effect.fnUntraced(function* (record: TaskRecord, turn: ActiveTurn) {
        let rows = record.restored;
        if (rows === undefined) {
          const ordinal = allocate(turn);
          const ids = itemIds(turn.scope, ordinal);
          rows = { ordinal, startedAt: yield* DateTime.now, nodeId: ids.nodeId, itemId: ids.id };
        }
        record.restored = undefined;
        record.owner = { ...turn.scope, ...rows };
      });
      const publishTask = Effect.fnUntraced(function* (record: TaskRecord) {
        if (record.owner === undefined && record.restored !== undefined && active) {
          yield* placeIn(record, active);
        }
        const owner = record.owner;
        if (!owner) {
          // Rows handed back for a task no turn of this session has published yet: the next turn does.
          if (record.restored !== undefined) republish.add(record);
          return;
        }
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
          record = {
            task,
            owner: undefined,
            restored: takeRestored(task.taskId),
            endedAt: undefined,
          };
          if (active) yield* placeIn(record, active);
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
            const record: TaskRecord = {
              task,
              owner: undefined,
              restored: takeRestored(task.taskId),
              endedAt: undefined,
            };
            tasks.set(task.taskId, record);
            yield* publishTask(record);
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
        // Rows handed back for tasks the cell no longer lists: those tasks
        // ended while no session was open, and the next turn settles the rows.
        for (const [taskId, rows] of restorable) {
          restorable.delete(taskId);
          if (tasks.has(taskId)) continue;
          changed = true;
          yield* publishTask({
            task: { taskId, status: "cancelled", ...(rows.title ? { title: rows.title } : {}) },
            owner: undefined,
            restored: rows,
            endedAt: undefined,
          });
        }
        if (changed) yield* syncBackground();
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
        // The head names this turn, weakly, once its run is over: completed,
        // or ended any other way after the head had named its run. A session
        // closing with the run still in the cell leaves the strong head in
        // place: that run is what the next session takes back.
        yield* updateThread({
          status: disposition === "broken" ? "error" : "idle",
          ...((status === "completed" || turn.head !== undefined) && !closed
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
      const wakeMessageId = (kind: PendingWake["kind"], runId: string) =>
        idAllocator.derive.messageFromProviderItem({
          driver: CELL_PROVIDER,
          nativeItemId: `${options.instanceId}:${project}:${kind === "wake" ? "run" : "restored"}:${runId}`,
        });
      const enqueueWake = Effect.fnUntraced(function* (opening: CellRunEvent) {
        const wake: PendingWake = {
          kind: "wake",
          messageId: wakeMessageId("wake", opening.runId),
          runId: opening.runId,
          firstSeq: opening.seq,
          events: [],
          ended: false,
          lost: false,
          reset: false,
          offered: false,
          dispatched: false,
        };
        wakes.push(wake);
        yield* offerWake(wake);
      });
      /**
       * The run the last session owned is queued like a wake, so what the cell
       * still sends for it, live or read back, is kept for the turn that
       * adopts it. It is not offered yet: T3's own restart continuation, if one
       * comes, adopts it, and an offer made now would queue a second turn.
       */
      const restoreRun = (run: RestoredRun) =>
        Effect.sync(() => {
          wakes.push({
            kind: "restored",
            messageId: wakeMessageId("restored", run.runId),
            runId: run.runId,
            firstSeq: run.cursor,
            events: [],
            ended: false,
            lost: false,
            reset: false,
            offered: false,
            dispatched: false,
          });
          resumeCursor = run.cursor;
        });
      const offerWake = Effect.fnUntraced(function* (wake: PendingWake) {
        wake.offered = true;
        if (!thread?.appThreadId || !options.continuationRequests) return;
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
                      Effect.tap(() =>
                        Effect.sync(() => {
                          wake.dispatched = true;
                        }),
                      ),
                      Effect.asSome,
                      Effect.tapError(() => dropWake(wake)),
                    )
                  : Effect.succeedNone,
              ),
            ),
          clearIfCurrent: () =>
            eventPermit.withPermits(1)(
              dropWake(wake).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    resetWakes.delete(wake.messageId);
                  }),
                ),
              ),
            ),
        });
      });
      /** Run events no turn owns: a wake opens a queued run, that run's events fill it, the rest is noise. */
      const routeUnowned = Effect.fnUntraced(function* (event: CellRunEvent) {
        if (isCellWakeEvent(event)) return yield* enqueueWake(event);
        const wake = wakes.find((candidate) => candidate.runId === event.runId);
        if (wake === undefined || wake.ended) {
          return yield* Effect.logDebug(
            `Ignored a cell ${event.type} event of run ${event.runId}, which no turn owns`,
          );
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
      const isActive = (turn: ActiveTurn) => active === turn;
      /**
       * Which turn, if any, a run event belongs to. With no answer to the
       * prompt yet it is a candidate. Once the run is named it is the turn's
       * exactly when it carries that run id. Another run's event numbered
       * below the run's opening event is an earlier run's tail that the 202
       * overtook on the way here: nobody's, and no evidence about T3's run.
       * One numbered after it means T3's run ended without its `done`
       * reaching T3, since the cell runs one agent loop at a time.
       */
      const handleRunEvent = Effect.fnUntraced(function* (event: CellRunEvent) {
        const turn = active;
        if (turn === undefined) return yield* routeUnowned(event);
        if (turn.run === undefined) {
          turn.candidates.push(event);
          return;
        }
        if (event.runId === turn.run.id) {
          yield* applyToTurn(turn, event);
          // The head follows every event of the run applied, so a session that ends here resumes from here.
          if (isActive(turn)) yield* publishHead(turn, turn.run.id, event.seq);
          return;
        }
        if (event.seq < turn.run.firstSeq) return yield* routeUnowned(event);
        yield* finish(turn, "failed", UNOBSERVED_END_MESSAGE, "transport_error");
        return yield* routeUnowned(event);
      });
      /** Candidates a refused or failed prompt leaves behind still matter when they are a wake's run. */
      const releaseCandidates = Effect.fnUntraced(function* (turn: ActiveTurn) {
        for (const event of turn.candidates.splice(0)) yield* routeUnowned(event);
      });
      const runsRun = (snapshot: CellSnapshot, runId: string) =>
        snapshot.running && snapshot.runId === runId;
      /**
       * The 202 has named T3's run. The candidates go through the same
       * ownership rule as live events, in sequence order, so a stranger among
       * them ends the turn exactly as it would have live and a wake among them
       * is queued. A reconnect that found the prompt unanswered is judged
       * next: a run that began at or before that snapshot, which the cell was
       * not running at it and whose `done` the replay did not supply, ended
       * unobserved.
       */
      const accept = Effect.fnUntraced(function* (turn: ActiveTurn, run: CellRun) {
        turn.run = run;
        const candidates = turn.candidates.splice(0).toSorted((a, b) => a.seq - b.seq);
        // Ownership is persisted at the number before anything of the run
        // still to be applied: the candidates were received while the prompt
        // was unanswered, so the last number received is past them, and a
        // session that ended with the head there would resume with none of
        // them. The head then follows each one applied.
        const boundary =
          Math.min(
            run.firstSeq,
            ...candidates.flatMap((event) => (event.runId === run.id ? [event.seq] : [])),
          ) - 1;
        yield* publishHead(turn, run.id, boundary);
        for (const event of candidates) yield* handleRunEvent(event);
        const snapshot = turn.snapshot;
        turn.snapshot = undefined;
        if (
          snapshot !== undefined &&
          isActive(turn) &&
          run.firstSeq <= snapshot.seq &&
          !runsRun(snapshot, run.id)
        ) {
          yield* finish(turn, "failed", UNOBSERVED_END_MESSAGE, "transport_error");
        }
      });
      /**
       * A continuation turn takes a queued run: the run is the turn's from
       * here, its buffered events are replayed into it, and what the cell
       * still sends for it arrives live. A run that lost events to a gap, or
       * whose end T3 could not read back, ends as lost even when its `done`
       * was read: what the buffer holds is not the whole run. A run a reset
       * discarded ends naming the reset, after what the cell did for it
       * before, since nothing more will come.
       */
      const adopt = Effect.fnUntraced(function* (turn: ActiveTurn, wake: PendingWake) {
        turn.run = { id: wake.runId, firstSeq: wake.firstSeq };
        // The head names the run from the number the queue started it at, a
        // wake's opener or where the last session left off, both applied
        // already, and follows each buffered event applied as it does live;
        // so the turn's end writes the head that follows it, and a session
        // ending during the replay resumes from what it had shown.
        yield* publishHead(turn, wake.runId, wake.firstSeq);
        for (const event of wake.events) {
          if (!isActive(turn)) break;
          if (wake.lost && event.type === "done") continue;
          yield* applyToTurn(turn, event);
          if (isActive(turn)) yield* publishHead(turn, wake.runId, event.seq);
        }
        if (!isActive(turn)) return;
        if (wake.reset) {
          return yield* finish(turn, "failed", RESTORED_RESET_MESSAGE, "transport_error");
        }
        if (wake.lost) {
          return yield* finish(
            turn,
            "failed",
            wake.kind === "restored" ? RESTORED_LOST_MESSAGE : WAKE_LOST_MESSAGE,
            "transport_error",
          );
        }
      });
      const dispatch: (event: CellEvent) => Effect.Effect<void> = Effect.fnUntraced(function* (
        event: CellEvent,
      ) {
        switch (event.type) {
          case "state":
            return;
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
            // Only a dispatched continuation will come asking for its wake;
            // the rest are simply gone. The run the last session owned stays,
            // ended by the reset: the turn that comes to take it back, T3's
            // own restart continuation or the offered one, must learn of the
            // reset rather than post its text to the cell as a new prompt.
            for (const wake of wakes.splice(0)) {
              if (wake.kind === "restored") {
                wake.reset = true;
                wake.ended = true;
                wakes.push(wake);
              } else if (wake.dispatched) {
                resetWakes.add(wake.messageId);
              }
            }
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
            return;
          default:
            if (isCellRunEvent(event)) return yield* handleRunEvent(event);
            // An error outside any run (a cleanup that failed, a root repaired) is the cell's diagnostic, not a turn's output.
            if (event.type === "error") {
              if (event.kind === "identity") return yield* reportMisnamed(event.text);
              return yield* Effect.logWarning(
                `The cell reported an error outside any run (event ${event.seq}): ${event.text}`,
              );
            }
            return;
        }
      });
      /** Applies an event by its number: one already applied, live or replayed, is dropped. */
      const applyEvent = Effect.fnUntraced(function* (event: CellEvent) {
        if (event.seq <= lastSeq) return;
        lastSeq = event.seq;
        yield* dispatch(event);
      });
      /**
       * Drops a connection that did not keep the protocol. Reconnecting goes
       * on with backoff, but a turn cannot wait on it for ever: after three
       * connections in a row dropped before their snapshot could be used, the
       * active turn ends as a transport error that says why.
       */
      const breakConnection = Effect.fnUntraced(function* (link: Connection, reason: string) {
        if (link.phase.kind === "broken") return;
        if (link.phase.kind === "roster") yield* Fiber.interrupt(link.phase.timer);
        link.phase = { kind: "broken" };
        consecutiveBreaks += 1;
        yield* Effect.logWarning(`Dropping the cell socket: ${reason}`);
        const turn = active;
        if (turn !== undefined && consecutiveBreaks >= MAX_CONSECUTIVE_BROKEN_CONNECTIONS) {
          // The candidates stay with the turn: a wake among them is still queued when the prompt's answer releases them.
          yield* finish(turn, "failed", reconnectGivenUpMessage(reason), "transport_error");
        }
        yield* Deferred.succeed(link.broken, reason);
      });
      const decodeRaw: (
        raw: unknown,
      ) => Effect.Effect<CellEvent | undefined, ProviderAdapter.ProviderAdapterProtocolError> =
        Effect.fnUntraced(function* (raw: unknown) {
          const type = eventType(raw);
          if (!isKnownCellEventType(type)) return undefined;
          if (options.nativeEventLogger)
            yield* options.nativeEventLogger.write(
              { provider: "cell", event: raw },
              input.threadId,
            );
          return yield* decodeEvent(raw).pipe(
            Effect.mapError((cause) =>
              protocolError(`The cell sent an unreadable ${type} event`, cause),
            ),
          );
        });
      const handleRaw: (
        raw: unknown,
      ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterProtocolError> = Effect.fnUntraced(
        function* (raw: unknown) {
          const event = yield* decodeRaw(raw);
          if (event !== undefined) yield* handleEvent(event);
        },
      );
      /** The log is best effort: an entry that cannot be read is skipped, not fatal. */
      const decodeReplayed = (raw: unknown) =>
        decodeRaw(raw).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Skipped an unreadable replayed cell event", cause).pipe(
              Effect.as(undefined),
            ),
          ),
        );
      /**
       * Reads the persisted events after `after` up to `upTo` a page at a
       * time, applying each page in sequence order before fetching the next,
       * so no more than one page is ever held. Events a page carries beyond
       * `upTo` are left to the socket, which delivers everything after its
       * snapshot live. A gap the cell no longer retains cannot be read back.
       */
      const catchUp: (after: number, upTo: number) => Effect.Effect<CatchUp> = Effect.fnUntraced(
        function* (after: number, upTo: number) {
          let cursor = after;
          while (cursor < upTo) {
            const response = yield* request(
              "GET",
              `/events?after=${cursor}&limit=${EVENTS_PAGE_LIMIT}`,
            ).pipe(Effect.result);
            if (Result.isFailure(response)) {
              return { kind: "failed", detail: response.failure.detail };
            }
            if (response.success.status !== 200) {
              return {
                kind: "failed",
                detail: `the cell answered ${response.success.status} to GET events`,
              };
            }
            const page = yield* decodeEventsPage(response.success.body).pipe(Effect.result);
            if (Result.isFailure(page)) {
              return {
                kind: "failed",
                detail: "the cell answered GET events without an event page",
              };
            }
            const { nextAfter, complete, truncated } = page.success;
            if (truncated === true) return { kind: "truncated", after, upTo };
            for (const raw of page.success.events) {
              const event = yield* decodeReplayed(raw);
              if (event !== undefined && event.seq <= upTo) yield* applyEvent(event);
            }
            // A page that moves the cursor nowhere, or a log that claims to end
            // before the number the cell's own state reported, is not reading
            // the gap back: the cursor stays where it was and the recovery fails.
            if (nextAfter <= cursor) {
              return {
                kind: "failed",
                detail: `the cell answered GET events?after=${cursor} with no progress (nextAfter ${nextAfter})`,
              };
            }
            if (complete && nextAfter < upTo) {
              return {
                kind: "failed",
                detail: `the cell's log claims to end at ${nextAfter}, before the ${upTo} its state reported`,
              };
            }
            cursor = nextAfter;
          }
          return { kind: "complete" };
        },
      );
      /**
       * The connection's snapshot is complete: `state` said where the cell is
       * and `tasks` is its roster at that number. On a reconnect the gap up to
       * that number is read back first, so each replayed task row is observed
       * with the turn that was running when the cell sent it, and the roster
       * is applied at its own number after them. The active turn and the
       * queued wakes are then judged against what the cell said it was
       * running: a replay that was complete has already ended a turn whose
       * `done` fell in the gap, so a run the cell is not running and that is
       * still open here ended unobserved.
       */
      const handleRoster = Effect.fnUntraced(function* (
        link: Connection,
        snapshot: CellSnapshot,
        roster: Extract<CellEvent, { readonly type: "tasks" }>,
      ) {
        const first = link.ordinal === 1;
        if (first) {
          consecutiveBreaks = 0;
          if (resumeCursor === undefined) {
            // Nothing before T3 attached is T3's: the first snapshot is where the cursor starts.
            lastSeq = snapshot.seq;
            yield* reconcileTasks(roster.tasks);
            if (ready) yield* Deferred.succeed(ready, undefined);
            return;
          }
          // The last session left off at the persisted number with a run of
          // its own in the cell. What the cell did since is read back as after
          // a reconnect, so that run is ended or taken back before anything is
          // sent to the cell.
          lastSeq = resumeCursor;
          resumeCursor = undefined;
        }
        if (snapshot.seq < lastSeq) {
          // The cell's counter never regresses, so a snapshot below the last
          // number applied is not the history T3 was following. Nothing from
          // it carries over: the turn and the queued wakes end as lost, and
          // the adapter attaches afresh at the new snapshot, as on a first
          // connection.
          yield* Effect.logWarning(
            `The cell's event numbering went backwards from ${lastSeq} to ${snapshot.seq}; attaching afresh`,
          );
          const turn = active;
          if (turn !== undefined) {
            turn.candidates.splice(0);
            yield* finish(
              turn,
              "failed",
              seqRegressedMessage(lastSeq, snapshot.seq),
              "transport_error",
            );
          }
          for (const wake of wakes) wake.lost = true;
          lastSeq = snapshot.seq;
          consecutiveBreaks = 0;
          yield* reconcileTasks(roster.tasks);
          if (first && ready) yield* Deferred.succeed(ready, undefined);
          return;
        }
        const caughtUp: CatchUp =
          snapshot.seq > lastSeq ? yield* catchUp(lastSeq, snapshot.seq) : { kind: "complete" };
        if (caughtUp.kind === "failed") {
          if (!first) {
            // The cursor stays where the replay stopped; the next connection's snapshot presents the rest of the gap again.
            return yield* breakConnection(
              link,
              `could not read back the events missed while disconnected: ${caughtUp.detail}`,
            );
          }
          // The thread must still attach. The restored run is given up as
          // lost, since what the socket delivers from here is not the whole
          // run, and the adapter attaches at the snapshot as on a first
          // connection.
          yield* Effect.logWarning(
            `Could not read back the events since the last session ended: ${caughtUp.detail}; attaching at the snapshot`,
          );
          for (const wake of wakes) wake.lost = true;
          lastSeq = snapshot.seq;
          yield* reconcileTasks(roster.tasks);
          if (ready) yield* Deferred.succeed(ready, undefined);
          return;
        }
        consecutiveBreaks = 0;
        if (lastSeq < snapshot.seq) lastSeq = snapshot.seq;
        yield* reconcileTasks(roster.tasks);
        const turn = active;
        if (turn !== undefined) {
          if (caughtUp.kind === "truncated") {
            yield* finish(
              turn,
              "failed",
              gapLostMessage(caughtUp.after, caughtUp.upTo),
              "transport_error",
            );
          } else if (turn.run === undefined) {
            // Which run is T3's is still unknown; the snapshot is judged once the 202 says.
            turn.snapshot = snapshot;
          } else if (!runsRun(snapshot, turn.run.id)) {
            yield* finish(turn, "failed", UNOBSERVED_END_MESSAGE, "transport_error");
          }
        }
        for (const wake of wakes) {
          if (wake.ended) continue;
          // A gap that cannot be read back took part of every wake still open
          // across it, whether or not the cell still runs that wake: what the
          // socket delivers after the snapshot is not the whole run.
          if (caughtUp.kind === "truncated" || !runsRun(snapshot, wake.runId)) wake.lost = true;
        }
        if (first && ready) yield* Deferred.succeed(ready, undefined);
      });
      /**
       * Frames follow the connection's phase: `state` first, then the `tasks`
       * roster numbered at the same point, then events applied by their
       * numbers. A frame out of that order, or a roster numbered elsewhere
       * than its state, means the tenant is not keeping the protocol, and the
       * connection is dropped rather than applied around.
       */
      const handleEvent: (event: CellEvent) => Effect.Effect<void> = Effect.fnUntraced(function* (
        event: CellEvent,
      ) {
        const link = connection;
        if (link === undefined) return;
        switch (link.phase.kind) {
          case "broken":
            return;
          case "state": {
            if (event.type !== "state") {
              return yield* breakConnection(link, `the cell sent ${event.type} before state`);
            }
            yield* reportMisnamed(event.misnamed);
            const timer = yield* Effect.sleep(ROSTER_TIMEOUT_MS).pipe(
              Effect.andThen(
                Queue.offer(inbox, { type: "roster_timeout", connection: link.ordinal }),
              ),
              Effect.asVoid,
              Effect.forkIn(scope),
            );
            link.phase = {
              kind: "roster",
              snapshot: { seq: event.seq, running: event.running, runId: event.runId },
              timer,
            };
            return;
          }
          case "roster": {
            if (event.type !== "tasks") {
              return yield* breakConnection(
                link,
                `the cell sent ${event.type} before the tasks roster`,
              );
            }
            const { snapshot, timer } = link.phase;
            if (event.seq !== snapshot.seq) {
              return yield* breakConnection(
                link,
                `the cell sent a tasks roster numbered ${event.seq} after a state numbered ${snapshot.seq}`,
              );
            }
            yield* Fiber.interrupt(timer);
            link.phase = { kind: "events" };
            return yield* handleRoster(link, snapshot, event);
          }
          case "events":
            return yield* applyEvent(event);
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
                connection = {
                  ordinal: connections,
                  broken: entry.broken,
                  phase: { kind: "state" },
                };
                return;
              case "roster_timeout": {
                const link = connection;
                if (link?.ordinal === entry.connection && link.phase.kind === "roster") {
                  yield* breakConnection(
                    link,
                    `the cell sent no tasks roster within ${ROSTER_TIMEOUT_MS / 1000} s of state`,
                  );
                }
                return;
              }
              case "disconnected":
                if (connection?.phase.kind === "roster") {
                  yield* Fiber.interrupt(connection.phase.timer);
                }
                connection = undefined;
                // Failing `ready` here, after the connection's frames, means a
                // snapshot that did complete before the close still opens the thread.
                if (ready) {
                  yield* Deferred.fail(
                    ready,
                    protocolError(`Could not connect to the cell: ${entry.detail}`),
                  );
                }
                return yield* Effect.logDebug(`Cell socket closed: ${entry.detail}`);
              case "frame": {
                // A frame that is not an event of the protocol (not JSON, a
                // known type with a bad number or a missing field) is a
                // violation like a frame out of order: the connection is
                // dropped and the gap read back from the log.
                const link = connection;
                return yield* handleFrame(entry.text).pipe(
                  Effect.catch((cause) =>
                    link === undefined
                      ? Effect.logWarning("Skipped an unreadable cell frame", cause)
                      : breakConnection(link, cause.detail),
                  ),
                );
              }
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
          const broken = yield* Deferred.make<string>();
          const outcome = yield* Effect.scoped(
            Effect.gen(function* () {
              const link = yield* options.transport.connect({ url, headers });
              connectedAt = yield* Clock.currentTimeMillis;
              yield* Queue.offer(inbox, { type: "connected", broken });
              const pump = Effect.gen(function* () {
                while (true) {
                  const frames = yield* link.pull;
                  for (const text of frames) yield* Queue.offer(inbox, { type: "frame", text });
                }
              });
              // The consumer drops a connection that breaks the protocol's order; the pump ends with it.
              return yield* Effect.raceFirst(
                pump,
                Deferred.await(broken).pipe(
                  Effect.flatMap((reason) =>
                    Effect.fail(
                      new CellTransportError({
                        operation: `read ${url}`,
                        detail: reason,
                        closed: true,
                      }),
                    ),
                  ),
                ),
              );
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
          resetWakes.clear();
          if (active) {
            active.candidates.splice(0);
            yield* finish(active, "cancelled", "Cell session closed", "provider_error", "broken");
          }
          // A delegated task runs on in the cell after the session closes:
          // its rows and the thread's roster are left as they are, and the
          // next session is handed the rows with the thread to publish the
          // task's end to.
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
        // A strong head on the thread handed back is a run the last session
        // owned when it ended; the first connection reads the log from there.
        const restored = readRestoredRun(existing?.nativeConversationHeadRef);
        if (restored !== undefined) yield* restoreRun(restored);
        const createdAt = yield* DateTime.now;
        // The rows an earlier session started for tasks still open: the first
        // roster, which the socket sends before anything else, decides which
        // of them the cell still has.
        restorable.clear();
        for (const item of args.openNativeSubagentItems ?? []) {
          const taskId = item.nativeItemRef?.nativeId;
          if (taskId == null || item.driver !== CELL_PROVIDER) continue;
          restorable.set(taskId, {
            nodeId: item.subagentId,
            itemId: item.id,
            ordinal: item.ordinal,
            startedAt: item.startedAt ?? createdAt,
            title: item.title ?? item.prompt,
          });
        }
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
              resumeCursor = undefined;
              wakes.splice(0);
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
      const validateThread = (candidate: OrchestrationV2ProviderThread) =>
        thread !== undefined &&
        candidate.driver === CELL_PROVIDER &&
        candidate.providerInstanceId === options.instanceId &&
        candidate.appThreadId === thread.appThreadId &&
        candidate.nativeThreadRef?.nativeId === project;
      /** The opening `user` event of T3's prompt: the one carrying the request id T3 posted it with. */
      const findOpener = (turn: ActiveTurn) =>
        turn.candidates.find(
          (event): event is Extract<CellRunEvent, { readonly type: "user" }> =>
            event.type === "user" && event.requestId === turn.requestId,
        );
      /**
       * The prompt's answer never came: a timeout, a dropped socket, or a
       * status from something between T3 and the cell. The cell may still
       * have started the run, and only the request id T3 sent with the prompt
       * can say so: the run's opening event carries it, among the candidates
       * or in the event log read from where T3 left off up to what `GET
       * state` reports, and the state names the id of the run in progress.
       * Nothing else is evidence; the prompt's text is not, since another
       * client may have sent the same words. Anything less fails the turn
       * with a message that says what T3 could not confirm.
       */
      const resolveLostPrompt = Effect.fnUntraced(function* (turn: ActiveTurn, detail: string) {
        const echoed = findOpener(turn);
        if (echoed !== undefined) {
          return yield* accept(turn, { id: echoed.runId, firstSeq: echoed.seq });
        }
        const state = yield* readState().pipe(Effect.result);
        if (Result.isFailure(state)) {
          yield* releaseCandidates(turn);
          return yield* finish(
            turn,
            "failed",
            unconfirmedPromptMessage(`${detail}; ${state.failure.message}`),
            "transport_error",
          );
        }
        if (state.success.seq > lastSeq) {
          const caughtUp = yield* catchUp(lastSeq, state.success.seq);
          if (caughtUp.kind !== "complete") {
            yield* releaseCandidates(turn);
            return yield* finish(
              turn,
              "failed",
              caughtUp.kind === "truncated"
                ? gapLostMessage(caughtUp.after, caughtUp.upTo)
                : `${detail}; ${caughtUp.detail}`,
              "transport_error",
            );
          }
        }
        if (!isActive(turn)) return;
        const replayed = findOpener(turn);
        if (replayed !== undefined) {
          return yield* accept(turn, { id: replayed.runId, firstSeq: replayed.seq });
        }
        const current = state.success;
        if (current.running && current.runId !== null && current.requestId === turn.requestId) {
          // The cell names T3's request id on the run in progress though its
          // opener is not among what T3 holds: the run is T3's from the
          // number the cell confirmed it at, and nothing numbered before that
          // is evidence about it.
          return yield* accept(turn, { id: current.runId, firstSeq: current.seq });
        }
        yield* releaseCandidates(turn);
        return yield* finish(
          turn,
          "failed",
          current.running ? unconfirmedPromptMessage(detail) : neverStartedMessage(detail),
          "transport_error",
        );
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
        const restart = turnInput.restartContinuationOfRunId !== undefined;
        const nativeId = `${project}:turn:${turnInput.providerTurnOrdinal}`;
        const startedAt = yield* DateTime.now;
        const requestId = yield* randomUuidV4;
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
          requestId,
          run: undefined,
          candidates: [],
          snapshot: undefined,
          head: undefined,
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
          if (continuation || restart) {
            const messageId = turnInput.message.messageId;
            // A provider continuation adopts only the wake this message was
            // offered for, never the next wake's output. T3's own restart
            // continuation adopts the run the last session owned, which the
            // thread's persisted head named; with nothing restored, its text
            // goes to the cell as a prompt like any other.
            const index = continuation
              ? wakes.findIndex((wake) => wake.messageId === messageId)
              : wakes.findIndex((wake) => wake.kind === "restored");
            if (index !== -1) return yield* adopt(turn, wakes.splice(index, 1)[0]!);
            if (continuation) {
              return resetWakes.delete(messageId)
                ? yield* finish(turn, "failed", WAKE_RESET_MESSAGE)
                : yield* finish(turn, "completed");
            }
          }
          // No restart continuation came for the run the last session owned,
          // so it is offered as a continuation now and its output follows this
          // turn; a prompt sent while the cell still runs it is refused below,
          // naming it.
          const restored = wakes.find((wake) => wake.kind === "restored" && !wake.offered);
          if (restored !== undefined) yield* offerWake(restored);
        }).pipe(eventPermit.withPermits(1));
        // `active` is read through a call: the guard above narrowed the captured
        // variable to undefined for the rest of this function, which is stale.
        // A turn that adopted a run has nothing to post.
        if (continuation || !isActive(turn) || turn.run !== undefined) return;
        const outcome = yield* request("POST", "/prompt", turnInput.message.text, {
          "x-request-id": turn.requestId,
        }).pipe(Effect.result);
        yield* Effect.gen(function* () {
          // A turn that ended while the prompt was in flight leaves its candidates to the unowned path.
          if (!isActive(turn)) return yield* releaseCandidates(turn);
          if (Result.isFailure(outcome)) {
            return yield* resolveLostPrompt(turn, outcome.failure.detail);
          }
          const response = outcome.success;
          if (response.status === 202) {
            const accepted = readPromptAccepted(response.body);
            if (accepted === undefined) {
              yield* releaseCandidates(turn);
              return yield* finish(turn, "failed", UNNAMED_RUN_MESSAGE, "transport_error");
            }
            return yield* accept(turn, { id: accepted.runId, firstSeq: accepted.seq });
          }
          const refusal = readCellRefusal(response.body);
          // A server error whose body is not the cell's own refusal, whatever
          // else it carries (a proxy's HTML, a gateway's JSON error, nothing
          // readable), came from something between T3 and the cell, or from a
          // cell that may already have started the run: an answer as lost as
          // no answer. Only the refusal shape itself fails the turn outright.
          if (response.status >= 500 && refusal === undefined) {
            return yield* resolveLostPrompt(
              turn,
              `the cell answered ${response.status} to the prompt`,
            );
          }
          // Nothing broadcast before the refusal was T3's run.
          yield* releaseCandidates(turn);
          const reason = refusal?.error ?? response.body.trim().slice(0, 500);
          if (response.status === 409) {
            return yield* finish(
              turn,
              "failed",
              refusal?.runId
                ? wakes.some((wake) => wake.kind === "restored" && wake.runId === refusal.runId)
                  ? OWN_RUN_RUNNING_MESSAGE
                  : ALREADY_RUNNING_MESSAGE
                : `The cell did not start the turn: ${reason || "the prompt was not started; check the tenant's model API key"}.`,
            );
          }
          yield* finish(
            turn,
            "failed",
            `The cell answered ${response.status} to the prompt${reason ? `: ${reason}` : ""}`,
            refusal !== undefined || response.status === 401 || response.status === 403
              ? "provider_error"
              : "transport_error",
          );
        }).pipe(eventPermit.withPermits(1));
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
            ...(args.openNativeSubagentItems === undefined
              ? {}
              : { openNativeSubagentItems: args.openNativeSubagentItems }),
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
            if (!validateThread(args.providerThread))
              return yield* protocolError("This cell turn is no longer active");
            // Decided under the event permit, behind whatever reconciliation
            // is in progress: a replay that has just ended the turn leaves
            // nothing to cancel, and the run the cancel names keeps the cell
            // from ending a later run instead.
            const target = yield* eventPermit.withPermits(1)(
              Effect.sync(() => {
                const turn = active;
                if (turn === undefined || turn.providerTurn.id !== args.providerTurnId) {
                  return lastProviderTurn?.id === args.providerTurnId
                    ? ("settled" as const)
                    : ("unknown" as const);
                }
                turn.interruptRequested = true;
                return turn;
              }),
            );
            // A settled turn has nothing left to stop.
            if (target === "settled") return;
            if (target === "unknown")
              return yield* protocolError("This cell turn is no longer active");
            const turn = target;
            if (turn.run === undefined)
              return yield* protocolError("The cell has not named this turn's run yet");
            const response = yield* request("POST", "/cancel", undefined, {
              "x-run-id": turn.run.id,
            });
            if (response.status === 409) {
              // The cell runs another run now, so this turn's has ended; its
              // `done` reaches the turn live or by replay and settles it.
              yield* Effect.logDebug(
                `The cell refused to cancel run ${turn.run.id}: it runs ${readCellRefusal(response.body)?.runId ?? "another run"}`,
              );
            } else if (response.status >= 400) {
              return yield* protocolError(`The cell answered ${response.status} to cancel`);
            }
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
