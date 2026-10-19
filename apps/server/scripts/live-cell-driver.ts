// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off globalDateInEffect:off globalConsole:off preferSchemaOverJson:off - a live driver: it runs the real orchestrator and adapter against a cell on celld and logs on the wall clock.
/**
 * Drives the real orchestrator and the real cell adapter against a live cell
 * (celld) for the scenarios the fixture recorder cannot express, and logs
 * every HTTP exchange and every socket frame with a wall-clock stamp.
 *
 *   node scripts/live-cell-driver.ts --scenario <shell|interrupt|delegate|limit|reconnect|restart|attach-retry|delete-delegate> --url http://127.0.0.1:9884 [--tag t] [--token t] [--log dir]
 *
 * The frames and projections go to --log, by default a new directory under the
 * OS temp directory named for the scenario and the run's tag; it is printed
 * first.
 *
 * reconnect: the socket is terminated 3 s after the prompt's 202 and the next
 * connect is held for RECONNECT_HOLD_MS, so the run's output lands in the gap
 * and the adapter has to read it back.
 * restart: two orchestrator runtimes over one SQLite file; the first stops the
 * way the server stops while the cell still runs the turn, the second recovers.
 * attach-retry: the first socket delivers `state` and nothing after it, then
 * is terminated (1006), so the roster never arrives on it; the second attaches
 * and the prompt completes.
 * delete-delegate: the run delegates and completes while the child still runs
 * (hold the child with the scripted API's CHILD_DELAY_MS), the thread is
 * deleted, and the cell must have received the task's cancel and list the
 * task as cancelled.
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { NodeWS as WS } from "@effect/platform-node-shared/NodeSocket";
import {
  CommandId,
  type OrchestrationV2ThreadProjection,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import { makeHttpCellTransport, type CellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import { CELL_REPLAY_PROTOCOL } from "@t3tools/provider-cell/testing";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Socket from "effect/socket/Socket";
import { TestClock } from "effect/testing";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  CELL_PROVIDER_KIND,
  CellOrchestratorReplayHarness,
  layer as cellLayer,
} from "../src/orchestration-v2/Adapters/CellAdapterV2.testkit.ts";
import * as EffectWorker from "../src/orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../src/orchestration-v2/Orchestrator.ts";
import * as ProviderRuntimeRecoveryService from "../src/orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "../src/orchestration-v2/ProviderSessionManager.ts";
import * as SqlitePersistence from "../src/persistence/Sqlite.ts";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import {
  CELL_MODEL_SELECTION,
  materializeFixtureInput,
  type OrchestratorFixtureInputStep,
} from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2Scenario } from "../src/orchestration-v2/testkit/OrchestratorScenario.ts";
import * as ProviderReplayHarness from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";

const CLOCK_TICK = Duration.millis(20);
const POLL_LIMIT = 9_000; // × 10 ms wall time
const RECONNECT_KILL_AFTER_MS = 3_000;
const RECONNECT_HOLD_MS = Number(process.env.RECONNECT_HOLD_MS ?? 25_000);

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenarioName = readArgValue("--scenario") ?? "shell";
// Every run gets its own thread, hence its own cell project: the deterministic
// runtime mints the same request id each run, which a reused cell would answer
// with the run that ended last instead of starting one.
const runTag =
  readArgValue("--tag") ??
  new Date()
    .toISOString()
    .replace(/[-:.TZ]/g, "")
    .slice(0, 14);
const baseUrl = readArgValue("--url") ?? "http://127.0.0.1:9884";
// Each run logs to a directory of its own unless --log names one.
const logDir =
  readArgValue("--log") ??
  NodePath.join(NodeOS.tmpdir(), `live-cell-driver-${scenarioName}-${runTag}`);
NodeFS.mkdirSync(logDir, { recursive: true });
const framesPath = NodePath.join(logDir, `${scenarioName}.frames.log`);
NodeFS.writeFileSync(framesPath, "");
console.log(`logging to ${logDir}`);
const stamp = () => new Date().toISOString();
const log = (line: string) => {
  const text = `${stamp()} ${line}`;
  NodeFS.appendFileSync(framesPath, `${text}\n`);
  console.log(text);
};

const decodeCellSettings = Schema.decodeUnknownEffect(CellSettings);

const PROMPTS = {
  shell:
    "Do not answer immediately. First run the local shell command `sleep 1`, then respond with exactly: shell fixture ok.",
  delegate:
    "Delegate a review to a child cell. Reply with exactly: STARTED. When the child reports, reply with exactly: WAKE_DONE",
  delegateFollowUp: "Respond with exactly: USER_REPLY",
  limit: "Limit: Respond with exactly: never answered",
  afterLimit: "Respond with exactly: after the limit ok",
  reconnect: "Hold: Respond with exactly: reconnect ok",
  restart: "Hold: Respond with exactly: RESUMED",
  attachRetry: "Respond with exactly: attach retry ok",
} as const;

/** The adapter runs on the test clock; the cell on the wall clock. */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

let latestSocket: WS.WebSocket | undefined;
let socketOrdinal = 0;
const makeWebSocket: Socket.WebSocketConstructor["Service"] = (url, options) => {
  socketOrdinal += 1;
  const socket =
    options === undefined || typeof options === "string" || Array.isArray(options)
      ? new WS.WebSocket(url, options)
      : new WS.WebSocket(url, options.protocols, { headers: options.headers });
  const ordinal = socketOrdinal;
  latestSocket = socket;
  socket.on("open", () => log(`ws#${ordinal} open ${url}`));
  socket.on("close", (code, reason) =>
    log(`ws#${ordinal} closed code=${code} reason=${String(reason)}`),
  );
  socket.on("error", (error) => log(`ws#${ordinal} error ${error.message}`));
  return socket as unknown as globalThis.WebSocket;
};
const layerWebSocketConstructor = Layer.succeed(Socket.WebSocketConstructor)(makeWebSocket);

let holdConnectUntil = 0;
let onPromptAccepted: ((body: string) => void) | undefined;
let firstSocketCut = false;
interface TaskCancelRequest {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}
// delete-delegate: a cancel leaving is recorded apart from its answer, which
// the adapter stops waiting for after its own bound, though the cell may still
// carry the cancel out.
let onTaskCancelSent: ((request: TaskCancelRequest) => void) | undefined;
let onTaskCancelAnswered:
  | ((answer: TaskCancelRequest & { readonly status: number; readonly body: string }) => void)
  | undefined;
const redact = (headers: Readonly<Record<string, string>>) =>
  Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [
      key,
      key.toLowerCase() === "authorization" ? "<redacted>" : value,
    ]),
  );

const loggingTransport = (live: CellTransport): CellTransport => ({
  request: (input) =>
    Effect.gen(function* () {
      log(
        `>> ${input.method} ${input.url} headers=${JSON.stringify(redact(input.headers))}${input.body === undefined ? "" : ` body=${JSON.stringify(input.body)}`}`,
      );
      const isTaskCancel = input.method === "POST" && /\/tasks\/[^/]+\/cancel$/.test(input.url);
      if (isTaskCancel) onTaskCancelSent?.({ url: input.url, headers: input.headers });
      const response = yield* live.request(input).pipe(
        Effect.tapError((error) =>
          Effect.sync(() => log(`<< ${input.method} ${input.url} FAILED ${error.detail}`)),
        ),
        Effect.onInterrupt(() =>
          Effect.sync(() => log(`<< ${input.method} ${input.url} abandoned before its answer`)),
        ),
      );
      log(`<< ${response.status} ${input.method} ${input.url} ${response.body.slice(0, 600)}`);
      if (input.method === "POST" && input.url.endsWith("/prompt") && response.status === 202) {
        onPromptAccepted?.(response.body);
      }
      if (isTaskCancel) {
        onTaskCancelAnswered?.({
          url: input.url,
          headers: input.headers,
          status: response.status,
          body: response.body,
        });
      }
      return response;
    }),
  connect: (input) =>
    Effect.gen(function* () {
      const wait = holdConnectUntil - Date.now();
      if (wait > 0) {
        log(`ws connect held for ${wait} ms (${input.url})`);
        yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, wait)));
      }
      log(`ws connect ${input.url} headers=${JSON.stringify(redact(input.headers))}`);
      const connection = yield* live.connect(input);
      // attach-retry: the first connection delivers up to `state` and nothing
      // after it, then is terminated, so its roster never arrives.
      const cut = scenarioName === "attach-retry" && !firstSocketCut;
      if (cut) firstSocketCut = true;
      let cutDone = false;
      return {
        pull: connection.pull.pipe(
          Effect.map((frames) => {
            if (!cut) return frames;
            if (cutDone) {
              for (const frame of frames)
                log(`attach-retry: dropped on the cut ws: ${frame.slice(0, 200)}`);
              return [];
            }
            const index = frames.findIndex((frame) => frame.startsWith('{"type":"state"'));
            if (index === -1) return frames;
            cutDone = true;
            const kept = frames.slice(0, index + 1);
            const socket = latestSocket;
            log(
              `attach-retry: delivering ${kept.length} frame(s) up to state, dropping ${frames.length - kept.length}, terminating ws#${socketOrdinal}`,
            );
            setTimeout(() => socket?.terminate(), 0);
            return kept;
          }),
          Effect.tap((frames) =>
            Effect.sync(() => {
              for (const frame of frames) log(`ws <- ${frame.slice(0, 700)}`);
            }),
          ),
          Effect.tapError((error) =>
            Effect.sync(() =>
              log(`ws pull ended: ${error.detail} closed=${error.closed ?? false}`),
            ),
          ),
        ),
      };
    }),
});

const describeProjection = (projection: OrchestrationV2ThreadProjection) =>
  [
    ...projection.runs.map(
      (run) =>
        `run ${run.id.slice(-12)} ${run.status} continuationOf=${run.restartContinuationOfRunId?.slice(-12) ?? "-"} note=${JSON.stringify(run.restartCancelledBackgroundWork ?? [])}`,
    ),
    ...projection.messages.map(
      (message) =>
        `message ${message.id.slice(-12)} by=${message.createdBy}:${message.creationSource} notification=${JSON.stringify(message.notification?.summary ?? null)}: ${message.text.slice(0, 140)}`,
    ),
    ...projection.turnItems.map((item) => {
      const base = `item ${item.type} ${item.status} run=${item.runId?.slice(-12)}`;
      switch (item.type) {
        case "assistant_message":
          return `${base} text=${JSON.stringify(item.text.slice(0, 120))}`;
        case "error":
          return `${base} failure=${JSON.stringify(item.failure)}`;
        case "command_execution":
          return `${base} ${JSON.stringify({ command: (item as { command?: string }).command, exitCode: (item as { exitCode?: number | null }).exitCode })}`;
        case "subagent":
          return `${base} ${JSON.stringify({ title: (item as { title?: string }).title, result: (item as { result?: string | null }).result })}`;
        case "system_notice":
          return `${base} ${JSON.stringify((item as { text?: string }).text ?? (item as { message?: string }).message ?? "")}`;
        default:
          return base;
      }
    }),
    ...projection.providerTurns.map(
      (turn) =>
        `providerTurn ${turn.ordinal} ${turn.status} attempt=${turn.runAttemptId?.slice(-12)}`,
    ),
    ...projection.providerThreads.map(
      (thread) =>
        `providerThread ${thread.status} native=${thread.nativeThreadRef?.nativeId ?? "-"} head=${JSON.stringify(thread.nativeConversationHeadRef)} roster=${JSON.stringify(thread.pendingBackgroundTasks ?? [])}`,
    ),
    ...projection.providerSessions.map(
      (session) => `session ${session.id.slice(-12)} ${session.status} ${session.lastError ?? ""}`,
    ),
  ].join("\n");

const report = (name: string, projection: OrchestrationV2ThreadProjection) => {
  const path = NodePath.join(logDir, `${name}.projection.json`);
  NodeFS.writeFileSync(path, JSON.stringify(projection, null, 1));
  log(`---- projection ${name} (full copy at ${path}) ----\n${describeProjection(projection)}`);
};

const until = <E>(
  read: Effect.Effect<OrchestrationV2ThreadProjection, E>,
  ready: (value: OrchestrationV2ThreadProjection) => boolean,
  what: string,
): Effect.Effect<OrchestrationV2ThreadProjection, E> =>
  Effect.gen(function* () {
    let last: OrchestrationV2ThreadProjection | undefined;
    for (let attempt = 0; attempt < POLL_LIMIT; attempt++) {
      last = yield* read;
      if (ready(last)) return last;
      yield* TestClock.withLive(Effect.sleep("10 millis"));
    }
    return yield* Effect.die(
      new Error(
        `Gave up waiting for ${what}:\n${last === undefined ? "(nothing read)" : describeProjection(last)}`,
      ),
    );
  });

const strongHead = (projection: OrchestrationV2ThreadProjection) => {
  const head = projection.providerThreads[0]?.nativeConversationHeadRef;
  return head?.strength === "strong" ? head : undefined;
};

const stepsFor = (name: string): ReadonlyArray<OrchestratorFixtureInputStep> => {
  switch (name) {
    case "shell":
      return [{ type: "message", text: PROMPTS.shell }];
    case "delegate":
      return [
        { type: "message", text: PROMPTS.delegate },
        { type: "await_run_status", targetRunIndex: 2, status: "completed" },
        { type: "message", text: PROMPTS.delegateFollowUp },
      ];
    case "limit":
      return [
        { type: "message", text: PROMPTS.limit },
        { type: "message", text: PROMPTS.afterLimit },
      ];
    case "reconnect":
      return [{ type: "message", text: PROMPTS.reconnect }];
    case "interrupt":
      return [
        {
          type: "message",
          text: "Do not answer immediately. First run the local shell command `sleep 30`, then respond with exactly: interrupt fixture should not finish naturally.",
        },
        { type: "interrupt", targetRunIndex: 1, waitForTurnItemType: "command_execution" },
      ];
    case "restart":
      return [{ type: "message", text: PROMPTS.restart }];
    case "attach-retry":
      return [{ type: "message", text: PROMPTS.attachRetry }];
    case "delete-delegate":
      return [{ type: "message", text: PROMPTS.delegate }];
    default:
      throw new Error(`Unknown scenario ${name}`);
  }
};

const main = Effect.gen(function* () {
  const settings = yield* decodeCellSettings({
    enabled: true,
    baseUrl,
    token: readArgValue("--token") ?? "",
  });
  // The adapter's timers (reconnect backoff, roster timeout) run on the test clock; the cell on the wall clock.
  yield* followWallClock.pipe(Effect.forkScoped);
  const transport = loggingTransport(yield* makeHttpCellTransport);
  const harness = {
    driver: CellOrchestratorReplayHarness.driver,
    decodeTranscript: (transcript: ProviderReplayTranscript) => Effect.succeed(transcript),
    makeProviderAdapterRegistryLayer: () => cellLayer({ transport, settings }),
  };
  const transcript: ProviderReplayTranscript = {
    provider: CELL_PROVIDER_KIND,
    protocol: CELL_REPLAY_PROTOCOL,
    version: "live",
    scenario: `live_${scenarioName}_${runTag}`,
    entries: [],
  };
  const fixtureInput = { steps: stepsFor(scenarioName) };
  const materialized = yield* materializeFixtureInput({
    scenario: transcript.scenario,
    fixtureInput,
    driver: harness.driver,
    modelSelection: CELL_MODEL_SELECTION,
  });
  log(
    `scenario ${scenarioName} (tag ${runTag}) against ${baseUrl}; steps: ${fixtureInput.steps.map((step) => step.type).join(", ")}`,
  );

  if (scenarioName === "reconnect") {
    onPromptAccepted = (body) => {
      log(
        `reconnect: prompt accepted (${body}); terminating the socket in ${RECONNECT_KILL_AFTER_MS} ms and holding the next connect ${RECONNECT_HOLD_MS} ms`,
      );
      setTimeout(() => {
        holdConnectUntil = Date.now() + RECONNECT_HOLD_MS;
        log(`reconnect: terminating ws#${socketOrdinal} now`);
        latestSocket?.terminate();
      }, RECONNECT_KILL_AFTER_MS);
      onPromptAccepted = undefined;
    };
  }

  if (scenarioName === "delete-delegate") {
    const workspace = yield* checkpointWorkspace("live-delete-delegate");
    const threadId = materialized.projectionThreadIds[0]!;
    const firstIdle = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
    const scenario = {
      name: `${transcript.scenario}:delete`,
      transcript,
      commands: materialized.commands,
      steps: materialized.steps.slice(0, firstIdle),
      projectionThreadIds: materialized.projectionThreadIds,
      runtimePolicyOverride: { cwd: workspace },
    };
    const cancelSent = new Promise<TaskCancelRequest>((resolve) => {
      onTaskCancelSent = resolve;
    });
    let cancelAnswer: Parameters<NonNullable<typeof onTaskCancelAnswered>>[0] | undefined;
    onTaskCancelAnswered = (answer) => {
      cancelAnswer = answer;
    };
    yield* Effect.scoped(
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* runOrchestratorV2Scenario(scenario);
        const running = yield* until(
          orchestrator.getThreadProjection(threadId),
          (current) =>
            current.runs[0]?.status === "completed" &&
            current.subagents.some((task) => task.status === "running"),
          "the first run to complete while its delegated task still runs",
        );
        report("delete-delegate:before-delete", running);
        log("delete-delegate: dispatching thread.delete");
        yield* orchestrator.dispatch({
          type: "thread.delete",
          commandId: CommandId.make(`command:live-delete-${runTag}`),
          threadId,
        });
        const settled = yield* until(
          orchestrator.getThreadProjection(threadId),
          (current) =>
            current.thread.deletedAt !== null &&
            current.subagents.every((task) => task.status !== "running"),
          "the deletion to settle the task rows",
        );
        report("delete-delegate:after-delete", settled);
        // The session detach the deletion queues sends the cancel.
        const sent = yield* Effect.promise(() => cancelSent).pipe(
          Effect.timeoutOrElse({
            duration: "30 seconds",
            orElse: () =>
              Effect.die(new Error("The deletion never sent the cell a task cancel request")),
          }),
          TestClock.withLive,
        );
        const taskId = decodeURIComponent(/\/tasks\/([^/]+)\/cancel$/.exec(sent.url)?.[1] ?? "");
        log(`delete-delegate: the cancel of task ${taskId} was sent`);
        // The cell's state is read on its own: the adapter gives up on the
        // answer after 3 s, while the cell has 30 s to carry the cancel out,
        // and a cancelled task may leave the roster.
        const tasksUrl = sent.url.replace(/\/tasks\/[^/]+\/cancel$/, "/tasks");
        let listed: string | undefined;
        for (let attempt = 0; attempt < 35; attempt++) {
          const tasks = yield* transport.request({
            method: "GET",
            url: tasksUrl,
            headers: sent.headers,
          });
          listed =
            tasks.status === 200
              ? ((JSON.parse(tasks.body) as ReadonlyArray<{ taskId: string; status: string }>).find(
                  (task) => task.taskId === taskId,
                )?.status ?? "removed")
              : `unreadable (${tasks.status})`;
          if (listed !== "running" && !listed.startsWith("unreadable")) break;
          yield* TestClock.withLive(Effect.sleep("1 second"));
        }
        const answered =
          cancelAnswer === undefined
            ? "no answer to the cancel had been observed (the adapter may have stopped waiting for it)"
            : `the cell answered the cancel ${cancelAnswer.status}`;
        if (listed !== "cancelled" && listed !== "removed")
          return yield* Effect.die(
            new Error(
              `The cancel was sent and ${answered}, but the cell lists task ${taskId} as ${listed ?? "unread"}`,
            ),
          );
        log(`delete-delegate: ${answered}; the cell lists task ${taskId} as ${listed}`);
        log(`delete-delegate: check passed: the cell no longer runs task ${taskId}`);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerProviderReplay(scenario, harness, {
            runContinuationWorker: true,
          }),
        ),
      ),
    );
    return;
  }

  if (scenarioName !== "restart") {
    const workspace = yield* checkpointWorkspace(`live-${scenarioName}`);
    const result = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${transcript.scenario}/cell:live`,
        transcript,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { cwd: workspace },
      },
      harness,
      scenarioName === "delegate" ? { runContinuationWorker: true } : {},
    );
    for (const [threadId, projection] of result.projections)
      report(`${scenarioName}:${threadId.slice(-12)}`, projection);
    const rosterHistory = result.domainEvents
      .flatMap((event) =>
        event.type === "provider-thread.updated"
          ? [`${event.payload.status}:${event.payload.pendingBackgroundTasks?.length ?? 0}`]
          : [],
      )
      .join(" ");
    log(`provider-thread.updated history (status:pendingBackgroundTasks): ${rosterHistory}`);
    if (scenarioName === "attach-retry") {
      const [projection] = result.projections.values();
      const answered = projection?.turnItems.some(
        (item) => item.type === "assistant_message" && item.text.trim() === "attach retry ok",
      );
      if (
        !firstSocketCut ||
        socketOrdinal < 2 ||
        projection?.runs[0]?.status !== "completed" ||
        !answered
      )
        return yield* Effect.die(
          new Error(
            `attach-retry: expected the first socket cut and the prompt completed on a later one (sockets ${socketOrdinal}, run ${projection?.runs[0]?.status})`,
          ),
        );
      log(`attach-retry: check passed: ${socketOrdinal} sockets, the prompt completed`);
    }
    return;
  }

  // restart: two runtimes over one database
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tempDir = yield* Effect.acquireRelease(
    fs.makeTempDirectory({ prefix: "t3-live-cell-restart-" }),
    (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
  );
  const workspace = yield* checkpointWorkspace("live-restart");
  const layerDatabase = SqlitePersistence.layerFromPath(path.join(tempDir, "state.sqlite")).pipe(
    Layer.provide(NodeServices.layer),
  );
  const threadId = materialized.projectionThreadIds[0]!;
  const firstIdle = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
  const scenario = (name: string) => ({
    name: `${transcript.scenario}:${name}`,
    transcript,
    commands: materialized.commands,
    steps: materialized.steps.slice(0, firstIdle),
    projectionThreadIds: materialized.projectionThreadIds,
    runtimePolicyOverride: { cwd: workspace },
  });
  const runtimeOptions = {
    databaseLayer: layerDatabase,
    runContinuationWorker: true,
    continueThreadsAfterServerUpdate: true,
  };
  log("restart phase 1: dispatch the prompt, wait for the strong head, then stop like the server");
  const before = yield* Effect.scoped(
    Effect.gen(function* () {
      yield* followWallClock.pipe(Effect.forkScoped);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* runOrchestratorV2Scenario(scenario("before-restart"));
      const projection = yield* until(
        worker
          .drain()
          .pipe(Effect.orDie, Effect.andThen(orchestrator.getThreadProjection(threadId))),
        (current) => strongHead(current) !== undefined && current.runs[0]?.status === "running",
        "the run in progress with a strong head naming it",
      );
      report("restart:before-stop", projection);
      const recovery = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      log("restart phase 1: prepareForShutdown, sessions.shutdown, reconcile(shutdown)");
      yield* recovery.prepareForShutdown.pipe(Effect.ensuring(sessions.shutdown));
      yield* recovery.reconcile("shutdown");
      const stopped = yield* orchestrator.getThreadProjection(threadId);
      report("restart:stopped", stopped);
      return { projection, stopped };
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerProviderReplay(scenario("before-restart"), harness, {
          ...runtimeOptions,
          runEffectWorker: false,
        }),
      ),
    ),
  );
  const [sourceRun] = before.projection.runs;
  log(
    `restart: cell still runs ${strongHead(before.projection)?.nativeId}; starting phase 2 (recoverOnStartup)`,
  );
  const after = yield* Effect.scoped(
    Effect.gen(function* () {
      yield* followWallClock.pipe(Effect.forkScoped);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* worker.drain();
      const continued = yield* until(
        orchestrator.getThreadProjection(threadId),
        (current) =>
          current.runs.some(
            (run) =>
              run.restartContinuationOfRunId === sourceRun?.id &&
              run.status !== "queued" &&
              run.status !== "starting" &&
              run.status !== "preparing",
          ),
        "the restart continuation to start",
      );
      report("restart:continuation-started", continued);
      return yield* until(
        orchestrator.getThreadProjection(threadId),
        (current) =>
          current.runs.length >= 2 &&
          current.runs.every(
            (run) =>
              run.status === "completed" || run.status === "cancelled" || run.status === "failed",
          ),
        "every run to settle",
      );
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerProviderReplay(scenario("after-restart"), harness, {
          ...runtimeOptions,
          recoverOnStartup: true,
        }),
      ),
    ),
  );
  report("restart:after", after);
});

await Effect.runPromise(
  main.pipe(
    Effect.scoped,
    provideDeterministicTestRuntime,
    Effect.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        NodeServices.layer,
        NodeHttpClient.layerUndici,
        layerWebSocketConstructor,
      ),
    ),
  ),
);
log("driver finished");
process.exit(0);
