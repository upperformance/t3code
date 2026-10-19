// @effect-diagnostics globalTimers:off - the fake cell runs live I/O; the orchestrator runs on the test clock.
/**
 * A server restart while a cell runs a turn that has delegated work. The
 * cell notices nothing: the run and the child go on. T3's restart has to
 * take the run back natively, which it can only do if recovery records none
 * of that work as cancelled: a restart note would turn the continuation into
 * a prompt, which the cell refuses while its own run is in progress. The
 * child's rows survive too: the next session is handed them with the thread,
 * and the child's result lands on the subagent row that showed it start.
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, describe, it } from "@effect/vitest";
import type {
  OrchestrationV2ThreadProjection,
  ProviderReplayTranscript,
  ThreadId,
} from "@t3tools/contracts";
import { makeHttpCellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import { CELL_REPLAY_PROTOCOL, startFakeCellServer } from "@t3tools/provider-cell/testing";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  CELL_PROVIDER_KIND,
  CellOrchestratorReplayHarness,
  layer as cellLayer,
} from "../Adapters/CellAdapterV2.testkit.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as EventSink from "../EventSink.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProviderRuntimeRecoveryService from "../ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { CELL_MODEL_SELECTION, materializeFixtureInput } from "./fixtures/shared.ts";
import { runOrchestratorV2Scenario } from "./OrchestratorScenario.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";

const SCENARIO = "cell_restart_adoption";
const PROMPT = "Delegate a review to a child cell, then keep working.";
const RESTART_NOTE_PREFIX = "Note: the T3 server restarted";
const TASK_ID = "ab12";
const RESULT = "The review is done.";
/** A bound on each wait, on the wall clock: the orchestrator's clock moves only when the test moves it. */
const WAIT_LIMIT = Duration.seconds(25);

const decodeCellSettings = Schema.decodeUnknownEffect(CellSettings);

/** The thread's state in one line per record, for a wait that gave up. */
const describeProjection = (projection: OrchestrationV2ThreadProjection) =>
  [
    ...projection.runs.map(
      (run) =>
        `run ${run.id} ${run.status} continuationOf=${run.restartContinuationOfRunId ?? "-"} note=${JSON.stringify(run.restartCancelledBackgroundWork ?? [])}`,
    ),
    ...projection.messages.map((message) => `message ${message.id}: ${message.text.slice(0, 120)}`),
    ...projection.turnItems.flatMap((item) =>
      item.type === "error" ? [`error ${item.runId}: ${item.failure.message}`] : [],
    ),
    ...projection.providerTurns.map(
      (turn) => `providerTurn ${turn.ordinal} ${turn.status} attempt=${turn.runAttemptId}`,
    ),
    ...projection.turnItems.map((item) => `item ${item.type} ${item.status}`),
    ...projection.subagents.map(
      (subagent) =>
        `subagent ${subagent.nativeTaskRef?.nativeId} ${subagent.status} run=${subagent.runId} result=${JSON.stringify(subagent.result)}`,
    ),
    ...projection.providerThreads.map(
      (thread) =>
        `providerThread ${thread.status} head=${JSON.stringify(thread.nativeConversationHeadRef)} roster=${thread.pendingBackgroundTasks?.length ?? 0}`,
    ),
    ...projection.providerSessions.map(
      (session) => `session ${session.id} ${session.status} ${session.lastError ?? ""}`,
    ),
  ].join("\n");

/**
 * The thread's projection once `ready` holds for it. The projection is read
 * when the wait begins and again after every event the thread stores from
 * then on, so a milestone is seen when it is persisted and never polled for;
 * the stream starts at the sequence read first, so an event stored between
 * the two is not missed. The bound runs on the wall clock, since the
 * orchestrator's clock does not move on its own here.
 */
const once = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ready: (projection: OrchestrationV2ThreadProjection) => boolean,
  what: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const read = orchestrator.getThreadProjection(threadId);
  const latest = yield* sink.latestSequence({ threadId });
  let last = yield* read;
  if (ready(last)) return last;
  const found = yield* sink.stream({ threadId, afterSequence: latest }).pipe(
    Stream.mapEffect(() => read),
    Stream.tap((projection) =>
      Effect.sync(() => {
        last = projection;
      }),
    ),
    Stream.filter(ready),
    Stream.take(1),
    Stream.runCollect,
    Effect.timeoutOrElse({
      duration: WAIT_LIMIT,
      orElse: () =>
        Effect.die(new Error(`Gave up waiting for ${what}:\n${describeProjection(last)}`)),
    }),
    TestClock.withLive,
  );
  return found[0]!;
});

const strongHeadOrdinal = (projection: OrchestrationV2ThreadProjection) => {
  const head = projection.providerThreads[0]?.nativeConversationHeadRef;
  return head?.strength === "strong" ? (head.ordinal ?? -1) : -1;
};

const assistantTexts = (projection: OrchestrationV2ThreadProjection, runId: string | undefined) =>
  projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );

const settled = (status: string) => status === "completed" || status === "cancelled";

describe("cell restart with background work", () => {
  it.effect(
    "the restart continuation takes the surviving run back natively, with no cancellation note and no prompt, and the child's result lands on its original row",
    () =>
      Effect.gen(function* () {
        let release = () => {};
        const held = new Promise<void>((resolve) => {
          release = resolve;
        });
        const fake = yield* Effect.acquireRelease(
          Effect.promise(() =>
            startFakeCellServer({
              // The run delegates, answers once, then waits in the cell across the restart.
              script: () => [
                {
                  type: "delegate",
                  taskId: TASK_ID,
                  title: "review",
                  summary: RESULT,
                  wake: [{ type: "assistant", text: "WAKE_DONE" }],
                },
                { type: "assistant", text: "STARTED" },
                { type: "hold", until: held },
                { type: "assistant", text: "RESUMED" },
              ],
            }),
          ),
          (server) => Effect.promise(() => server.close()),
        );
        const settings = yield* decodeCellSettings({
          enabled: true,
          baseUrl: fake.baseUrl,
          token: "",
        });
        const transport = yield* makeHttpCellTransport;
        const harness = {
          driver: CellOrchestratorReplayHarness.driver,
          decodeTranscript: (transcript: ProviderReplayTranscript) => Effect.succeed(transcript),
          makeProviderAdapterRegistryLayer: () => cellLayer({ transport, settings }),
        };
        const transcript: ProviderReplayTranscript = {
          provider: CELL_PROVIDER_KIND,
          protocol: CELL_REPLAY_PROTOCOL,
          version: "live",
          scenario: SCENARIO,
          entries: [],
        };
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* Effect.acquireRelease(
          fs.makeTempDirectory({ prefix: "t3-orchestration-v2-cell-restart-" }),
          (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
        );
        const workspace = yield* checkpointWorkspace(SCENARIO);
        const layerDatabase = SqlitePersistence.layerFromPath(
          path.join(tempDir, "state.sqlite"),
        ).pipe(Layer.provide(NodeServices.layer));
        const materialized = yield* materializeFixtureInput({
          scenario: SCENARIO,
          fixtureInput: { steps: [{ type: "message", text: PROMPT }] },
          driver: CellOrchestratorReplayHarness.driver,
          modelSelection: CELL_MODEL_SELECTION,
        });
        const threadId = materialized.projectionThreadIds[0]!;
        const firstIdle = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
        const scenario = (name: string) => ({
          name: `${SCENARIO}:${name}`,
          transcript,
          commands: materialized.commands,
          // The thread is created and the message dispatched; the run is left in progress.
          steps: materialized.steps.slice(0, firstIdle),
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { cwd: workspace },
        });
        const runtimeOptions = {
          databaseLayer: layerDatabase,
          runContinuationWorker: true,
          continueThreadsAfterServerUpdate: true,
        };

        // The first server: the run delegates, answers once and is still in
        // progress with its child running when the server stops the way it
        // does: its effect worker stopped first, then the restart continuation
        // prepared, the sessions shut down and what they left reconciled. The
        // worker runs only while the run is driven, in a scope of its own, so
        // that it is not running when the continuation is prepared, which it
        // would otherwise execute at once against a run that is still live.
        const before = yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const projection = yield* Effect.scoped(
              Effect.gen(function* () {
                yield* EffectWorker.runDaemon.pipe(Effect.forkScoped);
                yield* runOrchestratorV2Scenario(scenario("before-restart"));
                return yield* once(
                  threadId,
                  (current) =>
                    current.turnItems.some(
                      (item) => item.type === "assistant_message" && item.text === "STARTED",
                    ) &&
                    current.subagents.some((subagent) => subagent.status === "running") &&
                    // The head follows the run's own events: user 1, tool 2 (the task is
                    // announced starting at 3 and running at 4), tool output 5, the answer 6.
                    strongHeadOrdinal(current) >= 6,
                  "the first answer, the delegated child and the persisted head naming the run",
                );
              }),
            );
            const recovery = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
            const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
            yield* recovery.prepareForShutdown.pipe(Effect.ensuring(sessions.shutdown));
            yield* recovery.reconcile("shutdown");
            return {
              projection,
              stopped: yield* orchestrator.getThreadProjection(threadId),
            };
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
        assert.equal(sourceRun?.status, "running");
        assert.equal(
          before.projection.providerThreads[0]?.nativeConversationHeadRef?.nativeId,
          "run-1",
        );
        const [child] = before.projection.subagents;
        assert.lengthOf(before.projection.subagents, 1);
        assert.equal(child?.nativeTaskRef?.nativeId, TASK_ID);
        assert.equal(child?.status, "running");
        const childItem = before.projection.turnItems.find((item) => item.type === "subagent");
        assert.equal(childItem?.status, "running");
        const stoppedState = describeProjection(before.stopped);
        assert.equal(
          before.stopped.subagents[0]?.status,
          "running",
          `the stop leaves the child's row open, since the child runs on: ${stoppedState}`,
        );
        assert.deepEqual(
          before.stopped.providerThreads[0]?.pendingBackgroundTasks?.map((task) => task.taskId),
          [TASK_ID],
          "the stop leaves the roster, which is still true",
        );

        // The second server starts later than the first stopped; nothing
        // else moves the orchestrator's clock.
        yield* TestClock.adjust("1 minute");

        // The second server recovers, continues the thread, and the adapter
        // takes the cell's run back: once released, the rest of the run and
        // then the child's result reach the thread without a prompt.
        const after = yield* Effect.scoped(
          Effect.gen(function* () {
            const continued = yield* once(
              threadId,
              (current) =>
                current.runs.some(
                  (run) =>
                    run.restartContinuationOfRunId === sourceRun?.id &&
                    run.status !== "queued" &&
                    run.status !== "starting" &&
                    run.status !== "preparing",
                ),
              `the restart continuation to start (after the stop: ${stoppedState})`,
            );
            const continuationRun = continued.runs.find(
              (run) => run.restartContinuationOfRunId === sourceRun?.id,
            );
            assert.equal(
              continuationRun?.status,
              "running",
              `the continuation must adopt the cell's run and wait on it: ${describeProjection(continued)}`,
            );
            release();
            const projection = yield* once(
              threadId,
              (current) =>
                current.runs.length >= 3 &&
                current.runs.every((run) => settled(run.status)) &&
                current.subagents.every((subagent) => subagent.status === "completed"),
              "the resumed turn, the child's result and the child's wake to complete",
            );
            // What the child's row went through, across both servers.
            const sink = yield* EventSink.EventSinkV2;
            const latest = yield* sink.latestSequence({ threadId });
            const stored = yield* sink.stream({ threadId }).pipe(
              Stream.takeUntil((entry) => entry.sequence >= latest),
              Stream.runCollect,
            );
            const childStatuses = stored
              .flatMap((entry) =>
                entry.event.type === "subagent.updated" && entry.event.payload.id === child?.id
                  ? [entry.event.payload.status]
                  : [],
              )
              .filter((status, index, all) => status !== all[index - 1]);
            return { projection, childStatuses };
          }).pipe(
            Effect.provide(
              ProviderReplayHarness.layerProviderReplay(scenario("after-restart"), harness, {
                ...runtimeOptions,
                recoverOnStartup: true,
              }),
            ),
          ),
        );

        const final = after.projection;
        const project = final.providerThreads[0]?.nativeThreadRef?.nativeId ?? "";
        assert.deepEqual(fake.prompts(project), [PROMPT], "the restart posts no prompt");
        const [cut, continuation, wake] = final.runs;
        assert.equal(cut?.id, sourceRun?.id);
        assert.equal(cut?.status, "cancelled", "the server's stop ends the run's record");
        assert.deepEqual(
          cut?.restartCancelledBackgroundWork ?? [],
          [],
          "the child runs on in the cell, so nothing is recorded as cancelled",
        );
        assert.equal(continuation?.restartContinuationOfRunId, sourceRun?.id);
        assert.equal(continuation?.status, "completed");
        assert.deepEqual(assistantTexts(final, continuation?.id), ["RESUMED"]);
        assert.equal(wake?.status, "completed");
        assert.deepEqual(assistantTexts(final, wake?.id), ["WAKE_DONE"]);
        const noted = final.messages.filter((message) =>
          message.text.includes(RESTART_NOTE_PREFIX),
        );
        assert.deepEqual(noted, [], "no turn is told its background work was cancelled");
        assert.equal(final.providerThreads[0]?.nativeConversationHeadRef?.strength, "weak");
        assert.deepEqual(final.providerThreads[0]?.pendingBackgroundTasks ?? [], []);

        // The child's result lands on the row the first server showed, which
        // the continuation took over; the restart created no second row.
        assert.lengthOf(final.subagents, 1, "the child stays one subagent row");
        const [done] = final.subagents;
        assert.equal(done?.id, child?.id);
        assert.equal(done?.status, "completed");
        assert.equal(done?.result, RESULT);
        assert.equal(done?.runId, continuation?.id, "the turn that took the run back owns the row");
        assert.deepEqual(after.childStatuses, ["running", "completed"]);
        const childItems = final.turnItems.filter((item) => item.type === "subagent");
        assert.lengthOf(childItems, 1);
        assert.equal(childItems[0]?.id, childItem?.id);
        assert.equal(childItems[0]?.ordinal, childItem?.ordinal);
        assert.equal(childItems[0]?.status, "completed");
        assert.equal(childItems[0]?.type === "subagent" ? childItems[0].result : null, RESULT);
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            NodeServices.layer,
            NodeHttpClient.layerUndici,
            NodeSocket.layerWebSocketConstructorWS,
          ),
        ),
      ),
  );
});
