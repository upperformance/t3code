// @effect-diagnostics globalTimers:off - the fake cell and the restart drive live I/O under the test clock.
/**
 * A server restart while a cell runs a turn that has delegated work. The
 * cell notices nothing: the run and the child go on. T3's restart has to
 * take the run back natively, which it can only do if recovery records none
 * of that work as cancelled: a restart note would turn the continuation into
 * a prompt, which the cell refuses while its own run is in progress.
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";
import { makeHttpCellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import { CELL_REPLAY_PROTOCOL, startFakeCellServer } from "@t3tools/provider-cell/testing";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  CELL_PROVIDER_KIND,
  CellOrchestratorReplayHarness,
  layer as cellLayer,
} from "../Adapters/CellAdapterV2.testkit.ts";
import * as EffectWorker from "../EffectWorker.ts";
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
const CLOCK_TICK = Duration.millis(20);
const POLL_LIMIT = 2_000;

const decodeCellSettings = Schema.decodeUnknownEffect(CellSettings);

/** The adapter's timeouts and the orchestrator's timers run on the test clock; the cell runs on the wall clock. */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

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
    ...projection.providerThreads.map(
      (thread) =>
        `providerThread ${thread.status} head=${JSON.stringify(thread.nativeConversationHeadRef)} roster=${thread.pendingBackgroundTasks?.length ?? 0}`,
    ),
    ...projection.providerSessions.map(
      (session) => `session ${session.id} ${session.status} ${session.lastError ?? ""}`,
    ),
  ].join("\n");

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

const strongHeadOrdinal = (projection: OrchestrationV2ThreadProjection) => {
  const head = projection.providerThreads[0]?.nativeConversationHeadRef;
  return head?.strength === "strong" ? (head.ordinal ?? -1) : -1;
};

const assistantTexts = (projection: OrchestrationV2ThreadProjection, runId: string | undefined) =>
  projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );

describe("cell restart with background work", () => {
  it.effect(
    "the restart continuation takes the surviving run back natively, with no cancellation note and no prompt",
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
                  taskId: "ab12",
                  title: "review",
                  summary: "The review is done.",
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
        // prepared, the sessions shut down and what they left reconciled.
        // The worker is driven by hand here so that it is not running when
        // the continuation is prepared, which it would otherwise execute at
        // once against a run that is still live.
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
              (current) =>
                current.turnItems.some(
                  (item) => item.type === "assistant_message" && item.text === "STARTED",
                ) &&
                current.turnItems.some(
                  (item) => item.type === "subagent" && item.status === "running",
                ) &&
                strongHeadOrdinal(current) >= 5,
              "the first answer, the delegated child and the persisted head naming the run",
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
        const stoppedState = describeProjection(before.stopped);

        // The second server recovers, continues the thread, and the adapter
        // takes the cell's run back: once released, the rest of the run and
        // then the child's result reach the thread without a prompt.
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
            return yield* until(
              orchestrator.getThreadProjection(threadId),
              (current) =>
                current.runs.length >= 3 &&
                current.runs.every(
                  (run) => run.status === "completed" || run.status === "cancelled",
                ),
              "the resumed turn and the child's wake to complete",
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

        const project = after.providerThreads[0]?.nativeThreadRef?.nativeId ?? "";
        assert.deepEqual(fake.prompts(project), [PROMPT], "the restart posts no prompt");
        const [cut, continuation, wake] = after.runs;
        assert.equal(cut?.id, sourceRun?.id);
        assert.equal(cut?.status, "cancelled", "the server's stop ends the run's record");
        assert.deepEqual(
          cut?.restartCancelledBackgroundWork ?? [],
          [],
          "the child runs on in the cell, so nothing is recorded as cancelled",
        );
        assert.equal(continuation?.restartContinuationOfRunId, sourceRun?.id);
        assert.equal(continuation?.status, "completed");
        assert.deepEqual(assistantTexts(after, continuation?.id), ["RESUMED"]);
        assert.equal(wake?.status, "completed");
        assert.deepEqual(assistantTexts(after, wake?.id), ["WAKE_DONE"]);
        const noted = after.messages.filter((message) =>
          message.text.includes(RESTART_NOTE_PREFIX),
        );
        assert.deepEqual(noted, [], "no turn is told its background work was cancelled");
        assert.equal(after.providerThreads[0]?.nativeConversationHeadRef?.strength, "weak");
        assert.deepEqual(after.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
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
