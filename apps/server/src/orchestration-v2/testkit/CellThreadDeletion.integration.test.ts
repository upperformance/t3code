// @effect-diagnostics globalTimers:off - the fake cell runs live I/O; the orchestrator runs on the test clock.
/**
 * A thread deleted while a cell runs a task its finished run delegated. The
 * deletion settles the task's rows and the session's detach asks the cell to
 * cancel the task, then releases the session. The run settled long before, so
 * the release ending its event stream only ends the watch on the task: it is
 * an expected close, logged as information, not an ingestion failure.
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  type OrchestrationV2ThreadProjection,
  type ProviderReplayTranscript,
  type ThreadId,
} from "@t3tools/contracts";
import { makeHttpCellTransport, type CellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import { CELL_REPLAY_PROTOCOL, startFakeCellServer } from "@t3tools/provider-cell/testing";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
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
import * as EventSink from "../EventSink.ts";
import * as Orchestrator from "../Orchestrator.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { CELL_MODEL_SELECTION, materializeFixtureInput } from "./fixtures/shared.ts";
import { runOrchestratorV2Scenario } from "./OrchestratorScenario.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";

const SCENARIO = "cell_thread_deletion";
const PROMPT = "Delegate a review to a child cell.";
const TASK_ID = "ab12";
const INGESTION_FAILED = "orchestration V2 provider event ingestion failed";
const STREAM_ENDED = "orchestration V2 provider event stream ended after its run settled";
/** A bound on each wait, on the wall clock: the orchestrator's clock moves only when the test moves it. */
const WAIT_LIMIT = Duration.seconds(25);

const decodeCellSettings = Schema.decodeUnknownEffect(CellSettings);

/** The thread's projection once `ready` holds for it, read again after every event the thread stores. */
const once = Effect.fnUntraced(function* (
  threadId: ThreadId,
  ready: (projection: OrchestrationV2ThreadProjection) => boolean,
  what: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const read = orchestrator.getThreadProjection(threadId);
  const latest = yield* sink.latestSequence({ threadId });
  const first = yield* read;
  if (ready(first)) return first;
  const found = yield* sink.stream({ threadId, afterSequence: latest }).pipe(
    Stream.mapEffect(() => read),
    Stream.filter(ready),
    Stream.take(1),
    Stream.runCollect,
    Effect.timeoutOrElse({
      duration: WAIT_LIMIT,
      orElse: () => Effect.die(new Error(`Gave up waiting for ${what}`)),
    }),
    TestClock.withLive,
  );
  return found[0]!;
});

describe("cell thread deletion with background work", () => {
  // The logger is installed around the whole runtime, so the run's ingestion
  // fiber, forked inside it, logs to it.
  const warnings: Array<string> = [];
  const streamEnds: Array<string> = [];
  let streamEnded = () => {};
  const ended = new Promise<void>((resolve) => {
    streamEnded = resolve;
  });
  const capture = Logger.make(({ message }) => {
    const [text] = Array.isArray(message) ? message : [message];
    if (text === INGESTION_FAILED) warnings.push(text);
    if (text === STREAM_ENDED) streamEnds.push(text);
    if (text === INGESTION_FAILED || text === STREAM_ENDED) streamEnded();
  });

  it.effect(
    "the deletion cancels the task in the cell and ends the settled run's stream without an ingestion warning",
    () =>
      Effect.gen(function* () {
        const fake = yield* Effect.acquireRelease(
          Effect.promise(() =>
            startFakeCellServer({
              // The child would report a minute later: within the test it never does.
              delegateDelayMs: 60_000,
              script: () => [
                {
                  type: "delegate",
                  taskId: TASK_ID,
                  title: "review",
                  summary: "The review is done.",
                  wake: [],
                },
                { type: "assistant", text: "STARTED" },
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
        const live = yield* makeHttpCellTransport;
        // The task's cancel is answered here rather than by the fake cell,
        // whose announcement of it could otherwise end the run's watch on the
        // task before the release does: the release is what is under test.
        const cancelled: Array<string> = [];
        const transport: CellTransport = {
          ...live,
          request: (request) => {
            const taskId = /\/tasks\/([^/]+)\/cancel$/.exec(new URL(request.url).pathname)?.[1];
            if (taskId === undefined) return live.request(request);
            cancelled.push(decodeURIComponent(taskId));
            return Effect.succeed({
              status: 200,
              body: JSON.stringify({ taskId, title: "review", status: "cancelled" }),
            });
          },
        };
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
        const workspace = yield* checkpointWorkspace(SCENARIO);
        const materialized = yield* materializeFixtureInput({
          scenario: SCENARIO,
          fixtureInput: { steps: [{ type: "message", text: PROMPT }] },
          driver: CellOrchestratorReplayHarness.driver,
          modelSelection: CELL_MODEL_SELECTION,
        });
        const threadId = materialized.projectionThreadIds[0]!;
        const firstIdle = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
        const scenario = {
          name: SCENARIO,
          transcript,
          commands: materialized.commands,
          steps: materialized.steps.slice(0, firstIdle),
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { cwd: workspace },
        };

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* runOrchestratorV2Scenario(scenario);
          const running = yield* once(
            threadId,
            (current) =>
              current.runs[0]?.status === "completed" &&
              current.subagents.some((subagent) => subagent.status === "running"),
            "the run to complete while its delegated task runs",
          );
          assert.lengthOf(running.subagents, 1);

          yield* orchestrator.dispatch({
            type: "thread.delete",
            commandId: CommandId.make("delete-while-delegating"),
            threadId,
          });
          yield* Effect.promise(() => ended).pipe(
            Effect.timeoutOrElse({
              duration: WAIT_LIMIT,
              orElse: () => Effect.die(new Error("the run's event stream never ended")),
            }),
            TestClock.withLive,
          );
          const deleted = yield* orchestrator.getThreadProjection(threadId);
          assert.isNotNull(deleted.thread.deletedAt);
          assert.equal(deleted.runs[0]?.status, "completed", "the settled run stays completed");
          assert.deepEqual(
            deleted.subagents.map((subagent) => subagent.status),
            ["cancelled"],
          );
        }).pipe(
          Effect.scoped,
          Effect.provide(
            ProviderReplayHarness.layerProviderReplay(scenario, harness, {
              runContinuationWorker: true,
            }),
          ),
        );

        assert.deepEqual(cancelled, [TASK_ID], "the cell was asked to cancel the task");
        assert.deepEqual(warnings, [], "a deletion is not an ingestion failure");
        assert.deepEqual(streamEnds, [STREAM_ENDED]);
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            NodeServices.layer,
            NodeHttpClient.layerUndici,
            NodeSocket.layerWebSocketConstructorWS,
            Logger.layer([capture]),
          ),
        ),
      ),
  );
});
