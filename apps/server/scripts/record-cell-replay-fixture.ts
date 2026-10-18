// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - a recorder: it drives live I/O under the test clock.
/**
 * Records a `cell.http-ws` transcript for one shared fixture by running the
 * real orchestrator and adapter against a cell over the network, then
 * verifies the recording by replaying it. Pass `--url https://<tenant>` to
 * record against a tenant; without it the fake cell from
 * `@t3tools/provider-cell/testing` answers on a local port.
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import type { ProviderReplayEntry, ProviderReplayTranscript } from "@t3tools/contracts";
import { makeHttpCellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import {
  CELL_REPLAY_PROTOCOL,
  makeRecordingCellTransport,
  startFakeCellServer,
} from "@t3tools/provider-cell/testing";
import * as Console from "effect/Console";
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
  layer as cellReplayLayer,
} from "../src/orchestration-v2/Adapters/CellAdapterV2.testkit.ts";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";

const CLOCK_TICK = Duration.millis(20);

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenario = readArgValue("--scenario");
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((entry) => entry.name === scenario);
const variant = fixture?.providers.find((provider) => provider.driver === CELL_PROVIDER_KIND);
if (fixture === undefined || variant === undefined) {
  const names = ORCHESTRATOR_REPLAY_FIXTURES.filter((entry) =>
    entry.providers.some((provider) => provider.driver === CELL_PROVIDER_KIND),
  ).map((entry) => entry.name);
  throw new Error(`Pass --scenario with a fixture that registers cell: ${names.join(", ")}`);
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeCellSettings = Schema.decodeUnknownEffect(CellSettings);

function encodeTranscriptNdjson(transcript: ProviderReplayTranscript): string {
  const { entries, ...metadata } = transcript;
  return [
    encodeJson({ type: "transcript_start", ...metadata }),
    ...entries.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
}

/** Recording follows wall time so the cell's own timeouts still elapse. */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const requestedUrl = readArgValue("--url");
  const fake =
    requestedUrl === undefined ? yield* Effect.promise(() => startFakeCellServer()) : undefined;
  if (fake) yield* Effect.addFinalizer(() => Effect.promise(() => fake.close()));
  const baseUrl = requestedUrl ?? fake!.baseUrl;
  const settings = yield* decodeCellSettings({
    enabled: true,
    baseUrl,
    token: readArgValue("--token") ?? "",
  });
  const liveTransport = yield* makeHttpCellTransport;
  const fixtureInput = fixture.buildInput();
  const entries: Array<ProviderReplayEntry> = [];
  const materializeInput = materializeFixtureInput({
    scenario: fixture.name,
    fixtureInput,
    driver: variant.driver,
    modelSelection: variant.modelSelection,
  }).pipe(Effect.provide(IdAllocator.layer));
  const continuationOptions =
    variant.runContinuationWorker === true ? { runContinuationWorker: true } : {};

  yield* Effect.gen(function* () {
    yield* followWallClock.pipe(Effect.forkScoped);
    const workspace = yield* checkpointWorkspace(
      `cell-record-${fixture.name}`,
      fixtureInput.workspaceFiles,
    );
    const materialized = yield* materializeInput;
    const recorded = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/cell:record`,
        transcript: {
          provider: CELL_PROVIDER_KIND,
          protocol: CELL_REPLAY_PROTOCOL,
          version: "live",
          scenario: fixture.name,
          entries: [],
        },
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: workspace },
      },
      {
        driver: CellOrchestratorReplayHarness.driver,
        decodeTranscript: (transcript: ProviderReplayTranscript) => Effect.succeed(transcript),
        // The gate paces the recording like its replay: a gated child result
        // waits for the step that releases it, so the fixture's run order holds.
        makeProviderAdapterRegistryLayer: (_transcript, options) =>
          cellReplayLayer({
            transport: makeRecordingCellTransport(
              liveTransport,
              entries,
              options?.replayGate === undefined ? {} : { gate: options.replayGate },
            ),
            settings,
          }),
      },
      continuationOptions,
    );
    for (const projection of recorded.projections.values()) {
      yield* Console.log(
        `recorded runs: ${projection.runs.map((run) => run.status).join(", ")}; failures: ${projection.turnItems
          .flatMap((item) => (item.type === "error" ? [item.failure.message] : []))
          .join(" | ")}`,
      );
    }
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  const transcript = {
    provider: CELL_PROVIDER_KIND,
    protocol: CELL_REPLAY_PROTOCOL,
    // The cell reports its version only in `GET state`, which the adapter never calls.
    version: fake ? "fake-cell" : (readArgValue("--version") ?? "live"),
    scenario: fixture.name,
    metadata: {
      generatedBy: fake ? "fake-cell-recorder" : "live-cell-recorder",
      model: variant.modelSelection.model,
    },
    entries,
  } satisfies ProviderReplayTranscript;

  // Written before the verification replay, so a failed verification leaves
  // the recording behind for inspection instead of only its error.
  yield* fs.makeDirectory(path.dirname(outputPath), { recursive: true });
  yield* fs.writeFileString(outputPath, encodeTranscriptNdjson(transcript));
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
    const materialized = yield* materializeInput;
    const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/cell:verify`,
        transcript: yield* CellOrchestratorReplayHarness.decodeTranscript(transcript),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      CellOrchestratorReplayHarness,
      continuationOptions,
    );
    variant.assertOutput(replayResult, transcript);
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);
  yield* Console.log(
    `Wrote and verified ${transcript.entries.length} cell replay entries at ${outputPath}`,
  );
});

// The undici dispatcher lives in its layer's scope, so the layers wrap the whole recording.
await Effect.runPromise(
  record.pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeHttpClient.layerUndici,
        NodeSocket.layerWebSocketConstructorWS,
      ),
    ),
  ),
);
