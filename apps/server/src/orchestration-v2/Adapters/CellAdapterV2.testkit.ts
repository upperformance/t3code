import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderReplayEntry,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import { makeCellAdapterV2, type CellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import { CELL_REPLAY_PROTOCOL, makeReplayCellTransport } from "@t3tools/provider-cell/testing";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";

export const CELL_PROVIDER_KIND = "cell";

const CellReplayTranscript = Schema.Struct({
  provider: Schema.Literal(CELL_PROVIDER_KIND),
  protocol: Schema.Literal(CELL_REPLAY_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
type CellReplayTranscript = typeof CellReplayTranscript.Type;
const decodeCellReplayTranscript = Schema.decodeUnknownEffect(CellReplayTranscript);

class CellReplayTranscriptDecodeError extends Schema.TaggedError<CellReplayTranscriptDecodeError>()(
  "CellReplayTranscriptDecodeError",
  { scenario: Schema.optional(Schema.String), cause: Schema.Defect() },
) {
  override get message(): string {
    return `Failed to decode cell replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

/** The base URL only shapes request URLs; replay matches paths, so any origin will do. */
const CELL_REPLAY_SETTINGS = Schema.decodeUnknownSync(CellSettings)({
  enabled: true,
  baseUrl: "https://cell.replay.test",
});

export function layer(input: {
  readonly transport: CellTransport;
  readonly settings?: CellSettings;
}) {
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    Effect.gen(function* () {
      return [
        yield* makeCellAdapterV2({
          instanceId: ProviderInstanceId.make(CELL_PROVIDER_KIND),
          settings: input.settings ?? CELL_REPLAY_SETTINGS,
          transport: input.transport,
          // Same queue the continuation worker drains when the fixture runs it.
          continuationRequests: yield* ProviderContinuationRequests.ProviderContinuationRequests,
        }),
      ];
    }),
  ).pipe(Layer.provide(IdAllocator.layer));
}

export const CellOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  CellReplayTranscript,
  CellReplayTranscriptDecodeError
> = {
  driver: ProviderDriverKind.make(CELL_PROVIDER_KIND),
  decodeTranscript: (transcript: ProviderReplayTranscript) =>
    decodeCellReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) => new CellReplayTranscriptDecodeError({ scenario: transcript.scenario, cause }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (transcript) =>
    Layer.unwrap(
      Effect.gen(function* () {
        const controller = makeReplayCellTransport({
          scenario: transcript.scenario,
          entries: transcript.entries,
        });
        yield* Effect.addFinalizer(() => Effect.sync(() => controller.assertComplete()));
        return layer({ transport: controller.transport });
      }),
    ),
};
