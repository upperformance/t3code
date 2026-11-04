import { describe } from "@effect/vitest";

import { CellOrchestratorReplayHarness } from "../Adapters/CellAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Cell replay fixtures", () => {
  itRunsReplayFixtures("cell", (input) =>
    runReplayFixture({ ...input, harness: CellOrchestratorReplayHarness }),
  );
});
