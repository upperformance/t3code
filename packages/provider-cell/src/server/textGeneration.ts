import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import { fromRunner } from "@t3tools/provider-core/server/textGenerationOperations";

/**
 * A cell runs only its coding loop over the project tree; it has no side
 * channel for a one-off structured answer, so titles and commit messages come
 * from another provider.
 */
export const cellTextGeneration: ProviderTextGeneration = fromRunner(
  "CellTextGeneration",
  (request) =>
    Effect.fail(
      new TextGenerationError({
        operation: request.operation,
        detail:
          "Cell threads cannot generate text. Choose another provider for titles, commit messages, and pull requests.",
      }),
    ),
);
