/**
 * Failures the adapter raises before a prompt reaches the cell. Each carries
 * a message written for the thread's error row, because the orchestrator
 * shows the nested cause of a session-open or thread-load failure as is.
 */
import { RuntimeMode } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const CELL_RUNTIME_MODE_MESSAGE =
  "Cell runs only in Full access. Set this thread's permission mode to Full access and send the message again.";

export class CellRuntimeModeError extends Schema.TaggedError<CellRuntimeModeError>()(
  "CellRuntimeModeError",
  { runtimeMode: RuntimeMode },
) {
  override get message(): string {
    return CELL_RUNTIME_MODE_MESSAGE;
  }
}

export class CellModelError extends Schema.TaggedError<CellModelError>()("CellModelError", {
  requested: Schema.String,
  available: Schema.String,
}) {
  override get message(): string {
    return `This cell runs ${this.available} and cannot switch to ${this.requested}. Pick Default or ${this.available} in the model picker and send the message again.`;
  }
}
