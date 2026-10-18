/**
 * The cell's wire shapes: `GET state`, the WebSocket events, the prompt
 * responses, and the project naming the adapter and the status probe share.
 * Events the cell added after this build are ignored rather than failing the
 * socket, so an older T3 keeps working against a newer tenant. Run ids,
 * sequence numbers and structured `done` errors are optional for the same
 * reason in the other direction: a tenant without them still works, with the
 * conservative behaviour docs/internals/provider-cell.md describes.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const CELL_PROVIDER = ProviderDriverKind.make("cell");

/**
 * Durable Object names are capped at 64 and children append `.<id>` to the
 * parent's name, so the thread's share is a fixed 20 hex characters: 80 bits
 * of a SHA-256 over the whole thread id, never a slice of the id itself.
 */
const PROJECT_HASH_BYTES = 10;

export const CellState = Schema.Struct({
  model: Schema.String,
  running: Schema.Boolean,
  configured: Schema.Boolean,
  turns: Schema.optional(Schema.Number),
  version: Schema.optional(Schema.String),
  depth: Schema.optional(Schema.Number),
  tasks: Schema.optional(Schema.Number),
  runId: Schema.optional(Schema.NullOr(Schema.String)),
  seq: Schema.optional(Schema.Number),
});
export type CellState = typeof CellState.Type;

export const CellTask = Schema.Struct({
  taskId: Schema.String,
  title: Schema.optional(Schema.String),
  status: Schema.String,
  childProject: Schema.optional(Schema.String),
  childUrl: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.String),
  changes: Schema.optional(Schema.Array(Schema.Unknown)),
  merged: Schema.optional(Schema.Boolean),
  created: Schema.optional(Schema.Number),
  finished: Schema.optional(Schema.NullOr(Schema.Number)),
});
export type CellTask = typeof CellTask.Type;

export const CellTaskRemoved = Schema.Struct({
  taskId: Schema.String,
  removed: Schema.Literal(true),
});

export const CellDoneError = Schema.Struct({
  code: Schema.Literals([
    "cancelled",
    "usage_limit",
    "provider_error",
    "transport_error",
    "tool_error",
  ]),
  status: Schema.optional(Schema.Number),
  message: Schema.String,
  resetAt: Schema.optional(Schema.String),
});
export type CellDoneError = typeof CellDoneError.Type;

const sequenced = { seq: Schema.optional(Schema.Number) };
const runScoped = { ...sequenced, runId: Schema.optional(Schema.String) };
const prompted = { ...runScoped, origin: Schema.optional(Schema.Literals(["user", "wake"])) };

export const CellEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("state"),
    running: Schema.Boolean,
    parent: Schema.optional(Schema.Unknown),
    depth: Schema.optional(Schema.Number),
    runId: Schema.optional(Schema.NullOr(Schema.String)),
    seq: Schema.optional(Schema.Number),
  }),
  Schema.Struct({ type: Schema.Literal("tasks"), tasks: Schema.Array(CellTask), ...sequenced }),
  Schema.Struct({
    type: Schema.Literal("task"),
    task: Schema.Union([CellTaskRemoved, CellTask]),
    ...sequenced,
  }),
  Schema.Struct({ type: Schema.Literal("user"), text: Schema.String, ...prompted }),
  Schema.Struct({ type: Schema.Literal("wake"), text: Schema.String, ...prompted }),
  Schema.Struct({ type: Schema.Literal("assistant"), text: Schema.String, ...runScoped }),
  Schema.Struct({ type: Schema.Literal("shell"), command: Schema.String, ...runScoped }),
  Schema.Struct({
    type: Schema.Literal("shell_output"),
    command: Schema.String,
    stdout: Schema.String,
    stderr: Schema.String,
    exitCode: Schema.Number,
    timedOut: Schema.Boolean,
    ...runScoped,
  }),
  Schema.Struct({
    type: Schema.Literal("tool"),
    name: Schema.String,
    arguments: Schema.Unknown,
    ...runScoped,
  }),
  Schema.Struct({
    type: Schema.Literal("tool_output"),
    name: Schema.String,
    output: Schema.String,
    ...runScoped,
  }),
  Schema.Struct({
    type: Schema.Literal("patch"),
    operation: Schema.String,
    path: Schema.String,
    status: Schema.optional(Schema.String),
    text: Schema.optional(Schema.String),
    ...runScoped,
  }),
  Schema.Struct({ type: Schema.Literal("error"), text: Schema.String, ...runScoped }),
  Schema.Struct({
    type: Schema.Literal("done"),
    error: Schema.optional(Schema.NullOr(Schema.Union([Schema.String, CellDoneError]))),
    ...runScoped,
  }),
  Schema.Struct({ type: Schema.Literal("reset"), ...sequenced }),
  Schema.Struct({ type: Schema.Literal("fs"), changes: Schema.Unknown, ...sequenced }),
  Schema.Struct({ type: Schema.Literal("exec"), ...sequenced }),
]);
export type CellEvent = typeof CellEvent.Type;

const KNOWN_EVENT_TYPES: ReadonlySet<string> = new Set(
  CellEvent.members.map((member) => member.fields.type.literal),
);

export const isKnownCellEventType = (type: unknown): boolean =>
  typeof type === "string" && KNOWN_EVENT_TYPES.has(type);

export type CellRunEvent = Extract<
  CellEvent,
  {
    readonly type:
      | "user"
      | "wake"
      | "assistant"
      | "shell"
      | "shell_output"
      | "tool"
      | "tool_output"
      | "patch"
      | "error"
      | "done";
  }
>;

const RUN_EVENT_TYPES: ReadonlySet<string> = new Set<CellRunEvent["type"]>([
  "user",
  "wake",
  "assistant",
  "shell",
  "shell_output",
  "tool",
  "tool_output",
  "patch",
  "error",
  "done",
]);

/** Events that belong to one agent run, as opposed to connection snapshots and task bookkeeping. */
export const isCellRunEvent = (event: CellEvent): event is CellRunEvent =>
  RUN_EVENT_TYPES.has(event.type);

/** A `wake` event, or a `user` event the cell marked as wake-originated. */
export const isCellWakeEvent = (event: CellEvent): boolean =>
  event.type === "wake" || (event.type === "user" && event.origin === "wake");

/** Body of a 202 to `POST prompt`; a tenant without run ids answers with an empty body. */
export const CellPromptAccepted = Schema.Struct({
  runId: Schema.String,
  seq: Schema.optional(Schema.Number),
});
export type CellPromptAccepted = typeof CellPromptAccepted.Type;

/** Body of a 409 to `POST prompt` that names the run already in progress. */
export const CellPromptRejected = Schema.Struct({
  runId: Schema.optional(Schema.NullOr(Schema.String)),
});

const decodePromptAccepted = Schema.decodeUnknownOption(Schema.fromJsonString(CellPromptAccepted));
const decodePromptRejected = Schema.decodeUnknownOption(Schema.fromJsonString(CellPromptRejected));

export function readPromptAccepted(body: string): CellPromptAccepted | undefined {
  return Option.getOrUndefined(decodePromptAccepted(body));
}

export function readPromptRejectedRunId(body: string): string | undefined {
  return Option.getOrUndefined(decodePromptRejected(body))?.runId ?? undefined;
}

export type CellDoneOutcome =
  | { readonly kind: "completed" }
  | { readonly kind: "cancelled" }
  | {
      readonly kind: "failed";
      readonly failureClass: "usage_limit" | "provider_error" | "transport_error";
      readonly message: string;
      readonly code: string | null;
      readonly resetAt: string | null;
    };

const USAGE_LIMIT_TEXT = /insufficient_quota|rate[\s_-]?limit|\b429\b|quota/i;

/**
 * A structured `done.error` is classified by its code; a plain string, which
 * is all the current protocol sends, by the words providers use for quota.
 */
export function classifyCellDone(
  error: string | CellDoneError | null | undefined,
): CellDoneOutcome {
  if (error === null || error === undefined) return { kind: "completed" };
  if (typeof error === "string") {
    if (error === "Cancelled") return { kind: "cancelled" };
    return {
      kind: "failed",
      failureClass: USAGE_LIMIT_TEXT.test(error) ? "usage_limit" : "provider_error",
      message: error,
      code: null,
      resetAt: null,
    };
  }
  if (error.code === "cancelled") return { kind: "cancelled" };
  const message =
    error.status === undefined ? error.message : `${error.message} (HTTP ${error.status})`;
  return {
    kind: "failed",
    failureClass:
      error.code === "usage_limit"
        ? "usage_limit"
        : error.code === "transport_error"
          ? "transport_error"
          : "provider_error",
    message,
    code: error.code,
    resetAt: error.resetAt ?? null,
  };
}

/**
 * The project name is derived from the whole thread id, so a resumed thread
 * finds its cell again without a stored mapping, and two threads cannot share
 * a cell through a common suffix. The name is opaque on the tenant; the T3
 * thread title is its display name.
 */
export const cellProjectName = (prefix: string, threadId: string): Effect.Effect<string> =>
  Effect.promise(() =>
    globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(threadId)),
  ).pipe(
    Effect.map((digest) => {
      const hex = [...new Uint8Array(digest).slice(0, PROJECT_HASH_BYTES)]
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      return `${prefix}${hex}`;
    }),
  );

export function cellProbeProjectName(prefix: string): string {
  return `${prefix}probe`;
}

export function cellApiUrl(baseUrl: string, project: string, path: string): string {
  return `${baseUrl.replace(/\/+$/, "")}/cell/${encodeURIComponent(project)}${path}`;
}

export function cellSocketUrl(baseUrl: string, project: string, client: string): string {
  const url = new URL(cellApiUrl(baseUrl, project, "/ws"));
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("client", client);
  return url.toString();
}

export function cellAuthorizationHeaders(token: string): Readonly<Record<string, string>> {
  return token ? { authorization: `Bearer ${token}` } : {};
}
