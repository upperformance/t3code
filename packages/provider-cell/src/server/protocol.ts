/**
 * The cell's wire shapes at cell protocol level 3: `GET state`, the WebSocket
 * events, the prompt responses, the event-log page, and the project naming
 * the adapter and the status probe share. Every event carries the cell's
 * sequence number, every run-scoped event names its run, and a tenant that
 * states a lower level, or none, is refused before anything is sent to it.
 * Events the cell adds after this build are ignored rather than failing the
 * socket, so this T3 keeps working against a newer tenant.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const CELL_PROVIDER = ProviderDriverKind.make("cell");

/** The cell protocol this adapter speaks; `GET state` must report it or a later one. */
const CELL_PROTOCOL_LEVEL = 3;

/**
 * Durable Object names are capped at 64 and children append `.<id>` to the
 * parent's name, so the thread's share is a fixed 20 hex characters: 80 bits
 * of a SHA-256 over the whole thread id, never a slice of the id itself.
 */
const PROJECT_HASH_BYTES = 10;

/** The tenant speaks an older cell protocol; the message says which and what this provider needs. */
export class CellProtocolError extends Schema.TaggedError<CellProtocolError>()(
  "CellProtocolError",
  { level: Schema.NullOr(Schema.Number) },
) {
  override get message(): string {
    const spoken =
      this.level === null
        ? `a cell protocol older than ${CELL_PROTOCOL_LEVEL}, stating no level`
        : `cell protocol ${this.level}`;
    return `This cell tenant speaks ${spoken}; this provider needs ${CELL_PROTOCOL_LEVEL}. Update celld on the tenant.`;
  }
}

export const CellState = Schema.Struct({
  protocol: Schema.Number,
  model: Schema.String,
  running: Schema.Boolean,
  runId: Schema.NullOr(Schema.String),
  /** The last sequence number the cell emitted. */
  seq: Schema.Number,
  configured: Schema.Boolean,
  version: Schema.optional(Schema.String),
  turns: Schema.optional(Schema.Number),
  depth: Schema.optional(Schema.Number),
  tasks: Schema.optional(Schema.Number),
});
export type CellState = typeof CellState.Type;

const decodeProtocolLevel = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ protocol: Schema.optional(Schema.Number) })),
);
const decodeState = Schema.decodeUnknownEffect(Schema.fromJsonString(CellState));

/**
 * Reads a `GET state` body. The level is read first, so a tenant below level
 * 3 is refused with the level it states rather than with a decoding error
 * about the fields it lacks; a body that is not a JSON object is not a cell
 * state at all.
 */
export const readCellState = Effect.fnUntraced(function* (body: string) {
  const { protocol } = yield* decodeProtocolLevel(body);
  if (protocol === undefined || protocol < CELL_PROTOCOL_LEVEL) {
    return yield* new CellProtocolError({ level: protocol ?? null });
  }
  return yield* decodeState(body);
});

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

/** Every event is numbered; run-scoped ones name their run; `user` and `wake` say who started it. */
const sequenced = { seq: Schema.Number };
const runScoped = { ...sequenced, runId: Schema.String };
const prompted = { ...runScoped, origin: Schema.Literals(["user", "wake"]) };

export const CellEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("state"),
    running: Schema.Boolean,
    runId: Schema.NullOr(Schema.String),
    ...sequenced,
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
    error: Schema.optional(Schema.NullOr(CellDoneError)),
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

/** Body of a 202 to `POST prompt`: the run's id and the number of its opening `user` event. */
export const CellPromptAccepted = Schema.Struct({
  runId: Schema.String,
  seq: Schema.Number,
});
export type CellPromptAccepted = typeof CellPromptAccepted.Type;

/** Body of a 409 to `POST prompt` that names the run already in progress. */
export const CellPromptRejected = Schema.Struct({
  runId: Schema.optional(Schema.NullOr(Schema.String)),
});

/**
 * A page of `GET events?after=<seq>&limit=<n>`, ordered by number. `complete`
 * says the log has nothing after `nextAfter`; `truncated` says the cell no
 * longer retains events as far back as `after`, and `earliest` is the oldest
 * it keeps.
 */
export const CellEventsPage = Schema.Struct({
  events: Schema.Array(Schema.Unknown),
  nextAfter: Schema.Number,
  complete: Schema.Boolean,
  truncated: Schema.optional(Schema.Boolean),
  earliest: Schema.optional(Schema.Number),
});
export type CellEventsPage = typeof CellEventsPage.Type;

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
      readonly code: string;
      readonly resetAt: string | null;
    };

/** A `done` without an error completed; with one, the error's code names the failure class. */
export function classifyCellDone(error: CellDoneError | null | undefined): CellDoneOutcome {
  if (error === null || error === undefined) return { kind: "completed" };
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
