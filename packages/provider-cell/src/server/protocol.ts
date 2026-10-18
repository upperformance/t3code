/**
 * The cell's wire shapes: `GET state`, the WebSocket events, and the project
 * naming the adapter and the status probe share. Events the cell added after
 * this build are ignored rather than failing the socket, so an older T3 keeps
 * working against a newer tenant.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const CELL_PROVIDER = ProviderDriverKind.make("cell");

/** Durable Object names are capped at 64; children append `.<id>` to the parent's name. */
const PROJECT_NAME_BUDGET = 48;
const PROJECT_NAME_MIN_TAIL = 8;

export const CellState = Schema.Struct({
  model: Schema.String,
  running: Schema.Boolean,
  configured: Schema.Boolean,
  turns: Schema.optional(Schema.Number),
  version: Schema.optional(Schema.String),
  depth: Schema.optional(Schema.Number),
  tasks: Schema.optional(Schema.Number),
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

export const CellEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("state"),
    running: Schema.Boolean,
    parent: Schema.optional(Schema.Unknown),
    depth: Schema.optional(Schema.Number),
  }),
  Schema.Struct({ type: Schema.Literal("tasks"), tasks: Schema.Array(CellTask) }),
  Schema.Struct({ type: Schema.Literal("task"), task: Schema.Union([CellTaskRemoved, CellTask]) }),
  Schema.Struct({ type: Schema.Literal("user"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("wake"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("assistant"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("shell"), command: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("shell_output"),
    command: Schema.String,
    stdout: Schema.String,
    stderr: Schema.String,
    exitCode: Schema.Number,
    timedOut: Schema.Boolean,
  }),
  Schema.Struct({ type: Schema.Literal("tool"), name: Schema.String, arguments: Schema.Unknown }),
  Schema.Struct({
    type: Schema.Literal("tool_output"),
    name: Schema.String,
    output: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("patch"),
    operation: Schema.String,
    path: Schema.String,
    status: Schema.optional(Schema.String),
    text: Schema.optional(Schema.String),
  }),
  Schema.Struct({ type: Schema.Literal("error"), text: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("done"),
    error: Schema.optional(Schema.NullOr(Schema.String)),
  }),
  Schema.Struct({ type: Schema.Literal("reset") }),
  Schema.Struct({ type: Schema.Literal("fs"), changes: Schema.Unknown }),
  Schema.Struct({ type: Schema.Literal("exec") }),
]);
export type CellEvent = typeof CellEvent.Type;

const KNOWN_EVENT_TYPES: ReadonlySet<string> = new Set(
  CellEvent.members.map((member) => member.fields.type.literal),
);

export const isKnownCellEventType = (type: unknown): boolean =>
  typeof type === "string" && KNOWN_EVENT_TYPES.has(type);

/**
 * The project name is derived from the thread id, so a resumed thread finds
 * its cell again without a stored mapping. Only the tail is kept, since the id
 * ends in the part that varies.
 */
export function cellProjectName(prefix: string, threadId: string): string {
  const sanitized = threadId
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const budget = Math.max(PROJECT_NAME_MIN_TAIL, PROJECT_NAME_BUDGET - prefix.length);
  const tail = sanitized.length > budget ? sanitized.slice(-budget).replace(/^-+/, "") : sanitized;
  return `${prefix}${tail || "thread"}`;
}

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
