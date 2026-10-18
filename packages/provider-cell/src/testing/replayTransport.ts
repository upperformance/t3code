/**
 * The replay boundary for cell transcripts (`cell.http-ws`): outbound frames
 * are the HTTP requests and socket connects the adapter makes, inbound frames
 * are their responses and the events the socket delivered. The controller
 * walks the transcript in order on one promise chain: an outbound entry waits
 * for the adapter to make that request, inbound socket frames are delivered
 * as the walk reaches them, and a labelled frame can be held by a replay gate
 * until a scenario step releases it.
 */
import type { ProviderReplayEntry } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import {
  CellTransportError,
  type CellHttpRequest,
  type CellHttpResponse,
  type CellSocketConnection,
  type CellTransport,
} from "../server/transport.ts";

export const CELL_REPLAY_PROTOCOL = "cell.http-ws";
const PROJECT_PLACEHOLDER = "<project>";

export const CellReplayFrame = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("http"),
    method: Schema.String,
    path: Schema.String,
    body: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("http_response"),
    status: Schema.Number,
    body: Schema.String,
  }),
  /** The request never got an answer: the recorder saw a transport failure. */
  Schema.Struct({ kind: Schema.Literal("http_failure"), detail: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("ws_connect"), path: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("ws_event"), event: Schema.Unknown }),
  Schema.Struct({ kind: Schema.Literal("ws_closed"), code: Schema.optional(Schema.Number) }),
]);
export type CellReplayFrame = typeof CellReplayFrame.Type;

const decodeFrame = Schema.decodeUnknownSync(CellReplayFrame);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export class CellReplayMismatchError extends Schema.TaggedError<CellReplayMismatchError>()(
  "CellReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    expected: Schema.Unknown,
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `Cell replay frame mismatch at cursor ${this.cursor} in scenario ${this.scenario}: expected ${JSON.stringify(this.expected)}, got ${JSON.stringify(this.actual)}.`;
  }
}

export class CellReplayIncompleteError extends Schema.TaggedError<CellReplayIncompleteError>()(
  "CellReplayIncompleteError",
  { scenario: Schema.String, cursor: Schema.Number, remaining: Schema.Number },
) {
  override get message(): string {
    return `Cell replay ended with ${this.remaining} unconsumed entries at cursor ${this.cursor} in scenario ${this.scenario}.`;
  }
}

/**
 * The project segment of a cell path. Recordings keep `<project>` there, since
 * the hash differs per run; replay binds it to the first project it sees so a
 * request to a second project is caught instead of matched.
 */
function projectSegment(path: string): string | undefined {
  return /^\/cell\/([^/?]+)/.exec(path)?.[1];
}

function withProject(path: string, project: string): string {
  return path.replace(/^(\/cell\/)[^/?]+/, `$1${project}`);
}

/** Drops the socket's `client` query, which carries the session id that minted the recording. */
function anonymizeCellPath(path: string): string {
  const [pathname, search = ""] = path.split("?", 2) as [string, string?];
  const params = new URLSearchParams(search);
  params.delete("client");
  const query = params.toString();
  return `${withProject(pathname, PROJECT_PLACEHOLDER)}${query ? `?${query}` : ""}`;
}

function requestPath(url: string): string {
  const parsed = new URL(url);
  return `${parsed.pathname}${parsed.search}`;
}

export interface CellReplayGate {
  readonly beforeEmit: (label: string | undefined, signal?: AbortSignal) => Promise<void>;
}

interface Listener {
  readonly deliver: (frames: ReadonlyArray<string>) => void;
  readonly close: (code: number | undefined) => void;
}

type PendingResponse =
  | { readonly kind: "response"; readonly response: CellHttpResponse }
  | { readonly kind: "failure"; readonly error: CellTransportError };

export class CellReplayController {
  private cursor = 0;
  private failure: CellReplayMismatchError | undefined;
  private listener: Listener | undefined;
  private boundProject: string | undefined;
  private chain: Promise<void> = Promise.resolve();
  private readonly abort = new AbortController();
  private readonly entries: ReadonlyArray<ProviderReplayEntry>;
  private readonly scenario: string;
  private readonly gate: CellReplayGate | undefined;

  constructor(input: {
    readonly scenario: string;
    readonly entries: ReadonlyArray<ProviderReplayEntry>;
    readonly gate?: CellReplayGate;
  }) {
    this.scenario = input.scenario;
    this.entries = input.entries;
    this.gate = input.gate;
  }

  get transport(): CellTransport {
    return {
      request: (request) => this.request(request),
      connect: (input) => this.connect(input.url),
    };
  }

  assertComplete(): void {
    this.abort.abort();
    if (this.failure) throw this.failure;
    if (this.cursor !== this.entries.length) {
      throw new CellReplayIncompleteError({
        scenario: this.scenario,
        cursor: this.cursor,
        remaining: this.entries.length - this.cursor,
      });
    }
  }

  /** Every cursor movement runs on one chain, so deliveries and matches keep transcript order. */
  private enqueue<A>(task: () => Promise<A>): Promise<A> {
    const result = this.chain.then(task);
    this.chain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private request(request: CellHttpRequest): Effect.Effect<CellHttpResponse, CellTransportError> {
    const actual = {
      kind: "http" as const,
      method: request.method,
      path: anonymizeCellPath(requestPath(request.url)),
      ...(request.body === undefined ? {} : { body: request.body }),
    };
    const operation = `${request.method} ${request.url}`;
    const project = projectSegment(requestPath(request.url));
    return Effect.promise(() =>
      this.enqueue(async (): Promise<PendingResponse> => {
        await this.deliverInbound();
        if (!this.takeOutbound(actual, project)) {
          return { kind: "failure", error: this.fail(operation) };
        }
        // The cell broadcasts a prompt's first events before it answers 202, so
        // the socket frames recorded ahead of the response are delivered first.
        const response = await this.deliverInbound({ untilResponse: true });
        if (response === undefined) {
          this.failure ??= new CellReplayMismatchError({
            scenario: this.scenario,
            cursor: this.cursor,
            expected: "http_response",
            actual: this.entries[this.cursor] ?? null,
          });
          return { kind: "failure", error: this.fail(operation) };
        }
        if (response.kind === "http_failure") {
          return {
            kind: "failure",
            error: new CellTransportError({ operation, detail: response.detail }),
          };
        }
        return {
          kind: "response",
          response: { status: response.status, body: response.body },
        };
      }),
    ).pipe(
      Effect.flatMap((outcome) =>
        outcome.kind === "response" ? Effect.succeed(outcome.response) : Effect.fail(outcome.error),
      ),
      Effect.tap(() => Effect.sync(() => this.pump())),
    );
  }

  private connect(
    url: string,
  ): Effect.Effect<CellSocketConnection, CellTransportError, Scope.Scope> {
    const actual = { kind: "ws_connect" as const, path: anonymizeCellPath(requestPath(url)) };
    const project = projectSegment(requestPath(url));
    const attach = (listener: Listener) => {
      this.listener = listener;
    };
    const detach = (listener: Listener) => {
      if (this.listener === listener) this.listener = undefined;
    };
    const match = () =>
      this.enqueue(async () => {
        await this.deliverInbound();
        return this.takeOutbound(actual, project);
      });
    const fail = () => this.fail(`connect ${url}`);
    const pump = () => this.pump();
    return Effect.gen(function* () {
      const matched = yield* Effect.promise(match);
      if (!matched) return yield* fail();
      const pending: Array<string> = [];
      let closedWith: CellTransportError | undefined;
      let waiter: Deferred.Deferred<void> | undefined;
      const wakeWaiter = () => {
        if (waiter) Deferred.doneUnsafe(waiter, Effect.void);
      };
      const listener: Listener = {
        deliver: (frames) => {
          pending.push(...frames);
          wakeWaiter();
        },
        close: (code) => {
          closedWith = new CellTransportError({
            operation: `read ${url}`,
            detail: `socket closed${code === undefined ? "" : ` (${code})`}`,
            closed: true,
          });
          wakeWaiter();
        },
      };
      attach(listener);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          detach(listener);
        }),
      );
      const pull: CellSocketConnection["pull"] = Effect.gen(function* () {
        while (true) {
          if (pending.length > 0) return pending.splice(0);
          if (closedWith) return yield* closedWith;
          const next = yield* Deferred.make<void>();
          waiter = next;
          yield* Deferred.await(next);
          waiter = undefined;
        }
      });
      pump();
      return { pull };
    });
  }

  private takeOutbound(actual: CellReplayFrame, project: string | undefined): boolean {
    if (this.failure) return false;
    const entry = this.entries[this.cursor];
    const expected = entry?.type === "expect_outbound" ? decodeFrame(entry.frame) : undefined;
    const boundPath = (path: string) =>
      this.boundProject === undefined ? path : withProject(path, this.boundProject);
    const actualPath = actual.kind === "http" || actual.kind === "ws_connect" ? actual.path : "";
    const matches =
      expected !== undefined &&
      expected.kind === actual.kind &&
      (actual.kind === "http"
        ? expected.kind === "http" &&
          expected.method === actual.method &&
          boundPath(expected.path) === boundPath(actualPath) &&
          (expected.body ?? "") === (actual.body ?? "")
        : actual.kind === "ws_connect" &&
          expected.kind === "ws_connect" &&
          boundPath(expected.path) === boundPath(actualPath)) &&
      (this.boundProject === undefined || project === undefined || project === this.boundProject);
    if (!matches) {
      this.failure = new CellReplayMismatchError({
        scenario: this.scenario,
        cursor: this.cursor,
        expected: entry ?? null,
        actual: {
          ...actual,
          ...(project === undefined ? {} : { project }),
          ...(this.boundProject === undefined ? {} : { boundProject: this.boundProject }),
        },
      });
      return false;
    }
    if (this.boundProject === undefined && project !== undefined) this.boundProject = project;
    this.cursor += 1;
    return true;
  }

  /** Walks inbound entries from the cursor without waiting for the chain; callers hold it. */
  private async deliverInbound(options: { readonly untilResponse?: boolean } = {}) {
    while (this.failure === undefined) {
      const entry = this.entries[this.cursor];
      if (entry === undefined) return undefined;
      if (entry.type === "runtime_exit") {
        this.cursor += 1;
        this.listener?.close(undefined);
        continue;
      }
      if (entry.type !== "emit_inbound") return undefined;
      const frame = decodeFrame(entry.frame);
      if (frame.kind === "http_response" || frame.kind === "http_failure") {
        if (options.untilResponse !== true) return undefined;
        this.cursor += 1;
        return frame;
      }
      if (this.gate !== undefined) await this.gate.beforeEmit(entry.label, this.abort.signal);
      if (this.abort.signal.aborted) return undefined;
      this.cursor += 1;
      if (frame.kind === "ws_event") this.listener?.deliver([encodeJson(frame.event)]);
      else if (frame.kind === "ws_closed") this.listener?.close(frame.code);
    }
    return undefined;
  }

  /** Socket frames are delivered as soon as the transcript reaches them. */
  private pump(): void {
    void this.enqueue(() => this.deliverInbound());
  }

  private fail(operation: string): CellTransportError {
    return new CellTransportError({
      operation,
      detail: this.failure?.message ?? "replay transcript exhausted",
      cause: this.failure,
    });
  }
}

export function makeReplayCellTransport(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
  readonly gate?: CellReplayGate;
}): CellReplayController {
  return new CellReplayController(input);
}

/** `ws:task#2` names the second `task` event of a recording, for replay gates to hold. */
export function cellReplayEventLabel(type: string, ordinal: number): string {
  return `ws:${type}#${ordinal}`;
}

/**
 * Wraps a live transport and appends every exchange to `entries` in transcript
 * order. With a gate, a labelled socket frame is held until the scenario
 * releases it, so a recording is paced the way its replay will be.
 */
export function makeRecordingCellTransport(
  transport: CellTransport,
  entries: Array<ProviderReplayEntry>,
  options: { readonly gate?: CellReplayGate } = {},
): CellTransport {
  const eventCounts = new Map<string, number>();
  const record = (type: "expect_outbound" | "emit_inbound", frame: CellReplayFrame): string => {
    let label: string;
    if (frame.kind === "http") {
      label = `${frame.method} ${frame.path}`;
    } else if (frame.kind === "ws_event") {
      const eventType = String((frame.event as { type?: unknown } | null)?.type ?? "event");
      const ordinal = (eventCounts.get(eventType) ?? 0) + 1;
      eventCounts.set(eventType, ordinal);
      label = cellReplayEventLabel(eventType, ordinal);
    } else {
      label = frame.kind;
    }
    entries.push({ type, label, frame });
    return label;
  };
  return {
    request: (request) =>
      Effect.gen(function* () {
        record("expect_outbound", {
          kind: "http",
          method: request.method,
          path: anonymizeCellPath(requestPath(request.url)),
          ...(request.body === undefined ? {} : { body: request.body }),
        });
        const response = yield* transport
          .request(request)
          .pipe(
            Effect.tapError((error) =>
              Effect.sync(() =>
                record("emit_inbound", { kind: "http_failure", detail: error.detail }),
              ),
            ),
          );
        record("emit_inbound", {
          kind: "http_response",
          status: response.status,
          body: response.body,
        });
        return response;
      }),
    connect: (input) =>
      Effect.gen(function* () {
        record("expect_outbound", {
          kind: "ws_connect",
          path: anonymizeCellPath(requestPath(input.url)),
        });
        const connection = yield* transport.connect(input);
        // One frame per pull, so a gated frame holds the ones behind it too.
        const buffered: Array<string> = [];
        const pull: CellSocketConnection["pull"] = Effect.gen(function* () {
          if (buffered.length === 0) {
            const frames = yield* connection.pull.pipe(
              Effect.tapError((error) =>
                Effect.sync(() => {
                  if (error.closed) record("emit_inbound", { kind: "ws_closed" });
                }),
              ),
            );
            buffered.push(...frames);
            if (buffered.length === 0) return [];
          }
          const frame = buffered.shift()!;
          const event = yield* decodeEventFrame(frame).pipe(
            Effect.orElseSucceed((): unknown => frame),
          );
          const label = record("emit_inbound", { kind: "ws_event", event });
          if (options.gate !== undefined) {
            const gate = options.gate;
            yield* Effect.promise((signal) => gate.beforeEmit(label, signal));
          }
          return [frame];
        });
        return { pull };
      }),
  };
}

const decodeEventFrame = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
