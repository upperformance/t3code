/**
 * The replay boundary for cell transcripts (`cell.http-ws`): outbound frames
 * are the HTTP requests and socket connects the adapter makes, inbound frames
 * are their responses and the events the socket delivered. The controller
 * holds the cursor; the transport it hands out answers from the transcript
 * and fails on the first request the recording did not see.
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

/** `/cell/<project>/prompt` in a recording matches any project segment; the ids differ per run. */
function pathMatches(expected: string, actual: string): boolean {
  if (!expected.includes(PROJECT_PLACEHOLDER)) return expected === actual;
  const [prefix, suffix] = expected.split(PROJECT_PLACEHOLDER);
  if (prefix === undefined || suffix === undefined) return expected === actual;
  return (
    actual.startsWith(prefix) &&
    actual.endsWith(suffix) &&
    actual.length > prefix.length + suffix.length &&
    !actual.slice(prefix.length, actual.length - suffix.length).includes("/")
  );
}

/**
 * Replaces the project segment, and drops the socket's `client` query, so a
 * recording depends on neither the thread id nor the session id that minted it.
 */
function anonymizeCellPath(path: string): string {
  return path.replace(/\?.*$/, "").replace(/^(\/cell\/)[^/]+/, `$1${PROJECT_PLACEHOLDER}`);
}

function requestPath(url: string): string {
  const parsed = new URL(url);
  return `${parsed.pathname}${parsed.search}`;
}

interface Listener {
  readonly deliver: (frames: ReadonlyArray<string>) => void;
  readonly close: (code: number | undefined) => void;
}

export class CellReplayController {
  private cursor = 0;
  private failure: CellReplayMismatchError | undefined;
  private listener: Listener | undefined;
  private readonly entries: ReadonlyArray<ProviderReplayEntry>;
  private readonly scenario: string;

  constructor(input: {
    readonly scenario: string;
    readonly entries: ReadonlyArray<ProviderReplayEntry>;
  }) {
    this.scenario = input.scenario;
    this.entries = input.entries;
  }

  get transport(): CellTransport {
    return {
      request: (request) => Effect.suspend(() => this.request(request)),
      connect: (input) => Effect.suspend(() => this.connect(input.url)),
    };
  }

  assertComplete(): void {
    if (this.failure) throw this.failure;
    if (this.cursor !== this.entries.length) {
      throw new CellReplayIncompleteError({
        scenario: this.scenario,
        cursor: this.cursor,
        remaining: this.entries.length - this.cursor,
      });
    }
  }

  private request(request: CellHttpRequest): Effect.Effect<CellHttpResponse, CellTransportError> {
    const actual = {
      kind: "http" as const,
      method: request.method,
      path: anonymizeCellPath(requestPath(request.url)),
      ...(request.body === undefined ? {} : { body: request.body }),
    };
    const expected = this.takeOutbound(actual);
    if (expected === undefined) return this.fail(`${request.method} ${request.url}`);
    // The cell broadcasts a prompt's first events before it answers 202, so the
    // socket frames recorded ahead of the response are delivered first, as they were.
    this.drainSocketFrames();
    const response = this.takeInbound();
    if (response?.kind !== "http_response") {
      this.failure ??= new CellReplayMismatchError({
        scenario: this.scenario,
        cursor: this.cursor,
        expected: "http_response",
        actual: response ?? null,
      });
      return this.fail(`${request.method} ${request.url}`);
    }
    const result = Effect.succeed({ status: response.status, body: response.body });
    this.drainSocketFrames();
    return result;
  }

  private connect(
    url: string,
  ): Effect.Effect<CellSocketConnection, CellTransportError, Scope.Scope> {
    const actual = { kind: "ws_connect" as const, path: anonymizeCellPath(requestPath(url)) };
    if (this.takeOutbound(actual) === undefined) return this.fail(`connect ${url}`);
    const attach = (listener: Listener) => {
      this.listener = listener;
    };
    const detach = (listener: Listener) => {
      if (this.listener === listener) this.listener = undefined;
    };
    const drain = () => this.drainSocketFrames();
    return Effect.gen(function* () {
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
      drain();
      return { pull };
    });
  }

  private takeOutbound(actual: CellReplayFrame): CellReplayFrame | undefined {
    if (this.failure) return undefined;
    const entry = this.entries[this.cursor];
    const expected = entry?.type === "expect_outbound" ? decodeFrame(entry.frame) : undefined;
    const matches =
      expected !== undefined &&
      expected.kind === actual.kind &&
      (actual.kind === "http"
        ? expected.kind === "http" &&
          expected.method === actual.method &&
          pathMatches(expected.path, actual.path) &&
          (expected.body ?? "") === (actual.body ?? "")
        : actual.kind === "ws_connect" &&
          expected.kind === "ws_connect" &&
          pathMatches(expected.path, actual.path));
    if (!matches) {
      this.failure = new CellReplayMismatchError({
        scenario: this.scenario,
        cursor: this.cursor,
        expected: entry ?? null,
        actual,
      });
      return undefined;
    }
    this.cursor += 1;
    return expected;
  }

  private takeInbound(): CellReplayFrame | undefined {
    const entry = this.entries[this.cursor];
    if (entry?.type !== "emit_inbound") return undefined;
    this.cursor += 1;
    return decodeFrame(entry.frame);
  }

  /** Socket frames are delivered as soon as the transcript reaches them. */
  private drainSocketFrames(): void {
    while (this.failure === undefined) {
      const entry = this.entries[this.cursor];
      if (entry?.type === "runtime_exit") {
        this.cursor += 1;
        this.listener?.close(undefined);
        continue;
      }
      if (entry?.type !== "emit_inbound") return;
      const frame = decodeFrame(entry.frame);
      if (frame.kind === "http_response") return;
      this.cursor += 1;
      if (frame.kind === "ws_event") this.listener?.deliver([encodeJson(frame.event)]);
      else if (frame.kind === "ws_closed") this.listener?.close(frame.code);
    }
  }

  private fail(operation: string): Effect.Effect<never, CellTransportError> {
    return Effect.fail(
      new CellTransportError({
        operation,
        detail: this.failure?.message ?? "replay transcript exhausted",
        cause: this.failure,
      }),
    );
  }
}

export function makeReplayCellTransport(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
}): CellReplayController {
  return new CellReplayController(input);
}

/** Wraps a live transport and appends every exchange to `entries` in transcript order. */
export function makeRecordingCellTransport(
  transport: CellTransport,
  entries: Array<ProviderReplayEntry>,
): CellTransport {
  const record = (type: "expect_outbound" | "emit_inbound", frame: CellReplayFrame) => {
    const label =
      frame.kind === "http"
        ? `${frame.method} ${frame.path}`
        : frame.kind === "ws_event"
          ? `ws:${String((frame.event as { type?: unknown } | null)?.type ?? "event")}`
          : frame.kind;
    entries.push({ type, label, frame });
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
        const response = yield* transport.request(request);
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
        return {
          pull: connection.pull.pipe(
            Effect.tap((frames) =>
              Effect.forEach(frames, (frame) =>
                decodeEventFrame(frame).pipe(
                  Effect.orElseSucceed((): unknown => frame),
                  Effect.map((event) => record("emit_inbound", { kind: "ws_event", event })),
                ),
              ),
            ),
            Effect.tapError((error) =>
              Effect.sync(() => {
                if (error.closed) record("emit_inbound", { kind: "ws_closed" });
              }),
            ),
          ),
        };
      }),
  };
}

const decodeEventFrame = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
