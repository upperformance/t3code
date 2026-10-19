/**
 * The network boundary of the adapter: plain HTTP requests and a WebSocket
 * that yields text frames. Replay fixtures and the recorder substitute this
 * interface, so everything above it runs unchanged in tests.
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Socket from "effect/socket/Socket";

export type CellHttpMethod = "GET" | "POST" | "PUT" | "DELETE";

export interface CellHttpRequest {
  readonly method: CellHttpMethod;
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body?: string;
}

export interface CellHttpResponse {
  readonly status: number;
  readonly body: string;
}

export interface CellSocketConnection {
  /** Fails with `closed: true` once the cell or the network ended the socket. */
  readonly pull: Effect.Effect<ReadonlyArray<string>, CellTransportError>;
}

export class CellTransportError extends Schema.TaggedError<CellTransportError>()(
  "CellTransportError",
  {
    operation: Schema.String,
    detail: Schema.String,
    closed: Schema.optional(Schema.Boolean),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Cell ${this.operation} failed: ${this.detail}`;
  }
}

export interface CellTransport {
  readonly request: (
    request: CellHttpRequest,
  ) => Effect.Effect<CellHttpResponse, CellTransportError>;
  readonly connect: (input: {
    readonly url: string;
    readonly headers: Readonly<Record<string, string>>;
  }) => Effect.Effect<CellSocketConnection, CellTransportError, Scope.Scope>;
}

const SOCKET_OPEN_TIMEOUT = "15 seconds";

function errorDetail(cause: unknown): string {
  return cause instanceof Error && cause.message ? cause.message : String(cause);
}

const decoder = new TextDecoder();

export const makeHttpCellTransport: Effect.Effect<
  CellTransport,
  never,
  HttpClient.HttpClient | Socket.WebSocketConstructor
> = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  const webSocketConstructor = yield* Socket.WebSocketConstructor;
  return {
    request: (input) =>
      Effect.gen(function* () {
        const request = HttpClientRequest.make(input.method)(input.url, {
          headers: input.headers,
          ...(input.body === undefined
            ? {}
            : { body: HttpBody.text(input.body, "text/plain; charset=utf-8") }),
        });
        const response = yield* httpClient.execute(request);
        return { status: response.status, body: yield* response.text };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new CellTransportError({
              operation: `${input.method} ${input.url}`,
              detail: errorDetail(cause),
              cause,
            }),
        ),
      ),
    connect: (input) =>
      Effect.gen(function* () {
        const socket = yield* Socket.makeWebSocket(input.url, {
          headers: input.headers,
          openTimeout: SOCKET_OPEN_TIMEOUT,
        }).pipe(Effect.provideService(Socket.WebSocketConstructor, webSocketConstructor));
        const reader = yield* socket.reader.pipe(
          Effect.mapError(
            (cause) =>
              new CellTransportError({
                operation: `connect ${input.url}`,
                detail: errorDetail(cause),
                cause,
              }),
          ),
        );
        return {
          pull: reader.pull.pipe(
            Effect.map((frames) =>
              frames.map((frame) => (typeof frame === "string" ? frame : decoder.decode(frame))),
            ),
            Effect.mapError(
              (cause) =>
                new CellTransportError({
                  operation: `read ${input.url}`,
                  detail: errorDetail(cause),
                  closed: true,
                  cause,
                }),
            ),
          ),
        };
      }),
  };
});
