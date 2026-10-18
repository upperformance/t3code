import { assert, describe, it } from "@effect/vitest";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import type { ProviderReplayEntry } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { makeHttpCellTransport, type CellTransport } from "../server/transport.ts";
import { startFakeCellServer, type FakeCellServer } from "./fakeCellServer.ts";
import { makeRecordingCellTransport, makeReplayCellTransport } from "./replayTransport.ts";

const liveLayer = Layer.mergeAll(
  NodeHttpClient.layerUndici,
  NodeSocket.layerWebSocketConstructorWS,
);
const PROJECT = "t3-recorder";
const PROMPTS = ["Respond with exactly: one", "Respond with exactly: two"] as const;

const fakeCell = Effect.acquireRelease(
  Effect.promise(() => startFakeCellServer()),
  (fake: FakeCellServer) => Effect.promise(() => fake.close()),
);

const decodePage = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      events: Schema.Array(
        Schema.Struct({ type: Schema.String, requestId: Schema.optional(Schema.String) }),
      ),
    }),
  ),
);

/** The request ids the openers of a log page carry, in order. */
const openerRequestIds = (body: string) =>
  decodePage(body).events.flatMap((event) =>
    event.type === "user" ? [event.requestId ?? null] : [],
  );

/**
 * Two prompts posted with their own request ids, each left to finish, then
 * the whole log: one page carrying both openers. `settle` waits for the cell
 * between the prompts; a replay has no cell to wait for.
 */
const twoTurns = Effect.fnUntraced(function* (
  transport: CellTransport,
  baseUrl: string,
  requestIds: readonly [string, string],
  settle: () => Promise<void>,
) {
  const url = (path: string) => `${baseUrl}/cell/${PROJECT}${path}`;
  for (const [index, requestId] of requestIds.entries()) {
    const accepted = yield* transport.request({
      method: "POST",
      url: url("/prompt"),
      headers: { "x-request-id": requestId },
      body: PROMPTS[index]!,
    });
    assert.equal(accepted.status, 202);
    yield* Effect.promise(settle);
  }
  return yield* transport.request({
    method: "GET",
    url: url("/events?after=0&limit=1000"),
    headers: {},
  });
});

describe("cell replay recorder", () => {
  it.live("numbers each turn's request id, so a page carrying both openers replays both", () =>
    Effect.gen(function* () {
      const fake = yield* fakeCell;
      const live = yield* makeHttpCellTransport;
      const entries: Array<ProviderReplayEntry> = [];
      const recorded = yield* twoTurns(
        makeRecordingCellTransport(live, entries),
        fake.baseUrl,
        ["11111111-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "22222222-bbbb-4bbb-8bbb-bbbbbbbbbbbb"],
        () => fake.onceIdle(PROJECT),
      );
      assert.deepEqual(openerRequestIds(recorded.body), [
        "11111111-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "22222222-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      ]);
      const page = entries.at(-1);
      const frame = (page?.type === "emit_inbound" ? page.frame : undefined) as
        | { readonly body?: string }
        | undefined;
      assert.deepEqual(openerRequestIds(frame?.body ?? ""), ["<request-id:1>", "<request-id:2>"]);
      assert.notInclude(JSON.stringify(entries), "11111111-aaaa");
      assert.notInclude(JSON.stringify(entries), "22222222-bbbb");

      const controller = makeReplayCellTransport({ scenario: "two-turns", entries });
      const replayed = yield* twoTurns(
        controller.transport,
        "https://cell.replay.test",
        ["33333333-cccc-4ccc-8ccc-cccccccccccc", "44444444-dddd-4ddd-8ddd-dddddddddddd"],
        () => Promise.resolve(),
      );
      assert.deepEqual(openerRequestIds(replayed.body), [
        "33333333-cccc-4ccc-8ccc-cccccccccccc",
        "44444444-dddd-4ddd-8ddd-dddddddddddd",
      ]);
      controller.assertComplete();
    }).pipe(Effect.scoped, Effect.provide(liveLayer)),
  );
});
