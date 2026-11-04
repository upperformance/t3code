// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalConsole:off globalConsoleInEffect:off preferSchemaOverJson:off - a live probe driver that prints what it sees.
/**
 * Runs the cell provider's status probe against a live tenant (with --token, and without one) and
 * stand-ins for old protocols, a non-cell server, a closed port, a 401 and a failing-closed 503.
 *   node scripts/live-cell-probe.ts --url http://127.0.0.1:9884 [--token t]
 */
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeWS from "@effect/platform-node/NodeWS";
import { checkCellProviderStatus, makeHttpCellTransport } from "@t3tools/provider-cell/server";
import { CellSettings } from "@t3tools/provider-cell/settings";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NodeHttp from "node:http";

const argValue = (name: string) => {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
};
const liveUrl = argValue("--url") ?? "http://127.0.0.1:9884";
const liveToken = argValue("--token") ?? "";
const decode = Schema.decodeUnknownEffect(CellSettings);

function serve(
  answer: (path: string) => {
    status: number;
    type: string;
    body: string;
    headers?: Record<string, string>;
  },
): Promise<{ url: string; close: () => void }> {
  return new Promise((resolve) => {
    const requests: Array<string> = [];
    const server = NodeHttp.createServer((req, res) => {
      requests.push(`${req.method} ${req.url}`);
      const { status, type, body, headers } = answer(req.url ?? "");
      res.writeHead(status, { "content-type": type, ...headers });
      res.end(body);
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () => {
          console.log(`  (stand-in on ${port} saw: ${requests.join(", ") || "nothing"})`);
          server.close();
        },
      });
    });
  });
}

const oldState = (extra: Record<string, unknown>) =>
  JSON.stringify({
    version: "0.2.0",
    model: "gpt-6-astra",
    running: false,
    runId: null,
    seq: 0,
    configured: true,
    turns: 0,
    ...extra,
  });

const main = Effect.gen(function* () {
  const transport = yield* makeHttpCellTransport;
  const probe = Effect.fn("LiveCellProbe.probe")(function* (
    label: string,
    baseUrl: string,
    token = "",
    projectPrefix = "t3-",
  ) {
    const settings = yield* decode({ enabled: true, baseUrl, token, projectPrefix });
    const result = yield* checkCellProviderStatus(settings, transport);
    console.log(`${label}: ${baseUrl}`);
    console.log(
      `  status=${result.status} installed=${result.installed} version=${JSON.stringify(result.version)} models=${JSON.stringify(result.models.map((m) => m.slug))}`,
    );
    console.log(`  auth=${result.auth.status} message=${JSON.stringify(result.message)}`);
  });
  yield* probe(liveToken ? "live cell with the token" : "live cell", liveUrl, liveToken);
  if (liveToken) yield* probe("live cell without a token", liveUrl);
  const legacy = yield* Effect.promise(() =>
    serve(() => ({ status: 200, type: "application/json", body: oldState({}) })),
  );
  yield* probe("old-protocol tenant (state without `protocol`)", legacy.url);
  legacy.close();
  const level2 = yield* Effect.promise(() =>
    serve(() => ({ status: 200, type: "application/json", body: oldState({ protocol: 2 }) })),
  );
  yield* probe("protocol-2 tenant", level2.url);
  level2.close();
  const notACell = yield* Effect.promise(() =>
    serve(() => ({ status: 200, type: "text/html", body: "<html><body>hello</body></html>" })),
  );
  yield* probe("not a cell (HTML 200)", notACell.url);
  notACell.close();
  const notFound = yield* Effect.promise(() =>
    serve(() => ({ status: 404, type: "text/plain", body: "no such project" })),
  );
  yield* probe("404 server", notFound.url);
  notFound.close();
  const unconfigured = yield* Effect.promise(() =>
    serve(() => ({
      status: 200,
      type: "application/json",
      body: oldState({ protocol: 3, configured: false }),
    })),
  );
  yield* probe("protocol-3 tenant without an API key", unconfigured.url);
  unconfigured.close();
  const refusing = yield* Effect.promise(() =>
    serve(() => ({
      status: 401,
      type: "text/plain; charset=utf-8",
      headers: { "www-authenticate": 'Bearer realm="acme"' },
      body: "Unauthorized: the session was reset by a new invite code; log in at /login, or send the token of POST /login/token as Authorization: Bearer",
    })),
  );
  yield* probe("tenant refusing a stale token (401)", refusing.url, "stale-token");
  refusing.close();
  const failingClosed = yield* Effect.promise(() =>
    serve(() => ({
      status: 503,
      type: "text/plain; charset=utf-8",
      headers: { "retry-after": "60" },
      body: "Service unavailable: SESSION_SECRET is not set, so no request to a cell can be checked for a login; set SESSION_SECRET and INVITE_SECRET as secrets of this deployment (docs/cell-edition.md, Login)",
    })),
  );
  yield* probe("tenant with a base domain and no SESSION_SECRET (503)", failingClosed.url);
  failingClosed.close();
  yield* probe("closed port", "http://127.0.0.1:9");
});

await Effect.runPromise(
  main.pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeHttpClient.layerUndici,
        NodeWS.layerWebSocketConstructor,
      ),
    ),
  ),
);
process.exit(0);
