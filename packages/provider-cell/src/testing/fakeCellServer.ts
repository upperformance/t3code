// @effect-diagnostics nodeBuiltinImport:off globalTimers:off preferSchemaOverJson:off - a plain Node stand-in for celld, used by recorders and tests.
/**
 * A stand-in for a celld tenant: the cell's HTTP API and WebSocket on a local
 * port, with a scripted model instead of the Responses API. It answers the
 * shared fixture prompts the way the real cell does, so transcripts recorded
 * against it carry the real protocol shape.
 */
import * as NodeHttp from "node:http";
import { WebSocketServer, type WebSocket } from "ws";

export type FakeCellStep =
  | { readonly type: "assistant"; readonly text: string }
  | {
      readonly type: "shell";
      readonly command: string;
      readonly output: string;
      readonly exitCode: number;
    }
  | { readonly type: "shell_until_cancel"; readonly command: string }
  | {
      readonly type: "tool";
      readonly name: string;
      readonly arguments: Record<string, unknown>;
      readonly output: string;
    }
  | { readonly type: "patch"; readonly operation: string; readonly path: string }
  | { readonly type: "error"; readonly text: string }
  | { readonly type: "fail"; readonly error: string };

export type FakeCellScript = (prompt: string, turn: number) => ReadonlyArray<FakeCellStep>;

export interface FakeCellServer {
  readonly baseUrl: string;
  readonly prompts: (project: string) => ReadonlyArray<string>;
  /** Closes every socket as celld does when a cell moves; clients are expected to reconnect. */
  readonly dropSockets: () => void;
  /** While set, socket upgrades are refused, as when the origin is briefly unreachable. */
  readonly rejectSockets: (reject: boolean) => void;
  /** Resolves when the next socket attaches to the project. */
  readonly onceConnected: (project: string) => Promise<void>;
  /** Ends the project's run as the cell's own page would, without going through the client. */
  readonly cancelRun: (project: string) => void;
  readonly close: () => Promise<void>;
}

interface Project {
  readonly prompts: Array<string>;
  readonly sockets: Set<WebSocket>;
  readonly connectionWaiters: Array<() => void>;
  run: { cancel: () => void } | null;
  model: string;
  configured: boolean;
}

/** Answers the shared fixture prompts: "Respond with exactly: X" and a shell that waits for Stop. */
const defaultFakeCellScript: FakeCellScript = (prompt) => {
  const sleep = /`(sleep \d+[^`]*)`/.exec(prompt);
  const steps: Array<FakeCellStep> = [];
  if (sleep) steps.push({ type: "shell_until_cancel", command: sleep[1]! });
  const answer = /Respond with (?:exactly|the following text): (.+?)\s*$/m.exec(prompt);
  steps.push({ type: "assistant", text: answer ? answer[1]!.trim() : "ok" });
  return steps;
};

function readBody(request: NodeHttp.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => (body += chunk));
    request.on("end", () => resolve(body));
    request.on("error", reject);
  });
}

export async function startFakeCellServer(
  options: {
    readonly script?: FakeCellScript;
    readonly model?: string;
    readonly configured?: boolean;
    readonly version?: string;
    readonly token?: string;
  } = {},
): Promise<FakeCellServer> {
  const script = options.script ?? defaultFakeCellScript;
  const version = options.version ?? "0.2.0";
  const projects = new Map<string, Project>();
  let rejectSockets = false;
  const project = (name: string): Project => {
    let existing = projects.get(name);
    if (!existing) {
      existing = {
        prompts: [],
        sockets: new Set(),
        connectionWaiters: [],
        run: null,
        model: options.model ?? "gpt-6-astra",
        configured: options.configured ?? true,
      };
      projects.set(name, existing);
    }
    return existing;
  };
  const broadcast = (target: Project, event: Record<string, unknown>) => {
    const payload = JSON.stringify(event);
    for (const socket of target.sockets) {
      if (socket.readyState === socket.OPEN) socket.send(payload);
    }
  };
  const authorized = (request: NodeHttp.IncomingMessage) =>
    !options.token || request.headers.authorization === `Bearer ${options.token}`;

  const startRun = (target: Project, prompt: string): boolean => {
    if (target.run || !prompt.trim()) return false;
    if (!target.configured) {
      broadcast(target, { type: "error", text: "OPENAI_API_KEY is not configured for this cell" });
      return false;
    }
    target.prompts.push(prompt);
    const steps = [...script(prompt, target.prompts.length)];
    let cancelled = false;
    let wakeCancel: (() => void) | undefined;
    const run = {
      cancel: () => {
        cancelled = true;
        wakeCancel?.();
      },
    };
    target.run = run;
    broadcast(target, { type: "user", text: prompt });
    void (async () => {
      let error: string | null = null;
      for (const step of steps) {
        if (cancelled) break;
        switch (step.type) {
          case "assistant":
            broadcast(target, { type: "assistant", text: step.text });
            break;
          case "shell":
            broadcast(target, { type: "shell", command: step.command });
            broadcast(target, {
              type: "shell_output",
              command: step.command,
              stdout: step.output,
              stderr: "",
              exitCode: step.exitCode,
              timedOut: false,
            });
            break;
          case "shell_until_cancel": {
            broadcast(target, { type: "shell", command: step.command });
            await new Promise<void>((resolve) => {
              wakeCancel = resolve;
            });
            wakeCancel = undefined;
            broadcast(target, {
              type: "shell_output",
              command: step.command,
              stdout: "",
              stderr: "The operation was aborted\n",
              exitCode: 1,
              timedOut: false,
            });
            break;
          }
          case "tool":
            broadcast(target, { type: "tool", name: step.name, arguments: step.arguments });
            broadcast(target, { type: "tool_output", name: step.name, output: step.output });
            break;
          case "patch":
            broadcast(target, {
              type: "patch",
              operation: step.operation,
              path: step.path,
              status: "completed",
              text: "",
            });
            break;
          case "error":
            broadcast(target, { type: "error", text: step.text });
            break;
          case "fail":
            error = step.error;
            break;
        }
        if (error !== null) break;
      }
      if (cancelled) error = "Cancelled";
      broadcast(target, error ? { type: "done", error } : { type: "done" });
      if (target.run === run) target.run = null;
    })();
    return true;
  };

  const server = NodeHttp.createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://fake-cell");
    const match = /^\/cell\/([^/]+)(\/.*)?$/.exec(url.pathname);
    const text = (status: number, body = "") => {
      response.writeHead(status, { "content-type": "text/plain; charset=utf-8" });
      response.end(body);
    };
    if (!match) return text(404, "Not found");
    if (!authorized(request)) return text(401, "Unauthorized");
    const target = project(decodeURIComponent(match[1]!));
    const path = match[2] ?? "/";
    if (path === "/state" && request.method === "GET") {
      response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
      return response.end(
        JSON.stringify({
          model: target.model,
          running: target.run !== null,
          configured: target.configured,
          turns: target.prompts.length,
          sockets: target.sockets.size,
          parent: null,
          depth: 0,
          tasks: 0,
          version,
        }),
      );
    }
    if (path === "/prompt" && request.method === "POST") {
      const started = startRun(target, await readBody(request));
      return started
        ? text(202)
        : text(
            409,
            target.run ? "An agent run is already in progress" : "The prompt was not started",
          );
    }
    if (path === "/cancel" && request.method === "POST") {
      target.run?.cancel();
      return text(204);
    }
    if (path === "/reset" && request.method === "POST") {
      target.run?.cancel();
      broadcast(target, { type: "reset" });
      return text(204);
    }
    if (path === "/tasks" && request.method === "GET") {
      response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
      return response.end("[]");
    }
    return text(404, "Not found");
  });
  const sockets = new WebSocketServer({ noServer: true });
  server.on("upgrade", (request, socket, head) => {
    const url = new URL(request.url ?? "/", "http://fake-cell");
    const match = /^\/cell\/([^/]+)\/ws$/.exec(url.pathname);
    if (!match || !authorized(request) || rejectSockets) {
      socket.destroy();
      return;
    }
    const target = project(decodeURIComponent(match[1]!));
    sockets.handleUpgrade(request, socket, head, (ws) => {
      target.sockets.add(ws);
      ws.on("close", () => target.sockets.delete(ws));
      ws.on("message", (data) => {
        let message: unknown;
        try {
          message = JSON.parse(String(data));
        } catch {
          return;
        }
        if (typeof message !== "object" || message === null) return;
        const { type, text } = message as { type?: unknown; text?: unknown };
        if (type === "prompt") startRun(target, String(text ?? ""));
        else if (type === "cancel") target.run?.cancel();
      });
      ws.send(
        JSON.stringify({ type: "state", running: target.run !== null, parent: null, depth: 0 }),
      );
      ws.send(JSON.stringify({ type: "tasks", tasks: [] }));
      for (const waiter of target.connectionWaiters.splice(0)) waiter();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("fake cell did not bind a port");
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    prompts: (name) => projects.get(name)?.prompts ?? [],
    dropSockets: () => {
      for (const target of projects.values()) {
        for (const ws of target.sockets) ws.close(1001, "The cell moved");
      }
    },
    rejectSockets: (reject) => {
      rejectSockets = reject;
    },
    onceConnected: (name) =>
      new Promise<void>((resolve) => {
        project(name).connectionWaiters.push(resolve);
      }),
    cancelRun: (name) => projects.get(name)?.run?.cancel(),
    close: async () => {
      for (const target of projects.values()) {
        target.run?.cancel();
        for (const ws of target.sockets) ws.terminate();
      }
      sockets.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
