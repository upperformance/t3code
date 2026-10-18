// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off preferSchemaOverJson:off - a plain Node stand-in for celld, used by recorders and tests.
/**
 * A stand-in for a celld tenant: the cell's HTTP API and WebSocket on a local
 * port, with a scripted model instead of the Responses API. It answers the
 * shared fixture prompts the way the real cell does, so transcripts recorded
 * against it carry the real protocol shape. `protocol: "current"` speaks the
 * protocol with run ids, sequence numbers, event replay and structured `done`
 * errors; `protocol: "legacy"` is the tenant before those were added.
 */
import * as NodeHttp from "node:http";
import { WebSocketServer, type WebSocket } from "ws";

export type FakeCellProtocol = "current" | "legacy";

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
  | {
      readonly type: "fail";
      readonly error: string;
      readonly code?: "usage_limit" | "provider_error" | "transport_error" | "tool_error";
      readonly status?: number;
      readonly resetAt?: string;
    }
  /**
   * Delegates to a child cell: the run reports the task as running, and once
   * the run has ended the child finishes and wakes the cell with `wake` steps.
   */
  | {
      readonly type: "delegate";
      readonly taskId: string;
      readonly title: string;
      readonly summary: string;
      readonly wake: ReadonlyArray<FakeCellStep>;
    };

/** `answers` are the assistant texts of the project's earlier runs, oldest first. */
export type FakeCellScript = (
  prompt: string,
  turn: number,
  answers: ReadonlyArray<string>,
) => ReadonlyArray<FakeCellStep>;

export interface FakeCellServer {
  readonly baseUrl: string;
  readonly protocol: FakeCellProtocol;
  readonly prompts: (project: string) => ReadonlyArray<string>;
  /** Closes every socket as celld does when a cell moves; clients are expected to reconnect. */
  readonly dropSockets: () => void;
  /** While set, socket upgrades are refused, as when the origin is briefly unreachable. */
  readonly rejectSockets: (reject: boolean) => void;
  /** Resolves when the next socket attaches to the project. */
  readonly onceConnected: (project: string) => Promise<void>;
  /** Resolves when the project's current run ends, or at once when it is idle. */
  readonly onceIdle: (project: string) => Promise<void>;
  /** Ends the project's run as the cell's own page would, without going through the client. */
  readonly cancelRun: (project: string) => void;
  /** Starts a run as another client of the same cell would; T3's prompt then answers 409. */
  readonly startForeignRun: (
    project: string,
    prompt: string,
    steps?: ReadonlyArray<FakeCellStep>,
  ) => boolean;
  readonly close: () => Promise<void>;
}

interface Run {
  readonly runId: string;
  cancel: () => void;
}

interface Project {
  readonly prompts: Array<string>;
  readonly sockets: Set<WebSocket>;
  readonly connectionWaiters: Array<() => void>;
  readonly idleWaiters: Array<() => void>;
  readonly events: Array<Record<string, unknown>>;
  readonly tasks: Map<string, Record<string, unknown>>;
  readonly answers: Array<string>;
  run: Run | null;
  runs: number;
  seq: number;
  model: string;
  configured: boolean;
}

const EVENT_RETENTION = 20_000;

/**
 * Answers the shared fixture prompts: "Respond with exactly: X", a shell that
 * waits for Stop, and the resume prompt that repeats the previous answer.
 */
const defaultFakeCellScript: FakeCellScript = (prompt, _turn, answers) => {
  const delegated =
    /Delegate a (\w+) to a child cell\. Reply with exactly: (\S+)\. When the child reports, reply with exactly: (\S+)/.exec(
      prompt,
    );
  if (delegated) {
    return [
      {
        type: "delegate",
        taskId: "ab12",
        title: delegated[1]!,
        summary: `The ${delegated[1]!} is done.`,
        wake: [{ type: "assistant", text: delegated[3]! }],
      },
      { type: "assistant", text: delegated[2]! },
    ];
  }
  const sleep = /`(sleep \d+[^`]*)`/.exec(prompt);
  const steps: Array<FakeCellStep> = [];
  if (sleep) steps.push({ type: "shell_until_cancel", command: sleep[1]! });
  const answer = /(?:Respond with|write) (?:exactly|the following text): (.+?)\s*$/m.exec(prompt);
  const text = answer ? answer[1]!.trim() : "ok";
  const repeats = /repeat the exact final answer you gave in the previous turn/i.test(prompt);
  steps.push({
    type: "assistant",
    text: repeats ? [answers.at(-1) ?? "", text].filter(Boolean).join("\n") : text,
  });
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
    readonly protocol?: FakeCellProtocol;
    /** How long a delegated child works after its parent's run ended. */
    readonly delegateDelayMs?: number;
  } = {},
): Promise<FakeCellServer> {
  const script = options.script ?? defaultFakeCellScript;
  const protocol = options.protocol ?? "current";
  const current = protocol === "current";
  const version = options.version ?? (current ? "0.3.0" : "0.2.0");
  const delegateDelayMs = options.delegateDelayMs ?? 20;
  const projects = new Map<string, Project>();
  const timers = new Set<NodeJS.Timeout>();
  let rejectSockets = false;
  const project = (name: string): Project => {
    let existing = projects.get(name);
    if (!existing) {
      existing = {
        prompts: [],
        sockets: new Set(),
        connectionWaiters: [],
        idleWaiters: [],
        events: [],
        tasks: new Map(),
        answers: [],
        run: null,
        runs: 0,
        seq: 0,
        model: options.model ?? "gpt-6-astra",
        configured: options.configured ?? true,
      };
      projects.set(name, existing);
    }
    return existing;
  };
  const send = (target: Project, event: Record<string, unknown>) => {
    const payload = JSON.stringify(event);
    for (const socket of target.sockets) {
      if (socket.readyState === socket.OPEN) socket.send(payload);
    }
  };
  /** Persisted events carry the next sequence number and can be replayed. */
  const broadcast = (target: Project, event: Record<string, unknown>) => {
    if (!current) return send(target, event);
    const sequenced = { ...event, seq: ++target.seq };
    target.events.push(sequenced);
    if (target.events.length > EVENT_RETENTION) target.events.shift();
    send(target, sequenced);
  };
  const authorized = (request: NodeHttp.IncomingMessage) =>
    !options.token || request.headers.authorization === `Bearer ${options.token}`;
  const later = (ms: number, run: () => void) => {
    const timer = setTimeout(() => {
      timers.delete(timer);
      run();
    }, ms);
    timers.add(timer);
  };

  const finishDelegated = (
    target: Project,
    step: Extract<FakeCellStep, { readonly type: "delegate" }>,
  ) => {
    const task = {
      ...target.tasks.get(step.taskId),
      taskId: step.taskId,
      title: step.title,
      status: "completed",
      summary: step.summary,
      changes: [],
      finished: Date.now(),
    };
    target.tasks.set(step.taskId, task);
    broadcast(target, { type: "task", task });
    const text = `[Delegated task ${step.taskId} "${step.title}" completed]\n${step.summary}`;
    if (target.run) {
      broadcast(target, {
        type: "wake",
        text,
        ...(current ? { runId: target.run.runId, origin: "wake" } : {}),
      });
      return;
    }
    startRun(target, text, "wake", step.wake);
  };

  const startRun = (
    target: Project,
    prompt: string,
    origin: "user" | "wake",
    scripted?: ReadonlyArray<FakeCellStep>,
  ): { readonly runId: string; readonly seq: number } | undefined => {
    if (target.run || !prompt.trim()) return undefined;
    if (!target.configured) {
      broadcast(target, { type: "error", text: "OPENAI_API_KEY is not configured for this cell" });
      return undefined;
    }
    if (origin === "user") target.prompts.push(prompt);
    const steps = [...(scripted ?? script(prompt, target.prompts.length, target.answers))];
    const runId = `run-${++target.runs}`;
    const scope = current ? { runId } : {};
    let cancelled = false;
    let wakeCancel: (() => void) | undefined;
    const run: Run = {
      runId,
      cancel: () => {
        cancelled = true;
        wakeCancel?.();
      },
    };
    target.run = run;
    broadcast(target, {
      type: origin === "wake" ? "wake" : "user",
      text: prompt,
      ...scope,
      ...(current ? { origin } : {}),
    });
    const accepted = { runId, seq: target.seq };
    const delegated: Array<Extract<FakeCellStep, { readonly type: "delegate" }>> = [];
    void (async () => {
      let failure: Extract<FakeCellStep, { readonly type: "fail" }> | undefined;
      for (const step of steps) {
        if (cancelled) break;
        switch (step.type) {
          case "assistant":
            target.answers.push(step.text);
            broadcast(target, { type: "assistant", text: step.text, ...scope });
            break;
          case "shell":
            broadcast(target, { type: "shell", command: step.command, ...scope });
            broadcast(target, {
              type: "shell_output",
              command: step.command,
              stdout: step.output,
              stderr: "",
              exitCode: step.exitCode,
              timedOut: false,
              ...scope,
            });
            break;
          case "shell_until_cancel": {
            broadcast(target, { type: "shell", command: step.command, ...scope });
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
              ...scope,
            });
            break;
          }
          case "tool":
            broadcast(target, {
              type: "tool",
              name: step.name,
              arguments: step.arguments,
              ...scope,
            });
            broadcast(target, {
              type: "tool_output",
              name: step.name,
              output: step.output,
              ...scope,
            });
            break;
          case "patch":
            broadcast(target, {
              type: "patch",
              operation: step.operation,
              path: step.path,
              status: "completed",
              text: "",
              ...scope,
            });
            break;
          case "error":
            broadcast(target, { type: "error", text: step.text, ...scope });
            break;
          case "fail":
            failure = step;
            break;
          case "delegate": {
            const task = {
              taskId: step.taskId,
              title: step.title,
              status: "running",
              created: Date.now(),
              finished: null,
            };
            target.tasks.set(step.taskId, task);
            broadcast(target, {
              type: "tool",
              name: "delegate_task",
              arguments: { title: step.title },
              ...scope,
            });
            broadcast(target, {
              type: "tool_output",
              name: "delegate_task",
              output: JSON.stringify({ taskId: step.taskId, status: "running" }),
              ...scope,
            });
            broadcast(target, { type: "task", task });
            delegated.push(step);
            break;
          }
        }
        if (failure !== undefined) break;
      }
      const error = cancelled
        ? current
          ? { code: "cancelled", message: "Cancelled" }
          : "Cancelled"
        : failure === undefined
          ? undefined
          : current
            ? {
                code: failure.code ?? "provider_error",
                message: failure.error,
                ...(failure.status === undefined ? {} : { status: failure.status }),
                ...(failure.resetAt === undefined ? {} : { resetAt: failure.resetAt }),
              }
            : failure.error;
      broadcast(
        target,
        error === undefined ? { type: "done", ...scope } : { type: "done", error, ...scope },
      );
      if (target.run === run) target.run = null;
      for (const waiter of target.idleWaiters.splice(0)) waiter();
      for (const step of delegated) later(delegateDelayMs, () => finishDelegated(target, step));
    })();
    return accepted;
  };

  const server = NodeHttp.createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://fake-cell");
    const match = /^\/cell\/([^/]+)(\/.*)?$/.exec(url.pathname);
    const text = (status: number, body = "") => {
      response.writeHead(status, { "content-type": "text/plain; charset=utf-8" });
      response.end(body);
    };
    const json = (status: number, body: unknown) => {
      response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
      response.end(JSON.stringify(body));
    };
    if (!match) return text(404, "Not found");
    if (!authorized(request)) return text(401, "Unauthorized");
    const target = project(decodeURIComponent(match[1]!));
    const path = match[2] ?? "/";
    if (path === "/state" && request.method === "GET") {
      return json(200, {
        model: target.model,
        running: target.run !== null,
        configured: target.configured,
        turns: target.prompts.length,
        sockets: target.sockets.size,
        parent: null,
        depth: 0,
        tasks: target.tasks.size,
        version,
        ...(current ? { runId: target.run?.runId ?? null, seq: target.seq } : {}),
      });
    }
    if (path === "/prompt" && request.method === "POST") {
      const body = await readBody(request);
      if (target.run) {
        return current
          ? json(409, { runId: target.run.runId })
          : text(409, "An agent run is already in progress");
      }
      const accepted = startRun(target, body, "user");
      if (accepted === undefined) return text(409, "The prompt was not started");
      return current ? json(202, accepted) : text(202);
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
      return json(200, [...target.tasks.values()]);
    }
    if (path === "/events" && request.method === "GET" && current) {
      const after = Number(url.searchParams.get("after") ?? "0");
      const limit = Number(url.searchParams.get("limit") ?? String(EVENT_RETENTION));
      return json(
        200,
        target.events
          .filter((event) => typeof event.seq === "number" && event.seq > after)
          .slice(0, Math.max(0, limit)),
      );
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
        if (type === "prompt") startRun(target, String(text ?? ""), "user");
        else if (type === "cancel") target.run?.cancel();
      });
      ws.send(
        JSON.stringify({
          type: "state",
          running: target.run !== null,
          parent: null,
          depth: 0,
          ...(current ? { runId: target.run?.runId ?? null, seq: target.seq } : {}),
        }),
      );
      ws.send(JSON.stringify({ type: "tasks", tasks: [...target.tasks.values()] }));
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
    protocol,
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
    onceIdle: (name) =>
      new Promise<void>((resolve) => {
        const target = project(name);
        if (target.run === null) resolve();
        else target.idleWaiters.push(resolve);
      }),
    cancelRun: (name) => projects.get(name)?.run?.cancel(),
    startForeignRun: (name, prompt, steps) =>
      startRun(project(name), prompt, "user", steps) !== undefined,
    close: async () => {
      for (const timer of timers) clearTimeout(timer);
      timers.clear();
      for (const target of projects.values()) {
        target.run?.cancel();
        for (const ws of target.sockets) ws.terminate();
      }
      sockets.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
