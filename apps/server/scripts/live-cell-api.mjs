// A scripted OpenAI Responses API for the live cell run: point a cell's OPENAI_BASE_URL at it.
//   node scripts/live-cell-api.mjs [port=9815] [holdMs=20000]   (CHILD_DELAY_MS=30000 for delete-delegate)
// The latest user message picks the answer; the prompt table is in docs/internals/provider-cell.md (Testing).
// Every request is logged with its step and prompt; every answer reports usage (bytes / 4).
import * as NodeHttp from "node:http";

const port = Number(process.argv[2] ?? 9815);
const HOLD_MS = Number(process.argv[3] ?? 20_000);
const CHILD_DELAY_MS = Number(process.env.CHILD_DELAY_MS ?? 4_000);
const WAKE_WORD = process.env.WAKE_WORD ?? "WAKE_DONE";

let calls = 0;
const stamp = () => new Date().toISOString();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const say = (text) => ({
  type: "message",
  id: "msg",
  role: "assistant",
  content: [{ type: "output_text", text }],
});
const call = (name, args) => ({
  type: "function_call",
  id: "fc",
  call_id: `call_${Date.now()}_${calls}`,
  name,
  arguments: JSON.stringify(args),
});

const DELEGATE =
  /Delegate a (\w+) to a child cell\. Reply with exactly: (\S+)\.(?: Then fail with: ([^.]+)\.)? When the child reports, reply with exactly: (\S+)/;
const ANSWER = /(?:Respond with|write) (?:exactly|the following text): (.+?)\s*$/im;

NodeHttp.createServer((req, res) => {
  let data = "";
  req.on("data", (chunk) => (data += chunk));
  req.on("end", async () => {
    calls += 1;
    const n = calls;
    const json = (status, value, headers = {}) => {
      console.log(`${stamp()} [api] #${n} -> ${status} ${JSON.stringify(value).slice(0, 160)}`);
      res.writeHead(status, { "content-type": "application/json", ...headers });
      res.end(JSON.stringify(value));
    };
    let body;
    try {
      body = JSON.parse(data);
    } catch {
      return json(400, { error: { message: "bad json" } });
    }
    const items = body.input ?? [];
    let start = items.length - 1;
    while (start >= 0 && items[start].role !== "user") start -= 1;
    const prompt = typeof items[start]?.content === "string" ? items[start].content : "";
    const step = items.slice(start + 1).filter((item) => item.type === "function_call").length;
    const previous = items
      .slice(0, Math.max(start, 0))
      .toReversed()
      .find((item) => item.type === "message" && item.role === "assistant");
    const previousText =
      previous?.content?.find?.((part) => part.type === "output_text")?.text ?? "";
    const usage = (output) => ({
      input_tokens: Math.ceil(data.length / 4),
      input_tokens_details: { cached_tokens: 0 },
      output_tokens: Math.ceil(JSON.stringify(output).length / 4),
      output_tokens_details: { reasoning_tokens: 0 },
    });
    const ok = (output) =>
      json(200, { id: `resp_${n}`, status: "completed", output, usage: usage(output) });
    console.log(
      `${stamp()} [api] #${n} step ${step} model=${body.model} prompt=${JSON.stringify(prompt.split("\n")[0].slice(0, 100))}`,
    );

    const delegated = DELEGATE.exec(prompt);
    if (delegated) {
      const delegate = call("delegate_task", {
        task: "Review the project",
        title: delegated[1],
        mode: "async",
      });
      const failWith = delegated[3];
      if (failWith === undefined) return ok(step === 0 ? [delegate] : [say(delegated[2])]);
      // The run says A after delegating, so the step after the tool's answer can fail it.
      if (step === 0) return ok([delegate, say(delegated[2])]);
      return json(500, { error: { type: "server_error", message: failWith } });
    }
    if (prompt.startsWith("[Delegated task")) return ok([say(WAKE_WORD)]);
    if (prompt.startsWith("Review the project")) {
      if (step > 0) return ok([say("Reviewed.")]);
      await sleep(CHILD_DELAY_MS);
      return ok([call("shell", { command: "echo reviewed > review.txt" })]);
    }
    if (prompt.startsWith("Limit")) {
      return json(
        429,
        {
          error: {
            type: "usage_limit_reached",
            message: "You have hit your usage limit",
            resets_in_seconds: 3600,
          },
        },
        { "retry-after": "3600" },
      );
    }

    const answer = ANSWER.exec(prompt);
    const text = answer ? answer[1].trim() : `Ok: ${prompt.split("\n")[0].slice(0, 40)}`;
    const sleepCommand = /`(sleep (\d+)[^`]*)`/.exec(prompt);
    if (sleepCommand) {
      if (step === 0) return ok([call("shell", { command: sleepCommand[1] })]);
      // The Stop fixture: the answer after the shell step is held too, so the
      // run is still in progress when Stop arrives even if the shell returned.
      if (Number(sleepCommand[2]) >= 10) await sleep(HOLD_MS);
      return ok([say(text)]);
    }
    if (prompt.startsWith("Hold")) {
      await sleep(HOLD_MS);
      return ok([say(text)]);
    }
    if (/repeat the exact final answer you gave in the previous turn/i.test(prompt)) {
      return ok([say([previousText, text].filter(Boolean).join("\n"))]);
    }
    return ok([say(text)]);
  });
}).listen(port, "127.0.0.1", () =>
  console.log(
    `${stamp()} [api] listening on 127.0.0.1:${port}, hold ${HOLD_MS} ms, child delay ${CHILD_DELAY_MS} ms`,
  ),
);
