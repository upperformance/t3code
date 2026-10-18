# Cell provider

A cell is a project that lives in a Durable Object on celld: its file tree in
SQLite, a Codex-protocol agent loop over the OpenAI Responses API, just-bash
and a TypeScript compiler as the only tools, and child cells for delegated
work. The `cell` driver runs a T3 thread on one such cell over its HTTP and
WebSocket API. This page records the decisions behind the adapter in
[`packages/provider-cell`](../../packages/provider-cell) and the alternatives
they replaced. The cell's own design is in the vim.app repository
(`docs/cell-edition.md`); the [provider constraints](./providers.md) and the
[capability system](../orchestration-v2/provider-capability-system.md) explain
the rules the adapter follows.

## What a cell is to T3

T3's model of a provider is a process on the environment's machine that works
on a directory the environment owns. A cell is neither: it runs on a remote
origin, and its project tree is inside the cell. The adapter therefore treats
the cell as the whole agent plus the whole workspace, and T3's local project
directory as nothing more than the thread's home in the sidebar.

- **Instance.** A provider instance holds the tenant's base URL
  (`https://<tenant>.vim.app`), an optional bearer token, and the prefix for
  project names. Two tenants are two instances. There is no default instance,
  because an instance without a base URL cannot do anything, so the user adds
  one in Settings → Providers. The token is stored as a settings secret and
  sent as `authorization: Bearer <token>` on every request, including the
  WebSocket handshake. celld has no authentication today, so the field is
  optional; the adapter is ready for the day it is required.
- **Thread.** One T3 thread is one cell project. The project name is derived
  from the thread id (prefix plus the sanitized tail of the id, capped so
  child cells keep room for their suffix), recorded as the provider thread's
  native ref, and reused on resume. The first prompt creates the cell: a
  Durable Object exists from its first request. Nothing is deleted when a T3
  thread is deleted; the cell and its tree stay on the tenant until someone
  removes them there. A per-thread project name chosen by the user is not
  offered yet; see the open questions.
- **Conversation.** The cell keeps its own transcript and replays it on every
  model call. T3 never injects history into it. A context handoff from
  another provider arrives as the first user prompt of a fresh cell, which is
  the handoff path every other provider without native resume uses.

## What the local project directory means

Nothing. The cell's files are not on the environment's disk, so the features
T3 builds on that directory are empty or disabled for a cell thread:

- The diff view and checkpoints work on the local checkout, which the cell
  never touches. Checkpoints are still captured at turn boundaries, because
  the checkpoint boundary is app-owned and does not consult provider
  capabilities, but they contain no changes. The adapter reports
  `checkpointing.appCanCheckpointFilesystem: false` so the UI can hide the
  diff affordances once it reads that flag.
- Worktrees have no meaning; a cell thread in a worktree runs on the same
  cell as one on the root checkout.
- The terminal is the environment's shell, not the cell's. The cell's shell
  is reachable through its own page on the tenant.
- Attachments cannot be delivered. The adapter rejects a turn that carries
  one, with a validation failure, instead of pretending the agent saw it.
  Uploading files into the tree through `PUT fs/file` is a follow-up.

The capability flags that make the UI honest: `threads.canForkThread`,
`canForkFromTurn`, `canRollbackThread` and `canReadThreadSnapshot` off;
`checkpointing.providerCanRollbackConversation` and
`providerCanReadConversationSnapshot` off; every `approvals` flag off;
`tools.supportsMcpTools` off; `streaming.streamsAssistantText` off, because
the cell delivers each assistant message when a model step ends.

## Event mapping

The cell has one WebSocket per project and broadcasts the same events to every
client, so the adapter is one subscriber among the browser pages. Events map
to T3's normalized events as follows.

| Cell event               | T3 effect                                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `state`                  | On connect. `running` with no T3 turn means another client's turn is active; a prompt then answers 409.        |
| `tasks`                  | On connect. Running tasks become the provider thread's pending background tasks.                               |
| `user`                   | Ignored: it echoes the prompt T3 already recorded.                                                             |
| `assistant`              | A completed assistant message item. No streaming.                                                              |
| `shell` / `shell_output` | A `command_execution` item, running then completed with stdout and stderr joined and the exit code.            |
| `tool` / `tool_output`   | A `dynamic_tool` item for the orchestration tools (`delegate_task` and friends), with input and output.        |
| `patch`                  | A completed `file_change` item per changed path. The cell sends no diff, so the item has a name and operation. |
| `task`                   | Subagent lifecycle: `subagent.updated`, a `subagent` node and a `subagent` item keyed by the task id.          |
| `wake`                   | A provider-initiated turn: a continuation request so T3 opens a run that adopts the events that follow.        |
| `error`                  | A `system_notice` item during a turn (an apply_patch failure, for example); the turn goes on.                  |
| `done`                   | Turn completed; with `error`, failed; with `error: "Cancelled"`, interrupted when T3 asked, else cancelled.    |
| `reset`                  | The conversation was reset from another client. An active turn fails; an idle thread continues on a fresh one. |
| `fs`, `exec`             | Ignored: they serve the browser's file sync.                                                                   |

Interrupt is `POST cancel`; the cell confirms with `done {error: "Cancelled"}`,
and the adapter waits for that before reporting the turn interrupted. Steering
is by interrupt and restart, which the orchestrator implements. Queued
messages wait in T3; the cell refuses a second prompt with 409.

A `task` event can arrive after the turn that delegated the task settled, or
on a later connection. Updates are routed by task id to the subagent record
created when the task was first seen, never by whichever turn is active. A
task first seen with no turn active (started from the cell's own page) only
counts as pending background work; it gets no item in any run.

## Identity

The cell has no turn or item ids. Per
[entity ids and correlation](../orchestration-v2/entity-ids-and-correlation.md)
the adapter mints stable app ids by scoped ordinal and says so in the identity
capabilities: thread ids are strong (the project name is a durable identity),
turn ids are weak (the project plus the provider turn ordinal), item and
request ids are none (allocated by ordinal within the turn). Item ids are
derived from `instance:providerThread:turn:ordinal`, so replaying the same
transcript produces the same ids.

## Runtime modes and policy

The cell is the sandbox: nothing the agent runs leaves its isolate, there is
no network from the shell, and there are no approvals to ask for. The
presentation offers only `full-access`, per the rule that offered modes must
be enforced natively, and the capabilities declare native enforcement. A
thread stored with another mode runs as `full-access`; the server's policy
layer maps unoffered modes to Supervised, which the cell cannot distinguish,
so the user guide says so.

## Model catalog and status

`GET state` reports the cell's one model, so the catalog has one entry marked
default, plus the instance's custom models. Model switching is not supported
in a session; the model is the tenant's `OPENAI_MODEL`. The status probe reads
`GET state` of a dedicated probe project (`<prefix>probe`) and never posts a
prompt, so a background refresh cannot start a run, spend tokens, or create
work. The probe creates one empty Durable Object per tenant; that is the cost
of a probe without a dedicated health endpoint. `configured: false` is a
warning (the tenant has no model API key); an unreachable origin or a non-JSON
answer is an error. There is no version advisory beyond the manifest row: the
cell reports its own `version`, and nothing on the environment updates it.

## MCP

T3 injects its orchestrator MCP server into every thread so agents can
delegate, link pull requests, and read threads. The cell cannot run an MCP
client today: its agent loop has a fixed tool list, and the fleet's egress
policy only allows the model API. T3's MCP tools are therefore unavailable to
cell threads, and the adapter does not read the thread's MCP session or
advertise those tools in its instructions. The other direction exists: the
cell serves its own delegation tools at `/cell/<project>/mcp`, which any T3
thread on another provider can take as an outside MCP server. Bridging the two
(the cell calling T3's server through the fleet) needs egress and an MCP
client in the cell, both deferred.

## Reconnect

celld closes sockets when a cell moves, and a hibernated cell drops events
sent while nobody listened. The adapter reconnects whenever the socket closes
while the session is open, at once the first time and with backoff after. On
reconnect the cell sends `state` and `tasks` again. If `running` is false while
T3 still has an active turn, the turn ended unobserved; the adapter fails it
with a transport error that says the result stayed in the cell, and leaves the
thread reusable, because the next prompt continues the same transcript. If
`running` is true the turn is still going and its remaining events, including
the final assistant message, arrive normally.

## Alternatives rejected

- **An ACP bridge process.** The cell edition's own design deferred a Node
  program that speaks ACP on one side and the cell API on the other. A native
  adapter is smaller: no process to install, probe, or update, and the
  capability flags can say exactly what the cell does instead of what ACP
  assumes.
- **Mapping each T3 thread to a child cell of one tenant project.** Children
  are forks of a parent's tree and report back to it; a T3 thread needs a
  root. Root projects are the unit.
- **Forking a T3 thread by forking the cell.** `POST fork` copies a tree into
  a child and clears the conversation, which is not a fork of the
  conversation. Forks are declared unsupported and T3 uses portable handoff.
- **Streaming assistant text.** The cell delivers text per model step; faking
  deltas would lie about latency. The capability says no streaming.
- **Imitating approvals in T3.** There is nothing to approve: the cell cannot
  reach the host. Offering Supervised would be a mode the cell does not
  enforce.

## Testing

Replay fixtures for `simple`, `multi_turn` and `turn_interrupt` run the real
orchestrator, adapter and projections over a scripted transport that replaces
only the HTTP and WebSocket boundary. The transcript protocol is
`cell.http-ws`: outbound frames are the HTTP requests and WebSocket connects
the adapter makes, inbound frames are the responses and socket events. The
recorder in `apps/server/scripts/record-cell-replay-fixture.ts` records
against a live tenant (`--url`) or against the fake cell in
`@t3tools/provider-cell/testing`, a Node HTTP and WebSocket server that answers
the shared fixture prompts the way the cell does. The checked-in transcripts
were recorded against the fake cell; re-record against a tenant when the
cell's protocol changes.

## Open questions

- Project naming. Deriving the name from the thread id is deterministic and
  collision-free, but opaque on the tenant's project list. A user-chosen name
  per thread would need a thread-level setting the adapter contract does not
  carry today.
- Where cell threads appear. They are created under a local project like any
  thread, and the sidebar groups them with threads that edit that project's
  checkout. A project kind that has no directory would be the honest shape.
- Deleting a T3 thread. Nothing removes the cell. Whether T3 should offer
  that, and through which API, is undecided.
- Attachments. `PUT fs/file` can place files in the tree; the prompt would
  then name their paths. Not built.
