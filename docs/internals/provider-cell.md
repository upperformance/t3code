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
- **Thread.** One T3 thread is one cell project. The project name is the
  instance prefix plus 20 hex characters of a SHA-256 over the whole thread
  id: opaque on the tenant, fixed in width so child cells keep room for their
  `.<id>` suffix under the 64-character Durable Object limit, and never a
  slice of the id, so two ids with a common tail cannot share a cell. The name
  is recorded as the provider thread's native ref and read back from there on
  resume; the hash is only computed for a thread that has no ref yet. The T3
  thread title is the project's display name and stays in T3, because the
  cell has no API to name a project. The first prompt creates the cell: a
  Durable Object exists from its first request. Deleting a T3 thread never
  deletes the cell; the project and its tree stay on the tenant until someone
  removes them there. Attaching a thread to a cell project that already
  exists goes through the contract's `initialNativeThreadId`, which the
  adapter honours; offering it in the UI needs the native-session listing
  (`nativeSessions.canList`) and a tenant API to list projects, both
  follow-ups.
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
  `checkpointing.appCanCheckpointFilesystem: false`; nothing reads that flag
  yet, so the diff, checkpoint, worktree and terminal affordances still show
  for a cell thread and simply have nothing to do with the cell.
- Worktrees have no meaning; a cell thread in a worktree runs on the same
  cell as one on the root checkout.
- The terminal is the environment's shell, not the cell's. The cell's shell
  is reachable through its own page on the tenant.
- Attachments cannot be delivered. The adapter rejects a turn that carries
  one, with a validation failure, instead of pretending the agent saw it.
  Uploading files into the tree through `PUT fs/file` is a follow-up.

The honest shape is a workspace kind that has no local checkout: a thread
whose project is the remote cell, with the local-only affordances absent
rather than inert. That is the intended follow-up; until then cell threads sit
in the ordinary project list under whichever local project they were created
in.

The capability flags that make the rest of the UI honest: `threads.canForkThread`,
`canForkFromTurn`, `canRollbackThread` and `canReadThreadSnapshot` off;
`checkpointing.providerCanRollbackConversation` and
`providerCanReadConversationSnapshot` off; every `approvals` flag off;
`tools.supportsMcpTools` off; `streaming.streamsAssistantText` off, because
the cell delivers each assistant message when a model step ends.

## The protocol the adapter relies on

The cell broadcasts the same events to every client of a project, and a
tenant may run either of two protocol generations. The adapter reads the
newer fields when they are present and falls back to the conservative rules
below when they are not; [`protocol.ts`](../../packages/provider-cell/src/server/protocol.ts)
holds the shapes.

| Field                                               | With it                                                                                                                                                                                                                   | Without it                                                                                     |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `runId` on run-scoped events, `{runId, seq}` on 202 | Ownership is by run id: an event is T3's only if its run id is the one the 202 named, and the 202's `seq`, the number of the run's first event, is where T3's run begins: anything numbered below it is an earlier run's. | Ownership starts at the echo of T3's prompt (the `user` event) and runs to the next `done`.    |
| `seq` on events, `seq` in `state`                   | Missed events are read back with `GET events?after=<seq>` on reconnect, so nothing is lost.                                                                                                                               | A gap is a gap: a running turn cannot be proven to be the same turn, and the adapter fails it. |
| `origin` on `user` and `wake`                       | A `user` event marked `wake` is a cell-initiated run, like a `wake` event.                                                                                                                                                | Only `wake` events start cell-initiated runs.                                                  |
| `done.error` as an object                           | The failure class is the error's `code`; `resetAt` is kept for usage limits.                                                                                                                                              | The class is guessed from the words of the string (`429`, `insufficient_quota`, `rate_limit`). |
| `runId` and `seq` in `GET state`                    | Read once per session when the thread is attached, and again to recover a lost prompt reply.                                                                                                                              | `GET state` still reports the model and whether the cell is running.                           |

The current deployment of celld sends none of the newer fields; they are being
added in a follow-up to the cell. The checked-in fixtures were recorded
against the fake cell speaking the newer protocol; the adapter's own tests
cover both generations.

## Which events are T3's

Nothing a run broadcasts is attributed to a T3 turn until the cell has said
which run is T3's. While `POST prompt` is unanswered, run-scoped events are
candidates held in the turn. A 202 settles them: with a run id, the
candidates with that id are the turn's, candidates from a run that began
with a `wake` become a queued wake (see below), and the rest is another
client's and is dropped. Without run ids, the candidates up to the echo of
T3's prompt belong to earlier runs; the ones after it are the turn's. A 409
discards every candidate and fails the turn with a message that another
client is running the cell, so a foreign run's `assistant` and `done` racing
ahead of the refusal can never complete a prompt the cell rejected. The same
rule applies after acceptance, with one boundary: the 202 carries the
sequence number of the run's first event, and an event from a different run
numbered below it is an earlier run's tail that the 202 overtook on the way
to T3, nobody's and no evidence about T3's run. An event from a different run
numbered after T3's run began proves that run ended without its `done`, and
the turn fails as a transport error rather than adopting the stranger.
Without run ids, the candidates after the echo are applied to the turn up to
its `done`; what follows that `done`, typically a wake and its run, goes back
to the unowned path, so the wake is queued with its output instead of being
dropped with the candidates.

A prompt whose answer never arrives is not failed on the spot. The echo among
the candidates proves the run started; failing that, `GET state` says whether
the cell is running at all, and with run ids the event log is read from the
sequence number the prompt was sent after, to find the echo and its run id.
An unnumbered tenant that is running gets a short grace for the echo to
arrive. Anything less fails the turn with a message that says what T3 could
not confirm.

| Cell event               | T3 effect                                                                                                                   |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `state`                  | On connect. Seeds the sequence cursor; on a reconnect, drives the reconciliation below.                                     |
| `tasks`                  | On connect, right behind `state`. The roster at that moment: tasks it no longer lists are settled as cancelled and dropped. |
| `user`                   | The echo that starts T3's run on an unnumbered tenant; otherwise ignored, T3 already recorded the prompt.                   |
| `assistant`              | A completed assistant message item. No streaming.                                                                           |
| `shell` / `shell_output` | A `command_execution` item, running then completed with stdout and stderr joined and the exit code.                         |
| `tool` / `tool_output`   | A `dynamic_tool` item for the orchestration tools (`delegate_task` and friends), with input and output.                     |
| `patch`                  | A completed `file_change` item per changed path. The cell sends no diff, so the item has a name and operation.              |
| `task`                   | Subagent lifecycle: `subagent.updated`, a `subagent` node and a `subagent` item keyed by the task id.                       |
| `wake`                   | A cell-initiated run, queued and offered as a continuation; inside T3's own run, a notice item.                             |
| `error`                  | A `system_notice` item during a turn (an apply_patch failure, for example); the turn goes on.                               |
| `done`                   | Turn completed; cancelled or interrupted when the error is a cancellation; otherwise failed, classified.                    |
| `reset`                  | The conversation was reset from another client. An active turn fails, queued wakes are dropped.                             |
| `fs`, `exec`             | Ignored: they serve the browser's file sync.                                                                                |

Interrupt is `POST cancel`; the cell confirms with a cancelled `done`, and the
adapter waits for that before reporting the turn interrupted. A `done` that
beats the cancel settles the turn as completed. Stop pressed before the prompt
registered waits behind the prompt: start and interrupt share one permit, so
the cancel goes out only once the cell has said which run it would cancel.
Steering is by interrupt and restart, which the orchestrator implements.
Queued messages wait in T3; the cell refuses a second prompt with 409.

## Wakes

When a delegated child finishes while the cell is idle, the cell runs its
agent loop on its own: a `wake` event, then the run's events, then `done`.
Each such run is a `PendingWake` in an ordered queue, offered to the
orchestrator as a continuation with its own message id (derived from the
project and run id, or from a per-session counter without run ids). The
continuation turn the orchestrator dispatches for that message adopts exactly
that wake, by message id, so two children finishing in a row produce two
continuations that each carry their own output whichever order the
orchestrator starts them in. The match is exact: a continuation whose wake is
gone adopts nothing, completing empty when the orchestrator cleared the wake
and failing with a message that names the reset when a `reset` from another
client dropped it, so a stale continuation never consumes the next wake's
output. The wake's events are replayed into the turn, and the thread stays
pinned as having pending background work until the queue is empty. A
continuation the orchestrator clears (the instance was disabled, the thread
archived) drops its wake; the result still lives in the cell's transcript.

Rows the adapter publishes while no run is active, such as a task settling
between turns, are not persisted: the session manager only forwards adapter
events to a run's subscriber. The adapter keeps those records and publishes
them again, with the current roster, when the next turn starts, which is why
the subagent row of a child that finished while idle still settles in the run
that delegated it.

A `task` event can arrive after the turn that delegated the task settled, or
on a later connection. Updates are routed by task id to the subagent record
created when the task was first seen, never by whichever turn is active. A
task first seen with no turn active (started from the cell's own page) only
counts as pending background work; it gets no item in any run.

## Identity

The cell has no item ids. Per
[entity ids and correlation](../orchestration-v2/entity-ids-and-correlation.md)
the adapter mints stable app ids by scoped ordinal and says so in the identity
capabilities: thread ids are strong (the project name is a durable identity),
turn ids are weak (the project plus the provider turn ordinal; the cell's run
id is known only after the prompt is accepted), item and request ids are
none (allocated by ordinal within the turn). Item ids are derived from
`instance:providerThread:turn:ordinal`, so replaying the same transcript
produces the same ids.

## Runtime modes and policy

The cell is the sandbox: nothing the agent runs leaves its isolate, there is
no network from the shell, and there are no approvals to ask for. The
presentation offers only `full-access` and the capabilities declare native
enforcement, which means the adapter must not run a thread under a mode the
cell does not enforce. The server's policy layer maps a mode the provider does
not offer to Supervised, so a thread still set to another mode reaches the
adapter as `approval-required`; the adapter refuses it everywhere a policy is
passed: opening the session and attaching the thread fail with
`CellRuntimeModeError`, whose message the thread shows, and a turn that slips
through fails with a `permission_error` terminal. The user switches the thread
to Full access and sends again; nothing runs silently under a wider mode than
the thread asked for.

## Model catalog and status

`GET state` reports the cell's one model, so the catalog has exactly that
entry, marked default. Custom models are not a setting for this driver, and
`providerSupportsCustomModels` in contracts tells the clients not to offer
them. The adapter reads `GET state` when it attaches a thread and refuses any
selection other than T3's `default` sentinel or that model with
`CellModelError`; a turn that arrives with another model fails as a
validation error. The status probe reads `GET state` of a dedicated probe
project (`<prefix>probe`) and never posts a prompt, so a background refresh
cannot start a run, spend tokens, or create work. The probe creates one empty
Durable Object per tenant; that is the cost of a probe without a dedicated
health endpoint. `configured: false` is a warning (the tenant has no model API
key); an unreachable origin or a non-JSON answer is an error. There is no
version advisory beyond the manifest row: the cell reports its own `version`,
and nothing on the environment updates it.

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
while the session is open. Connection boundaries and frames go through one
queue in the order the socket produced them, so a `done` read just before a
close is processed before the reconnect, never skipped because the producer
already moved on. A drop after a connection that stayed up for thirty seconds
reconnects at once; connections that keep dying young back off, from half a
second to ten, so a proxy that accepts and closes cannot drive a tight loop.

On reconnect the cell sends `state` and `tasks` again, and the adapter
reconciles everything it was tracking against them:

- With sequence numbers, a `state.seq` ahead of the last event seen means the
  gap is read back with `GET events?after=<seq>` before anything is judged;
  duplicates are dropped by sequence number. A turn whose `done` was in the
  gap ends the way the cell ended it. The replay is applied only up to
  `state.seq`, the moment the snapshots describe: a page that runs past it is
  held, in order, until the `tasks` roster has been reconciled, and applied
  after it, so a task the roster dropped settles instead of being revived by
  a replayed row, and a task created after the snapshot is not cancelled by
  it. Live copies of held events are dropped by their numbers.
- The active turn survives only if the cell is running the run id the turn
  owns. Any other answer, `running: false`, another run id, or no run ids at
  all, fails the turn as a transport error whose message says the result
  stayed in the cell, and leaves the thread reusable, because the next prompt
  continues the same transcript. Without run ids `running: true` is not
  proof: another client may have started a run in the gap, and adopting it
  would be the misattribution the ownership rules exist to prevent.
- A turn still waiting for its prompt's answer records what the `state` said
  and is judged by the same rule once the 202, or the echo, says which run is
  T3's. A run that began before the gap (the 202's sequence number is at or
  below `state.seq`, or the echo was among the candidates before the gap) is
  alive only if the cell was running it; otherwise the turn fails as a
  transport error, which on a tenant without run ids is the outcome whenever
  the echo preceded the gap, since nothing can prove the run survived. A run
  that began after the gap lost nothing to it.
- A queued wake whose run is not the one the cell is running is marked lost;
  its continuation turn ends with a transport-error terminal instead of
  running forever on an incomplete buffer.
- The task roster is the snapshot, taken at the moment of connection. A task
  it no longer lists was removed while disconnected, so it is settled as
  cancelled if it was running and dropped either way, and the thread's
  pending background work follows. A roster that carries a sequence number
  is applied at that number, between the replayed events before and after
  it. One that carries none is authoritative at `state.seq`: task rows the
  replay found at or before that moment are superseded by it and never
  applied, while the gap's run events, which the roster cannot contradict,
  are applied before it.

## Failures

A failed `done` becomes a failed terminal with the class the cell's error code
names: `usage_limit` (with `resetAt` when the cell supplies it),
`transport_error`, or `provider_error` for everything else including tool
errors. A plain-string error, which is all the current tenant sends, is
classified by its words. `provider_error` is also the class for a prompt the
cell refused; `transport_error` is for what T3 could not observe; a turn the
adapter stopped itself for policy reasons is `permission_error` or
`validation_error`.

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
- **Attributing by socket order alone.** Before run ids, the first design
  installed the turn as the owner of every broadcast as soon as it posted the
  prompt. Another client's run ending in the window before the 409 completed
  the rejected prompt with that client's output. Nothing is owned until the
  cell says so.
- **Replaying the gap over the socket.** The cell can also replay missed
  events on connect with `ws?after=<seq>`. The adapter uses the HTTP log
  instead so the recovery is one request with a clear end, inside the
  handling of the `state` that revealed the gap, and so a recorded socket
  connect looks the same with or without a gap.

## Testing

Replay fixtures for `simple`, `multi_turn`, `queued_turn`, `turn_interrupt`,
`provider_thread_resume` and the cell-only `cell_wake` run the real
orchestrator, adapter and projections over a scripted transport that replaces
only the HTTP and WebSocket boundary; `cell_wake` runs the continuation
worker too, so the wake reaches the thread the way it does in production. The
transcript protocol is `cell.http-ws`: outbound frames are the HTTP requests
and WebSocket connects the adapter makes, inbound frames are the responses,
transport failures and socket events. Replay binds `<project>` to the first
project it sees, so a request to a second project fails the replay instead of
matching. Socket events are labelled `ws:<type>#<n>` so a fixture step can
hold the n-th event of a type behind a replay gate; the recorder honours the
same gate, so a recording is paced the way its replay will be. The recorder in
`apps/server/scripts/record-cell-replay-fixture.ts` records against a live
tenant (`--url`) or against the fake cell in
`@t3tools/provider-cell/testing`, a Node HTTP and WebSocket server that speaks
either protocol generation and answers the shared fixture prompts the way the
cell does. The checked-in transcripts were recorded against the fake cell
speaking the newer protocol; record them against a tenant once one speaks it.
The adapter's own tests cover the rules above on both generations: the
rejected prompt racing another run's events, a 202 overtaking the previous
run's tail, two wakes, a wake buffered behind the accepted run's `done`, a
cleared or reset-dropped continuation adopting nothing, a wake lost or
recovered across a reconnect, a reconnect while the prompt was unanswered,
judged once the run is named, replay held behind a numbered and an unnumbered
roster, the FIFO drain, roster reconciliation, Stop before and after the
prompt registered, a cancel racing `done`, a lost prompt response, session
resume, shutdown, a disabled instance, refused runtime modes and models, usage
limits, and the reconnect backoff. The reconnect races run over a hand-driven
socket, since a transcript walk cannot hold a prompt's answer until after the
frames that race it.

## Follow-ups

- A workspace kind without a local checkout, so cell threads stop borrowing a
  local project and the local-only affordances disappear instead of sitting
  inert; the capability flag for it already exists.
- Attaching a thread to an existing cell project from the UI, through the
  native-session listing, once the tenant can list projects.
- A display name on the tenant, once the cell has an API for it.
- Attachments through `PUT fs/file`, with the prompt naming the paths.
- Fixtures recorded against a tenant that speaks the newer protocol.
