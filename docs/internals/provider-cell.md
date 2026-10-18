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

The adapter speaks cell protocol level 3 and nothing older. Both sides of the
protocol are ours and nothing is deployed, so there is no compatibility to
keep: supporting the earlier generations meant a second set of ownership
rules for tenants without run ids or sequence numbers, and that is where the
mistakes lived. `GET state` reports `protocol`; a tenant that reports a lower
level, or none, is refused before anything is sent to it. The status probe
shows the refusal as an error naming the level the tenant speaks and the one
this provider needs, and attaching a thread to such a tenant fails with
`CellProtocolError`, so no turn ever posts a prompt to it.
[`protocol.ts`](../../packages/provider-cell/src/server/protocol.ts) holds the
shapes. What level 3 guarantees, and what the adapter builds on:

- **Every event is numbered.** `seq` is a per-cell monotonic integer the cell
  persists. Run-scoped events (`user`, `wake`, `assistant`, `shell`,
  `shell_output`, `tool`, `tool_output`, `patch`, `error`, `done`) carry the
  `runId` of the agent run they belong to; `user` and `wake` say whether the
  run was started by a user or by a child finishing (`origin`); `task` and
  `tasks` are numbered like everything else.
- **The 202 names the run.** `POST prompt` answers `{ runId, seq }`, where
  `seq` is the number of the run's opening `user` event. A 409 answers
  `{ runId }` naming the run already in progress.
- **Every connection opens the same way.** The socket sends `state` (the same
  fields as `GET state`, so `running`, `runId` and `seq` at that moment), then
  `tasks`, the full roster numbered at that same `seq`, then events. The
  roster therefore always arrives before any replayed or live event, and it
  describes the cell exactly at the number `state` gave.
- **A gap can be read back.** `GET events?after=<seq>&limit=<n>` (n ≤ 1000)
  answers `{ events, nextAfter, complete }`, ordered by number; `complete`
  says the log has nothing after `nextAfter`. The cell keeps the last 20,000
  events or 200 runs, whichever is smaller, and a request below that window
  answers `complete: false` with `truncated: true` and the earliest number it
  still holds.
- **`done` is structured.** A failed run's `done` carries
  `{ code, status?, message, resetAt? }` with `code` one of `cancelled`,
  `usage_limit`, `provider_error`, `transport_error` or `tool_error`; a
  successful run's `done` carries no error.

`GET state` is read once per session when the thread is attached, which is
where the level is checked and the tenant's model learned, and again to
recover a lost prompt reply. Events the cell adds after this build are
ignored rather than failing the socket, so this T3 keeps working against a
newer tenant.

## Which events are T3's

Nothing a run broadcasts is attributed to a T3 turn until the cell has said
which run is T3's, and the only thing that says so is the 202 to T3's own
prompt. While `POST prompt` is unanswered, run-scoped events are candidates
held in the turn. The 202 settles them: the candidates are put through the
same ownership rule as live events, in sequence order. That rule is one line:
an event is the turn's exactly when it carries the run id the 202 named. An
event of another run numbered below the run's opening event (the 202's `seq`)
is an earlier run's tail that the 202 overtook on the way to T3, nobody's and
no evidence about T3's run. An event of another run numbered after it proves
T3's run ended without its `done` reaching T3, since the cell runs one agent
loop at a time, and the turn fails as a transport error rather than adopting
the stranger; that holds whether the stranger arrived live or was among the
candidates, so a wake buffered behind T3's run while the prompt was unanswered
ends the turn exactly as it would have live, and is queued as a wake. A 409
releases every candidate to the unowned path and fails the turn with a message
that another client is running the cell, so a foreign run's `assistant` and
`done` racing ahead of the refusal can never complete a prompt the cell
rejected.

A prompt whose answer never arrives is not failed on the spot. The run's
opening `user` event, numbered after the prompt went out, user-originated and
carrying the prompt's text, names the run and its number, which is what the
202 would have said. It is looked for among the candidates, and failing that
in the event log read from the last number applied up to what `GET state`
reports; with the cell idle and no such event, the message most likely never
started and the turn says so; with the cell running some other run, the turn
fails with a message that says what T3 could not confirm.

| Cell event               | T3 effect                                                                                                                                      |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `state`                  | First on every connection: where the cell is, by number, and which run it is running. On a reconnect, drives the reconciliation below.         |
| `tasks`                  | Second on every connection, numbered at `state.seq`: the roster at that moment. Tasks it no longer lists are settled as cancelled and dropped. |
| `user`                   | Ignored: T3 already recorded the prompt. Its number is where the run begins, which the 202 reports.                                            |
| `assistant`              | A completed assistant message item. No streaming.                                                                                              |
| `shell` / `shell_output` | A `command_execution` item, running then completed with stdout and stderr joined and the exit code.                                            |
| `tool` / `tool_output`   | A `dynamic_tool` item for the orchestration tools (`delegate_task` and friends), with input and output.                                        |
| `patch`                  | A completed `file_change` item per changed path. The cell sends no diff, so the item has a name and operation.                                 |
| `task`                   | Subagent lifecycle: `subagent.updated`, a `subagent` node and a `subagent` item keyed by the task id.                                          |
| `wake`                   | A cell-initiated run, queued and offered as a continuation; inside T3's own run, a notice item.                                                |
| `error`                  | A `system_notice` item during a turn (an apply_patch failure, for example); the turn goes on.                                                  |
| `done`                   | Turn completed; cancelled or interrupted when the error is a cancellation; otherwise failed, classified.                                       |
| `reset`                  | The conversation was reset from another client. An active turn fails, queued wakes are dropped.                                                |
| `fs`, `exec`             | Ignored: they serve the browser's file sync.                                                                                                   |

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
orchestrator as a continuation with its own message id, derived from the
project and the run id. The
continuation turn the orchestrator dispatches for that message adopts exactly
that wake, by message id, so two children finishing in a row produce two
continuations that each carry their own output whichever order the
orchestrator starts them in. The match is exact: a continuation whose wake is
gone adopts nothing, completing empty when the orchestrator cleared the wake
and failing with a message that names the reset when a `reset` from another
client dropped it, so a stale continuation never consumes the next wake's
output. The reset marker is kept only for a wake whose continuation the
orchestrator had already dispatched, since only such a turn will come asking
for it, and it leaves when that turn starts or the orchestrator clears the
request; a wake dropped before it was dispatched leaves nothing behind. The
wake's events are replayed into the turn, and the thread stays pinned as
having pending background work until the queue is empty. A continuation the
orchestrator clears (the instance was disabled, the thread archived) drops
its wake; the result still lives in the cell's transcript.

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

Every connection is handled in phases that follow the protocol's order:
`state` first, then the `tasks` roster, then events. Nothing is applied
before the roster. A connection that sends anything else before its roster
is not keeping the protocol and is dropped and reopened rather than applied
around, and one that sends no roster within ten seconds of its `state` is
dropped the same way; the first connection of a session that fails this way
fails the thread's attachment with the reason. Once the roster is in, the
adapter reconciles what it was tracking against the snapshot:

- The gap is read back first. A `state.seq` ahead of the last number applied
  means events were missed, and `GET events` reads them back a page at a
  time, each page applied in sequence order before the next is fetched, so
  no more than one page is ever held. The replay stops at `state.seq`; what a
  page carries beyond it belongs to the socket, which delivers everything
  after its snapshot live, and a live copy of a replayed event is dropped by
  its number. A turn whose `done` was in the gap ends the way the cell ended
  it, and a task row from the gap is observed with the turn that was running
  when the cell sent it, so the subagent row lands in the run that delegated.
  A page the cell cannot serve (a transport failure, a bad answer) drops the
  connection; the next one presents the rest of the gap again from where the
  replay stopped.
- The roster is applied at its own number, after the replay up to it. It is
  the whole roster: a task it no longer lists was removed while disconnected,
  so it is settled as cancelled if it was running and dropped either way, and
  the thread's pending background work follows. A task it lists that nothing
  created is recorded with no owner.
- A gap the cell no longer retains (`truncated`) cannot be read back, and the
  active turn fails as a transport error naming the numbers that were lost;
  the result stays in the cell and the thread stays reusable.
- With the gap read back in full, the active turn is judged against what the
  cell said it was running at the snapshot. A `done` in the gap has already
  ended the turn by now, so a turn still open whose run the cell is not
  running ended without saying how; it fails as a transport error with that
  message. A turn still waiting for its prompt's answer records the snapshot
  and is judged by the same rule once the 202 names the run, after its
  candidates have been settled: a run that began at or before the snapshot
  and was not running at it ended unobserved; one that began after it lost
  nothing to the gap.
- A queued wake whose run is not the one the cell is running, and whose `done`
  the replay did not supply, is marked lost; its continuation turn ends with a
  transport-error terminal instead of running forever on an incomplete buffer.

## Failures

A failed `done` becomes a failed terminal with the class the cell's error code
names: `usage_limit` (with `resetAt` when the cell supplies it),
`transport_error`, or `provider_error` for everything else including tool
errors. `provider_error` is also the class for a prompt the cell refused;
`transport_error` is for what T3 could not observe, a gap it could not read
back, or a 202 that did not name the run; a turn the adapter stopped itself
for policy reasons is `permission_error` or `validation_error`.

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
  instead so the recovery is paged with a clear end, inside the handling of
  the connection whose snapshot revealed the gap, and so a recorded socket
  connect looks the same with or without a gap.
- **Keeping the earlier protocol generations working.** The first rounds of
  the adapter read run ids and numbers when present and fell back to the
  prompt's echo and arrival order when absent, with a separate judgement for
  reconnects on such tenants. Every reconnect finding of the third review
  lived in that second set of rules or in the seams between the two. Nothing
  is deployed and both sides are ours, so the adapter requires level 3 and
  the fallbacks are gone.

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
level 3 (numbered, persisted events; `state` then the numbered roster on
every socket; a paged event log with the protocol's retention; structured
`done` errors) and answers the shared fixture prompts the way the cell does.
The checked-in transcripts were recorded against the fake cell; record them
against a tenant once celld speaks level 3.

The adapter's own tests cover the rules above: the rejected prompt racing
another run's events, a 202 overtaking the previous run's tail, a stranger
after the run began ending the turn whether it arrived live or among the
candidates, two wakes, a wake buffered behind the accepted run's `done`, a
cleared or reset-dropped continuation adopting nothing and the reset marker
kept only for a dispatched one, a wake lost or recovered across a reconnect,
a reconnect between the prompt's opening event and its 202, a reconnect while
the prompt was unanswered judged once the run is named, a task row from the
gap landing in the run that delegated, a roster that never comes and a frame
sent before it both dropping the connection, the gap read back page by page,
a gap the cell no longer retains, the FIFO drain, roster reconciliation, Stop
before and after the prompt registered, a cancel racing `done`, a lost prompt
response, session resume, shutdown, a disabled instance, refused runtime
modes, models and protocol levels, usage limits, and the reconnect backoff.
The reconnect races run over a hand-driven socket, since a transcript walk
cannot hold a prompt's answer until after the frames that race it; the ones
that need the roster timeout and the backoff run on the test clock.

## Follow-ups

- A workspace kind without a local checkout, so cell threads stop borrowing a
  local project and the local-only affordances disappear instead of sitting
  inert; the capability flag for it already exists.
- Attaching a thread to an existing cell project from the UI, through the
  native-session listing, once the tenant can list projects.
- A display name on the tenant, once the cell has an API for it.
- Attachments through `PUT fs/file`, with the prompt naming the paths.
- Fixtures recorded against a tenant, once celld speaks level 3.
