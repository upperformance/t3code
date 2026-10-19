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
  removes them there. The deletion does settle what the thread still showed
  as running in the cell: the rows of a delegated task still open (the
  subagent, its node and its item) are cancelled, the thread's task roster is
  cleared, and the cell is asked, best effort, to cancel each task it still
  runs for the thread (`POST tasks/<id>/cancel`, the session runtime's
  `cancelBackgroundWork`, called from the session detach the deletion
  queues) when the thread's session is open at the time. A task the cell goes
  on running after that, because no session was open or the cell refused,
  keeps its result in the cell's transcript only. Attaching a thread to a cell project that already
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

- **Every event is numbered.** `seq` is a per-cell counter the cell
  persists: a non-negative integer that only grows, through resets and
  destroys too. Run-scoped events (`user`, `wake`, `assistant`, `shell`,
  `shell_output`, `tool`, `tool_output`, `patch`, `done`, and an `error`
  raised inside a run) carry the `runId` of the agent run they belong to;
  `user` and `wake` say whether the run was started by a user or by a child
  finishing (`origin`); `task` and `tasks` are numbered like everything else.
  An `error` outside any run (a cleanup that failed, a report to a parent
  that could not be delivered, a root repaired by a reset) is standalone: it
  carries `seq` and no `runId`. What the cell answers a client's own socket
  message (an `exec` result, a refused socket prompt) is a reply, not an
  event: unnumbered, sent only to that client, never kept. The adapter does
  not model replies, since T3 sends no socket commands; an `exec` frame is an
  unknown type and ignored, and an unnumbered `error` would be an unreadable
  event, handled as below.
- **A prompt carries a request id.** `POST prompt` takes an optional
  `x-request-id` header, an opaque string of at most 128 characters. The cell
  keeps it on the run it starts: the run's opening `user` event and `GET
state` carry it as `requestId`, null when the run has none. A prompt
  repeated with the request id of the run in progress, or of the run that
  ended last, answers 202 with that run's `{ runId, seq }` instead of starting
  another run, so a client whose answer was lost can ask again safely. The
  adapter mints a fresh UUID for every turn, sends it with the prompt and
  keeps it with the turn.
- **The 202 names the run.** `POST prompt` answers `{ runId, seq }`, where
  `seq` is the number of the run's opening `user` event. Every other answer
  is `{ runId, error }`: a different request id while a run is in progress
  answers 409 with `runId` naming that run; an empty prompt, a tenant without
  a model API key and a cell busy with a fork or a reset answer 409 with
  `runId: null` and the reason in `error`.
- **A cancel names its run.** `POST cancel` takes an optional `x-run-id`
  header. When the run in progress is another one, the cell answers 409
  `{ runId }` naming it and cancels nothing; with no run in progress it
  answers 204 and does nothing; without the header it cancels whatever runs.
  The adapter always sends the header.
- **The cell names itself.** `GET state` and the `state` frame carry `name`,
  `<tenant>/<project>`, and `misnamed`: null, or text saying the runtime's and
  the stored names disagreed when the cell was activated. The cell also
  records a disagreement once, as a numbered standalone `error` of kind
  `identity`. The adapter reports a misnaming as a warning in the server log
  naming the thread and the cell, once per session by its text: every
  connection's `state` repeats it, the recorded error may be read back from
  the log or arrive live, and none of those says anything new.
- **Every connection opens the same way.** The socket sends `state` (the same
  fields as `GET state`, so `running`, `runId`, `requestId`, `name`,
  `misnamed` and `seq` at that moment), then `tasks`, the full roster
  numbered at exactly that `seq`, then
  events. The roster therefore always arrives before any replayed or live
  event, and it describes the cell exactly at the number `state` gave.
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
where the level is checked, the tenant's model learned and a misnaming
reported, and again to recover a lost prompt reply. Events the cell adds after this build are
ignored rather than failing the socket, so this T3 keeps working against a
newer tenant. What the cell does send of the protocol is validated: a `seq`
that is not a non-negative integer, a 202 whose `seq` is not one, a roster
numbered elsewhere than its `state`, or a known event that cannot be decoded
is a violation like a frame out of order. Live, it drops the connection, and
the next one reads the gap back from the log; in the log, such an entry is
skipped with a warning, since the log is the only copy and dropping the
connection would present the same entry again.

## Which events are T3's

Nothing a run broadcasts is attributed to a T3 turn until the cell has said
which run is T3's. Two things say so: the 202 to T3's own prompt, and, when
that answer was lost, the request id T3 sent with the prompt found on the
run, as described below. While `POST prompt` is unanswered, run-scoped events
are candidates held in the turn. The 202 settles them: the candidates are put
through the same ownership rule as live events, in sequence order. That rule
is one line: an event is the turn's exactly when it carries the run id the
202 named. An event of another run numbered below the run's opening event
(the 202's `seq`) is an earlier run's tail that the 202 overtook on the way
to T3, nobody's and no evidence about T3's run. A 202 naming a run that began
at or before the snapshot T3 follows the log from, which the cell was not
running at that snapshot, names a run that had ended before T3 could see it:
the cell answers a repeated request id with the run it started for it, so
such an acceptance means the id T3 sent was one the cell had seen (the
deterministic test runtime mints the same ids every run, so a recorder rerun
against a kept cell sends one), and the turn fails at once naming the stale
acceptance instead of waiting for a `done` that was never on the socket. A
202 naming the run the cell was running at that snapshot keeps the turn,
which shows the rest of the run. An event of another run
numbered after it proves T3's run ended without its `done` reaching T3,
since the cell runs one agent loop at a time, and the turn fails as a
transport error rather than adopting the stranger; that holds whether the
stranger arrived live or was among the candidates, so a wake buffered behind
T3's run while the prompt was unanswered ends the turn exactly as it would
have live, and is queued as a wake. A 409 releases every candidate to the
unowned path and fails the turn: with a message that another client is
running the cell when the refusal names a run, with the cell's reason when it
names none. So a foreign run's `assistant` and `done` racing ahead of the
refusal can never complete a prompt the cell rejected.

A prompt whose answer never arrives is not failed on the spot, and it is not
recovered by its text either: another client may have sent the same words
first, and with T3's 409 lost, matching the opening event by text would hand
T3 that client's answer. The only evidence is the request id T3 sent with
the prompt. The run's opening `user` event carries it, and that event names
the run and its number, which is what the 202 would have said; it is looked
for among the candidates, and failing that in the event log read from the
last number applied up to what `GET state` reports. Failing that too, `GET
state` naming T3's request id on the run in progress makes that run T3's
from the number the state reported, so that nothing numbered before it is
taken as evidence about the run. Without any of these the turn fails as a
transport error: with the cell idle, the message most likely never started
and the turn says to send it again; with the cell running something else,
the turn says T3 cannot tell whether the message ran and that its result, if
any, stays in the cell. An answer T3 cannot trust counts as no answer. The
cell's refusal has one shape: JSON with `runId` present, a string naming the
run in progress for the 409 conflict, or `null` together with a string
`error` for every other refusal (an empty prompt, no API key, a cell busy
with a fork or reset). A server error (5xx) whose body is exactly that is the
cell's own and fails the turn with its reason; one with any other body, a
proxy's HTML page, a gateway's `{ "error": ... }` without `runId`, `{ "runId":
null }` alone, an empty or unreadable body, came from something between T3
and the cell, or from a cell that may already have started the run, and goes
through the same recovery by request id. A status below 500 without the
refusal shape fails the turn quoting the body, as a provider error when the
tenant rejected the token (401, 403) and as a transport error otherwise.

| Cell event               | T3 effect                                                                                                                                                                                                                                                |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `state`                  | First on every connection: where the cell is, by number, which run it is running, and its name. A `misnamed` text is logged once per session. On a reconnect, drives the reconciliation below.                                                           |
| `tasks`                  | Second on every connection, numbered at `state.seq`: the roster at that moment. Tasks it no longer lists are settled as cancelled and dropped.                                                                                                           |
| `user`                   | Ignored: T3 already recorded the prompt. Its number is where the run begins, which the 202 reports.                                                                                                                                                      |
| `assistant`              | A completed assistant message item. No streaming.                                                                                                                                                                                                        |
| `shell` / `shell_output` | A `command_execution` item, running then completed with stdout and stderr joined and the exit code.                                                                                                                                                      |
| `tool` / `tool_output`   | A `dynamic_tool` item for the orchestration tools (`delegate_task` and friends), with input and output.                                                                                                                                                  |
| `patch`                  | A completed `file_change` item per changed path. The cell sends no diff, so the item has a name and operation.                                                                                                                                           |
| `task`                   | Subagent lifecycle: `subagent.updated`, a `subagent` node and a `subagent` item keyed by the task id.                                                                                                                                                    |
| `wake`                   | A cell-initiated run, queued and offered as a continuation; inside T3's own run, a notice item.                                                                                                                                                          |
| `error`                  | Naming a run: a `system_notice` item of that run's turn (an apply_patch failure, for example); the turn goes on. Standalone: a warning in the log, assigned to no run; one of kind `identity` is the misnaming `state` reports, logged once per session. |
| `done`                   | Turn completed; cancelled or interrupted when the error is a cancellation; otherwise failed, classified.                                                                                                                                                 |
| `reset`                  | The conversation was reset from another client. An active turn fails, queued wakes are dropped.                                                                                                                                                          |
| `fs`                     | Ignored: it serves the browser's file sync.                                                                                                                                                                                                              |

Interrupt is `POST cancel` naming the turn's run in `x-run-id`; the cell
confirms with a cancelled `done`, and the adapter waits for that before
reporting the turn interrupted. A `done` that beats the cancel settles the
turn as completed. Stop pressed before the prompt registered waits behind the
prompt: start and interrupt share one permit, so the cancel goes out only
once the cell has said which run it would cancel. Which turn is active is
decided under the event permit, behind whatever reconciliation is in
progress: a reconnect's replay that carries the turn's `done` ends the turn
first, and Stop then finds a settled turn with nothing left to stop and posts
no cancel, so the run the cell started next is left alone. A cancel that does
reach the cell after the run ended is refused with 409 naming the run in
progress, which the adapter reads as the turn's run having ended: it cancels
nothing and the turn's `done`, live or read back, settles the turn. Steering
is by interrupt and restart, which the orchestrator implements. Queued
messages wait in T3; the cell refuses a second prompt with 409.

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
having pending background work until the queue is empty. A wake that lost
events to a gap the cell no longer retains ends its continuation as a
transport error even when its `done` was read, since what the buffer holds
is not the whole run; its events before and after the gap are still shown.
A continuation the orchestrator clears (the instance was disabled, the thread
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
  A page the cell cannot serve (a transport failure, a bad answer), a page
  that moves the cursor nowhere, or a log that claims to be complete before
  the number the snapshot itself reported, fails the recovery: the cursor
  stays where the replay stopped, nothing of the snapshot is applied, and the
  connection is dropped; the next one presents the rest of the gap again.
- A snapshot numbered below the last number applied is not the history T3
  was following: the cell's counter never regresses. Nothing carries over.
  The active turn fails as a transport error naming the numbers, queued wakes
  are marked lost, and the adapter attaches afresh at the new snapshot, as on
  a first connection, so the next turn follows the new numbering from there.
- The roster is applied at its own number, after the replay up to it. It is
  the whole roster: a task it no longer lists was removed while disconnected,
  so it is settled as cancelled if it was running and dropped either way, and
  the thread's pending background work follows. A task it lists that nothing
  created is recorded with no owner.
- A gap the cell no longer retains (`truncated`) cannot be read back, and the
  active turn fails as a transport error naming the numbers that were lost;
  the result stays in the cell and the thread stays reusable. Every queued
  wake still open across the gap is marked lost, whether or not the cell is
  still running it, since what the socket delivers after the snapshot is not
  the whole run.
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
- A thread attaches at the first snapshot to complete since it was attached,
  on whichever connection delivers it: a connection that ends after `state`
  and before its roster, or that could not be opened, leaves the next one to
  attach, and the persisted head of a run to take back is read from on that
  connection. Three connections in a row that end before their snapshot
  could be used fail the attach naming the last reason, inside the attach's
  own timeout. A failed attach leaves nothing behind, so the same session
  attaches on its next try from that try's first snapshot.
- Recovery is bounded. Connections dropped for not keeping the protocol (a
  frame before the roster, no roster within ten seconds of `state`, a roster
  numbered elsewhere than its `state`, an unreadable event, a replay that
  failed) are counted; the third in a row fails the active turn as a
  transport error naming the reason, so a turn never waits for ever on a
  tenant that keeps answering wrongly. The socket goes on reconnecting with
  its backoff for the idle thread, and a connection whose snapshot is applied
  resets the count.

## Restarts

The active turn lives in the adapter's memory, and a session ends with the
cell still running its run whenever T3 stops mid-turn: the cell does not
notice. So that the next session can take the run back, the thread's
persisted head names it while it is T3's. The head is
`nativeConversationHeadRef` on the provider thread, the record the
orchestrator stores from every `provider_thread.updated` and hands back to
`resumeThread` and `ensureThread`, and the one Codex carries across a resume
and Pi uses for its session leaf; `OrchestrationV2ProviderSession` has no
field of its own for adapter state, and `nativeMetadata` is a closed struct.
While a run is T3's the head is a strong ref: `nativeId` is the cell's run
id, `fingerprint` the `x-request-id` the prompt was posted with, `ordinal`
the number up to which the run's output is applied. It is written when the
202, the request id or a wake's adoption names the run, and again after every
event of the run applied, so that it always says where T3 left off; when the
turn ends in any way but the session closing it is replaced by the weak ref
to the finished turn that was already written on completion. A closing
session leaves the strong head in place: that run is what the next session
takes back.

The number is the output cursor, never the receive cursor. The cell
broadcasts a run's first events before it answers the prompt, so by the time
the 202 names the run the adapter has received, and numbered past, output it
has not yet applied: those events waited as candidates. The first head
therefore names the number before the earliest candidate of the run (the
opener's number less one when the opener was received, the accepted number
less one otherwise), and each head after it names the event just applied,
emitted behind that event's items in the same ordered stream. The
orchestrator persists both in order, so wherever a session ends, the stored
head names a point below which everything was shown and above which the next
session reads the log back. Adoption of a queued run follows the same rule:
the head starts at the number the queue started the run at (a wake's opener,
already consumed, or the persisted number) and follows each buffered event
replayed.

A session that resumes a thread whose head is strong queues the run it names
like a wake, with the persisted number as where the last session left off,
and handles its first connection as a reconnect rather than a fresh
attachment: the log is read back from that number to the snapshot, so the
run's events since, and its `done` if it ended, land in the queued entry; the
roster is applied at its own number; and the entry is judged as a wake is at
a reconnect, as still running at the snapshot, as ended by a `done` read
back, or as lost when the cell is not running it and no `done` was read, when
the gap is one the cell no longer retains, or when the numbering regressed. A
replay that fails on this first connection does not drop it, since the thread
must still attach: the run is marked lost and the adapter attaches at the
snapshot. Nothing is sent to the cell before this.

A `reset` read back in that gap discards the run: the cell's conversation,
and with it the run, is gone, and the run's `done` will never come. The
queued entry is not dropped with the other wakes the reset clears but kept as
a tombstone, ended and marked reset, so the turn that comes to take the run
back finds it. Dropping it would leave the restart continuation nothing to
adopt, and its text, a note that T3 restarted, would go to the cell as a new
prompt.

T3's own restart continuation, the message the server dispatches with
`restartContinuationOfRunId` after a restart, adopts the queued run instead
of posting its text: the turn owns the run from the persisted number, replays
what was buffered, and either waits for the live `done` or settles from the
`done` the log supplied; a lost run fails the turn as a transport error
saying how it ended could not be read back, a reset one fails it as a
transport error naming the reset after what the cell did for it before, and
the head follows the turn's end either way. A restart continuation that finds
nothing restored posts its text as a prompt, like any other message. When no
restart continuation comes, the next turn that starts offers the queued run
to the orchestrator as a continuation, exactly as a wake is offered, so its
output follows that turn; the offer waits for that turn because an offer made
at resume time would queue a second turn next to a restart continuation that
is about to adopt the same run. A message typed while the cell still runs the
restored run is refused by the cell with 409 naming it, and the adapter,
recognising its own run, says the turn in progress when T3 last stopped is
still running rather than that another client runs the cell.

Whether a restart continuation comes is the orchestrator's decision, not the
adapter's. It comes when the project has `continueThreadsAfterServerUpdate`
on and the thread is not archived, snoozed or settled, and when the
orchestrator's own restart recovery leaves the run adoptable. That recovery
records, for every provider, the background work a restart cut off (native
subagents of the cut run, the provider thread's pending task roster) and
tells the continuation about it in a note that is prompted as text, dropping
`restartContinuationOfRunId` on the way, because for a local provider that
work is gone and the agent must restart it. For a cell nothing is cut off:
the run and its delegated tasks live on the tenant. The adapter says so with
`sessions.backgroundWorkSurvivesRestart`, and recovery then records none of
the cell thread's work as cancelled, so no note is written, the continuation
keeps its `restartContinuationOfRunId`, and the adapter adopts. A session
record stored by a build from before the capability lacks the field; recovery
then asks the adapter's own capabilities through the registry, so the first
restart after an upgrade treats the thread the same way; a lookup that fails
or throws is read as work that does not survive, and the thread's recovery
still commits. A note that older build already wrote on the cut run is
withdrawn when recovery visits the thread, which it does only while the
thread still has something to recover (a run not yet terminal, a session
still recorded live): a thread whose run that build already cancelled, with
the thread idle and the roster cleared, is not visited, and its note stands
in the stored run. The note is harmless there: the turn start ignores a note
on a provider whose work survives, so it is neither prompted nor strips the
continuation's `restartContinuationOfRunId`, and the rows that build
cancelled are not reopened.

A delegated task survives with its rows. Recovery terminalizes the cut run's
own rows but leaves a provider-native subagent's rows (the subagent, its node
and its item) and the thread's task roster open, since the task runs on and
the thread is still waiting on it. The next turn hands the open subagent
items to the adapter with the thread (`openNativeSubagentItems` on
`ensureThread` and `resumeThread`), keyed by the task id in `nativeItemRef`.
The first roster decides what became of each: a task the cell still lists
keeps the rows it was handed, and the first turn of the session to publish
it, the restart continuation when one comes, takes them over as its own, so
the task's end, live or from the roster, lands on the subagent row that
showed it start, with its item's ordinal and start time kept; a task the cell
no longer lists ended while no session was open, and that turn settles its
rows as cancelled. A session closing leaves the rows alone for the same
reason: the task runs on, so the adapter no longer reports it cancelled at
close. The task's completion also wakes the cell, which the next session
receives as a wake if its socket is open then.

The bound on an open row is therefore the next turn on the thread. A task
the cell has in fact lost (its cell destroyed, a reset, a roster the cell no
longer lists it in) is settled at the first roster of the next attachment,
and a thread attaches when its next turn starts, so until someone sends a
message, or a wake or a restart continuation starts a turn, the rows stay
open; there is no elapsed-time bound, and an archived thread keeps its rows
until it is re-engaged. Deleting the thread settles them at once, as
described under _Thread_ above.

What restoration does not survive: a prompt whose 202 had not arrived when T3
stopped, a window of one round trip in which the head names no run yet, so
the next prompt is refused naming a run T3 cannot prove is its own; the
events emitted after the last head update the orchestrator persisted, which
a restoring turn shows again (at most the items of one event, since the head
follows every one); a cell recreated in the gap, whose run is reported lost,
and a `reset` in it, whose run is reported reset; the cell's own retention,
since a gap it no longer holds cannot be read back; and a wake that started
and ended while no session was open, since a weak head restores nothing and
the next session attaches at its first snapshot.

## Failures

A failed `done` becomes a failed terminal with the class the cell's error code
names: `usage_limit` (with `resetAt` when the cell supplies it),
`transport_error`, or `provider_error` for everything else including tool
errors. `provider_error` is also the class for a prompt the cell refused,
with the cell's reason; `transport_error` is for what T3 could not observe:
a lost prompt answer that no request id confirmed, a gap it could not read
back, a run the cell was no longer running, three dropped connections in a
row, a regressed numbering, a 202 that did not name the run, or one naming
a run that had ended before T3 attached. A turn the
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
- **Recovering a lost prompt answer by the prompt's text.** The fourth round
  still looked for the run's opening `user` event by the words of the prompt
  when the 202 was lost. Another client sending the same words first, with
  T3's 409 lost on the way back, completed T3's turn with that client's
  answer. A request id the cell keeps on the run is the only evidence that a
  run is T3's, and the protocol gained one.

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
matching, and `<request-id:n>` to the n-th distinct `x-request-id` the
adapter sends, so a transcript holds no minted id and a replayed opener,
state or log page carries exactly the ids the adapter is looking for, even
when one log page carries the openers of several turns; the recorder numbers
each distinct id in order of first use and writes its placeholder where the
cell echoed it. Socket events are labelled `ws:<type>#<n>` so a
fixture step can hold the n-th event of a type behind a replay gate; the
recorder honours the same gate, so a recording is paced the way its replay
will be. The recorder in `apps/server/scripts/record-cell-replay-fixture.ts`
records against a live tenant (`--url`) or against the fake cell in
`@t3tools/provider-cell/testing`, a Node HTTP and WebSocket server that speaks
level 3 (numbered, persisted events; `state` then the numbered roster on
every socket; a paged event log with the protocol's retention; structured
`done` errors; the request id kept on the run and answered again for a
repeat; refusals as `{ runId, error }`; a cancel that names its run; replies
to its own clients' socket prompts; standalone errors; a delegated task
announced as `starting` before the tool answers and `running` once the child
accepted its prompt, cancelled through `POST tasks/<id>/cancel`) and answers
the shared fixture prompts the way the cell does. The checked-in transcripts
were recorded against a live tenant, celld dev running the cell of
upperformance/vim `main` (the cell reports itself as version 0.3.0) with a
scripted Responses API behind it, and verified by replay; the fake cell is
for a recording without a tenant. The task's three states are why
`cell_wake` holds the third `task` event, the child's completion, behind its
gate: the first two are inside the delegating run. The restart itself is not a transcript:
`CellRestartAdoption.integration.test.ts` runs two orchestrator runtimes over
one database against the fake cell, stops the first the way the server stops
(worker halted, continuation prepared, sessions shut down, state reconciled)
while a turn with a delegated child is in progress, and checks that the
second takes the run back without a prompt, that no turn is told its
background work was cancelled, and that the child's result lands on the
subagent row the first server showed, which goes from running to completed
and stays the thread's only one. Its waits are driven by the thread's stored
events and bounded on the wall clock, and the orchestrator's test clock is
moved by the test alone, between the two servers.

The live run is not a test either, since it needs a running cell.
`apps/server/scripts/live-cell-driver.ts --scenario shell|interrupt|limit|delegate|reconnect|restart --url <cell> [--tag t] [--log dir]`
runs the real orchestrator and adapter against a cell on celld for what the
fixtures cannot express and logs every request, response and socket frame
with a wall-clock stamp to `<log>/<scenario>.frames.log`, with the thread's
projection beside it: `reconnect` terminates the socket three seconds after
the 202 and holds the next connect (`RECONNECT_HOLD_MS`, default 25 s) so
the run's output lands in the gap and has to be read back; `restart` runs
two orchestrator runtimes over one SQLite file and stops the first the way
the server stops; `delegate` runs the continuation worker so the wake
reaches the thread. `live-cell-probe.ts --url <cell>` runs the status probe
Settings shows against the live cell and against stand-ins for an
old-protocol tenant, a protocol-2 tenant, an HTML server, a 404, a tenant
without a key and a closed port. To repeat the run, start celld on a built
cell bundle whose `.dev.vars` points `OPENAI_BASE_URL` at a scripted
Responses API that answers the fixture prompts (the vim repository's
`docs/cell-edition.md` describes the build), record each fixture with
`node scripts/record-cell-replay-fixture.ts --scenario <name> --url <cell>`,
then run the drivers. Every driver run gets its own thread (`--tag`), and a
fixture is re-recorded only against a cell started afresh (`--clean`): the
deterministic test runtime mints the same request id every run, and a kept
cell answers it with the run that ended last, which the adapter fails at
once as a stale acceptance.

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
before and after the prompt registered, a cancel racing `done`, session
resume, shutdown, a disabled instance, refused runtime modes, models and
protocol levels, usage limits, and the reconnect backoff. The fifth review's
probes are tests too: a lost 409 behind another client's identical prompt
adopting nothing, a lost answer recovered by the request id on the opener,
in the log, or on the run the state names, a 5xx with and without the cell's
refusal, Stop decided behind a replay that ended the turn posting no cancel,
a cancel the cell refused settled by the replayed `done`, a truncated gap
marking a wake the cell still runs lost with the loss outranking its `done`,
a standalone error live and in the log, a replay page that makes no progress
or ends before the snapshot, three roster timeouts in a row failing the turn
while reconnection goes on, a regressed numbering, a roster numbered
elsewhere than its state and a fractional number each dropping the
connection, and the fake cell's request ids, refusals and run-targeted
cancels. The sixth review's are too: a gateway's JSON error without the
cell's `runId` recovered from the opener already buffered, a restart
continuation taking back the run the last session owned while the cell still
runs it and settling from the log when it ended in the gap, a message sent
meanwhile refused naming T3's own run and the run offered as a continuation,
and the recorder numbering two turns' request ids on one log page, replayed
with fresh ids. The reconnect races run over a hand-driven socket, since a
transcript walk cannot hold a prompt's answer until after the frames that
race it; the ones that need the roster timeout and the backoff run on the
test clock.

## Follow-ups

- A workspace kind without a local checkout, so cell threads stop borrowing a
  local project and the local-only affordances disappear instead of sitting
  inert; the capability flag for it already exists.
- Attaching a thread to an existing cell project from the UI, through the
  native-session listing, once the tenant can list projects.
- A display name on the tenant, once the cell has an API for it.
- Attachments through `PUT fs/file`, with the prompt naming the paths.
- Fixtures recorded against a tenant, once celld speaks level 3 with the
  request id.
