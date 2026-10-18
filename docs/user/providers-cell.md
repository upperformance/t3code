# Cell

Cell is a beta integration and is disabled by default. A cell is a project that
runs entirely on a vim.app tenant: its files, its shell, its compiler, and the
coding agent that works on them. Nothing is installed on the machine hosting
your environment; T3 Code talks to the tenant over the network.

Open **Settings → Providers**, add a **Cell** instance, and set **Base URL** to
your tenant, such as `https://acme.vim.app`. Set **Access token** if the tenant
requires one. Refresh provider status, then pick Cell in a thread's model picker.

## How a cell thread works

Each thread gets its own cell project, named with the instance's **Project
prefix** followed by a code derived from the thread. The first message creates
it. The project's files live on the tenant, so you can open its page there to
browse and edit them while the agent works. Deleting the thread in T3 Code does
not remove the cell.

The cell keeps the conversation. Reopening a thread continues where it left off,
and a delegated task that finishes while the thread is idle wakes it with the
result, as with other providers. Several tasks finishing in a row wake it once
each, in order.

## What is different

- **No local files.** The diff view, checkpoints, worktrees, and the terminal
  show the local project, which the cell never changes. Look at the cell's own
  page on the tenant for its files.
- **Attachments** are not delivered to the cell yet. Send text only.
- **Permissions.** Cell runs only in **Full access**. The cell is its own
  sandbox: the agent cannot reach your machine or the network, so there is
  nothing to approve. A thread set to another permission mode does not run on a
  cell; the message fails with a note to switch the thread to Full access and
  send it again.
- **Model.** The tenant decides the model. The picker shows that one model and
  **Default**, which means the same thing; custom models cannot be added to a
  Cell instance.
- **Forks and rewinds** are not available. Switching providers passes the
  conversation as a context handoff.
- **T3 Code's tools** are not available inside a cell, so the agent cannot link
  pull requests or delegate to T3 threads. It can delegate to child cells on the
  tenant.
- **Text generation** for thread titles and commit messages uses another
  provider.

## Status

Provider status checks that the tenant answers and whether it has a model API
key. If the tenant is unreachable, messages fail until it is back.

T3 Code needs a tenant whose celld speaks cell protocol 3 or later. An older
tenant shows an error in provider status naming the protocol it speaks and the
one T3 Code needs, and a message to it fails before anything is sent; update
celld on the tenant.

The connection to a cell may be reset while the tenant moves it; T3 Code
reconnects on its own, reads back what it missed, and the turn carries on. If
the tenant no longer holds the events that were missed, or the cell's run ended
while the connection was down without saying how, the turn is reported as
failed with a note that its result stayed in the cell; the work is not lost,
and the next message continues the same conversation. If someone else is using
the same cell from its page on the tenant, a message sent meanwhile fails with
a note to wait for that run to finish.
