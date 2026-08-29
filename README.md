# sbus

A message router for AI coding agent sessions on the same machine to talk
to each other by name, over a Unix domain socket. Store-and-forward: a
message to a name that isn't connected yet waits in memory until it is.
Any agent that can run a shell command and read a CLAUDE.md/AGENTS.md-style
instruction file can use it — Claude Code, Codex, or otherwise; nothing
about the protocol or CLI is Claude-specific past the default socket path.

Not part of mountOS — this is infrastructure for agent session-to-session
coordination, kept in mountos-tools alongside the other external tools.

## Security

No authentication, no encryption, no per-agent identity check: whoever can
reach the socket can register as any name, read what's queued for it, and
send as anyone. The socket file is created mode `0600` (owner-only), so on
a normal single-user machine that means "anything running as you can use
it" — which is the intended scope. This is for local sessions on one
machine, not for exposing over a network or sharing across users/hosts. Do
not put anything in a message you wouldn't want another local process
reading, and don't proxy the socket to a network port.

## Setup

Requires a Go 1.24+ toolchain.

```
cd sbus
go install ./cmd/sbus
```

This installs the `sbus` binary to `$(go env GOPATH)/bin` (`~/go/bin` by
default). Confirm that directory is on `$PATH`:

```
command -v sbus
```

If that prints nothing, `~/go/bin` isn't on `$PATH` yet — add it in your
shell rc (`~/.zshrc`, `~/.bashrc`, ...):

```
export PATH="$(go env GOPATH)/bin:$PATH"
```

then open a new shell (or `source` the rc file) and re-run `command -v
sbus`. Re-run `go install ./cmd/sbus` after editing the source to pick up
changes — there's no separate build step for agents to run.

## Tell your agent about it

An agent won't know sbus exists unless something tells it. Drop a section
like this into whatever instruction file your agent loads — `CLAUDE.md`
(`~/.claude/CLAUDE.md` for every project, or a project-level `CLAUDE.md`
for one repo), `AGENTS.md`, or equivalent:

```markdown
## Session bus (sbus)

To message another AI agent session on this machine, use the `sbus` CLI
(on PATH; setup and full protocol: <path-to-this-repo>/README.md). Never
start a server yourself — `send`/`recv`/`listen`/`ack` all auto-start the
hub on first use if it isn't already running.

- `sbus send --as <my-name> --to <name>[,<name>...] [--ack] "text"`
- `sbus recv --as <my-name> [--wait 10s]`
- `sbus listen --as <my-name>` never exits on its own — run it with
  whatever your harness gives you for a background task that reports
  output as it streams, not one that only reports back at process exit.
  An exit-only background runner will never notify you: `listen` has no
  exit to wait for.
- `--to "*"` broadcasts to every currently-listening session; `--to
  "prefix:*"` scopes it to listening names starting with `prefix:`.
- Pick `<my-name>` to be identifiable (project + role), e.g.
  `mountos-servers:blockserv-migration`.
```

Put it in the global file if you want every session on the machine to know
about sbus regardless of which repo it's working in; put it in a
project-level file to scope it to one project's sessions. Two agents can
only reach each other if they're pointed at the same socket (see below), so
if your agents live in different sandboxes/containers, set `$SBUS_SOCK` to
a path both can actually see instead of relying on the default.

## You never start the server yourself

`send`, `recv`, `listen`, and `ack` all self-bootstrap: the first one that
finds nobody listening on the socket spawns `sbus serve` detached and
retries. There is no install step, service file, or manual start/stop.
Logs from an auto-spawned hub land at `~/.claude/sbus.sock.log`.

## A name can have multiple live listeners

Any number of connections can register `listen`/`recv --wait` under the
same name at once — registering never evicts an existing listener. A
message reaching that name (a plain `send`, a receipt, or a matching
broadcast) is delivered to every one of its currently-live listeners, not
just one. This makes a standing `listen` and a one-off `recv --wait` under
the same name safe to run at the same time — the one-off doesn't disturb
the standing listener — and it makes restarting a session's listener under
the same name safe too: the old connection just keeps running alongside
the new one until it's stopped or its process ends on its own.

## Commands

```
sbus send   --as NAME --to NAME[,NAME...] [--ttl DURATION] [--ack] [--reply-to ID] BODY
sbus recv   --as NAME [--wait DURATION]
sbus listen --as NAME
sbus ack    --as NAME --id ID
```

- `send` is fire-and-forget: it returns as soon as the hub has accepted or
  rejected the message for each target, it does not wait for the target to
  read it.
- `--to` takes one or more names, comma-separated. A trailing `*` is a
  broadcast pattern instead of a name: `"*"` reaches every session currently
  running `listen`; `"prefix:*"` reaches only listening names starting with
  `prefix:`. Broadcasts are never queued — a session that isn't listening
  right now just doesn't see it. Use it for presence/announcement traffic
  ("I'm touching this folder"), not for anything a session must eventually
  see.
- `recv` drains whatever's queued right now. Add `--wait 30s` to block up to
  that long for something new instead of returning empty-handed.
- `listen` is `recv` with no time limit — it blocks and streams messages
  forever, so it never exits on its own. That makes a plain "run this in
  the background" primitive the wrong tool if that primitive only reports
  back when the process exits (a common default): it will sit there
  silently and never notify you, since `listen` gives it no exit to catch.
  Use whatever your harness offers for a background task whose output
  streams back live, line by line, while it's still running. Relaunching
  `recv --wait N` in a loop is not a fix for this either — `recv --wait`
  keeps streaming until N elapses, same as `listen`, so it has the same
  exit-only-notification problem, just bounded to N.
- `--ack` on `send` asks for receipts in your own mailbox: a `delivered`
  receipt the moment the message leaves the queue (read via `recv`/`listen`,
  automatic), and an `acked` receipt if the recipient later runs
  `sbus ack --id <the message's id>` once it's actually handled the
  message, not just seen it.
- `--reply-to <id>` on `send` threads a reply to an earlier message: the
  receiver sees `id` (the new message's own id, for further ack/reply) and
  `reply_to` (the id it's answering) on delivery. Purely informational — the
  hub doesn't validate that the referenced id ever existed.
- Every delivered message prints its own id, sender, sent time, age, and
  (if set) what it's a reply to
  (`[2026-08-29T05:38:04+05:30 +4s] id=2 sess-B (re: 1): body`), so the
  receiver can tell whether it's still relevant and what to reply to.
- Socket path defaults to `~/.claude/sbus.sock`; override with `--sock` or
  `$SBUS_SOCK`.

## Example

```
# session B: run under a streaming-aware background watcher (not a plain
# fire-and-forget background shell — that only reports at exit, and
# listen never exits):
sbus listen --as sess-B

# session A, from anywhere on the machine:
sbus send --as sess-A --to sess-B --ack "restarting the blockserv pair, hold off on writes"

# session B (or, if it wasn't listening, the next time it checks):
sbus recv --as sess-B --wait 5s

# session B, once it's actually acted on it:
sbus ack --as sess-B --id <id from the recv output>
```

## Wire protocol

See the doc comment at the top of `internal/proto/proto.go` for the full
NDJSON envelope spec. Delivery is at-most-once — the hub never retries on
its own; `--ack`/`ack` exist so a sender can find out what happened instead.
Queues are capped (100 messages, 24h TTL per mailbox by default) and swept
periodically; a message can request a shorter TTL but never a longer one.
