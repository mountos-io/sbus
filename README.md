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
- `sbus listen --as <my-name>` (run in background to watch for messages)
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

## Commands

```
sbus send   --as NAME --to NAME[,NAME...] [--ttl DURATION] [--ack] BODY
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
- `listen` is `recv` with no time limit — run it with `run_in_background` in
  a session that wants to be notified as messages arrive, rather than
  polling.
- `--ack` on `send` asks for receipts in your own mailbox: a `delivered`
  receipt the moment the message leaves the queue (read via `recv`/`listen`,
  automatic), and an `acked` receipt if the recipient later runs
  `sbus ack --id <the message's id>` once it's actually handled the
  message, not just seen it.
- Every delivered message prints its sender, sent time, and age
  (`[2026-08-29T05:38:04+05:30 +4s] sess-A: body`), so the receiver can tell
  whether it's still relevant.
- Socket path defaults to `~/.claude/sbus.sock`; override with `--sock` or
  `$SBUS_SOCK`.

## Example

```
# session B, in the background, gets notified live:
sbus listen --as sess-B &

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
