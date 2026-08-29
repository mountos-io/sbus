# csbus

A message router for Claude Code sessions on the same machine to talk to
each other by name, over a Unix domain socket. Store-and-forward: a message
to a name that isn't connected yet waits in memory until it is.

Not part of mountOS — this is infrastructure for Claude session-to-session
coordination, kept in mountos-tools alongside the other external tools.

## Install

```
cd csbus
go install ./cmd/csbus
```

Installs to `$(go env GOPATH)/bin/csbus` (`~/go/bin` by default), which is
already on `$PATH`. Re-run after editing the source to pick up changes.

## You never start the server yourself

`send`, `recv`, `listen`, and `ack` all self-bootstrap: the first one that
finds nobody listening on the socket spawns `csbus serve` detached and
retries. There is no install step, service file, or manual start/stop.
Logs from an auto-spawned hub land at `~/.claude/csbus.sock.log`.

## Commands

```
csbus send   --as NAME --to NAME[,NAME...] [--ttl DURATION] [--ack] BODY
csbus recv   --as NAME [--wait DURATION]
csbus listen --as NAME
csbus ack    --as NAME --id ID
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
  `csbus ack --id <the message's id>` once it's actually handled the
  message, not just seen it.
- Every delivered message prints its sender, sent time, and age
  (`[2026-08-29T05:38:04+05:30 +4s] sess-A: body`), so the receiver can tell
  whether it's still relevant.
- Socket path defaults to `~/.claude/csbus.sock`; override with `--sock` or
  `$CSBUS_SOCK`.

## Example

```
# session B, in the background, gets notified live:
csbus listen --as sess-B &

# session A, from anywhere on the machine:
csbus send --as sess-A --to sess-B --ack "restarting the blockserv pair, hold off on writes"

# session B (or, if it wasn't listening, the next time it checks):
csbus recv --as sess-B --wait 5s

# session B, once it's actually acted on it:
csbus ack --as sess-B --id <id from the recv output>
```

## Wire protocol

See the doc comment at the top of `internal/proto/proto.go` for the full
NDJSON envelope spec. Delivery is at-most-once — the hub never retries on
its own; `--ack`/`ack` exist so a sender can find out what happened instead.
Queues are capped (100 messages, 24h TTL per mailbox by default) and swept
periodically; a message can request a shorter TTL but never a longer one.
