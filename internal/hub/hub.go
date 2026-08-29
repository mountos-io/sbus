// Package hub implements the csbus server: an in-memory, per-name mailbox
// keyed store-and-forward router reachable over a Unix domain socket.
//
// A message is delivered immediately if the target is currently listening,
// otherwise it's queued (bounded, TTL-limited) until the target connects.
// Delivery is at-most-once: the hub never retries on its own. A sender that
// asked for "ack" instead gets a receipt back in its own mailbox once the
// message leaves the queue ("delivered"), and again if the receiver
// explicitly acks it ("acked").
package hub

import (
	"maps"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mountos-io/mountos-tools/csbus/internal/proto"
)

const (
	DefaultCap = 100             // max queued messages per idle mailbox
	DefaultTTL = 24 * time.Hour  // max time a queued message waits for its recipient
	sweepEvery = 5 * time.Minute // background cleanup of expired queue entries
	dialProbe  = 200 * time.Millisecond
)

type deliverStatus int

const (
	full deliverStatus = iota
	queued
	deliveredLive
)

type queuedMsg struct {
	id      string // empty for receipts: not itself ack-trackable
	expires time.Time
	env     *proto.Envelope
}

type liveConn struct{ w *proto.Writer }

type mailbox struct {
	mu    sync.Mutex
	queue []*queuedMsg
	live  *liveConn
}

func (b *mailbox) push(m *queuedMsg, cap int) deliverStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.live != nil {
		if err := b.live.w.Write(m.env); err == nil {
			return deliveredLive
		}
		b.live = nil
	}
	if len(b.queue) >= cap {
		return full
	}
	b.queue = append(b.queue, m)
	return queued
}

// pushLiveOnly writes env directly to the current listener, if any, without
// ever touching the queue. Used for broadcast, which has no single owner to
// hold a backlog for.
func (b *mailbox) pushLiveOnly(env *proto.Envelope) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.live == nil {
		return false
	}
	if err := b.live.w.Write(env); err != nil {
		b.live = nil
		return false
	}
	return true
}

// attach makes w this mailbox's live listener and returns its current
// backlog (expired entries dropped), clearing the queue.
func (b *mailbox) attach(w *proto.Writer) []*queuedMsg {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.live = &liveConn{w: w}
	return b.drainLocked()
}

// drain returns and clears the current backlog without becoming the live
// listener (used by poll, a one-shot check).
func (b *mailbox) drain() []*queuedMsg {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.drainLocked()
}

func (b *mailbox) drainLocked() []*queuedMsg {
	now := time.Now()
	out := make([]*queuedMsg, 0, len(b.queue))
	for _, m := range b.queue {
		if now.Before(m.expires) {
			out = append(out, m)
		}
	}
	b.queue = nil
	return out
}

func (b *mailbox) detach(w *proto.Writer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.live != nil && b.live.w == w {
		b.live = nil
	}
}

type pendingAck struct {
	from    string
	expires time.Time
}

// Hub is the in-memory router. Zero value is not usable; use New.
type Hub struct {
	cap        int
	defaultTTL time.Duration
	idSeq      atomic.Uint64

	mu        sync.Mutex
	mailboxes map[string]*mailbox

	amu  sync.Mutex
	acks map[string]pendingAck
}

func New() *Hub {
	return &Hub{
		cap:        DefaultCap,
		defaultTTL: DefaultTTL,
		mailboxes:  make(map[string]*mailbox),
		acks:       make(map[string]pendingAck),
	}
}

func (h *Hub) nextID() string {
	return strconv.FormatUint(h.idSeq.Add(1), 36)
}

func (h *Hub) box(name string) *mailbox {
	h.mu.Lock()
	defer h.mu.Unlock()
	b, ok := h.mailboxes[name]
	if !ok {
		b = &mailbox{}
		h.mailboxes[name] = b
	}
	return b
}

func (h *Hub) enqueue(target, id string, expires time.Time, env *proto.Envelope) deliverStatus {
	return h.box(target).push(&queuedMsg{id: id, expires: expires, env: env}, h.cap)
}

func (h *Hub) trackAck(id, from string, ttl time.Duration) {
	h.amu.Lock()
	defer h.amu.Unlock()
	h.acks[id] = pendingAck{from: from, expires: time.Now().Add(ttl)}
}

// peekAck reports the original sender of id without consuming the entry, so
// both an auto "delivered" receipt and a later explicit ack can use it.
func (h *Hub) peekAck(id string) (string, bool) {
	h.amu.Lock()
	defer h.amu.Unlock()
	p, ok := h.acks[id]
	if !ok || time.Now().After(p.expires) {
		return "", false
	}
	return p.from, true
}

func (h *Hub) takeAck(id string) (string, bool) {
	h.amu.Lock()
	defer h.amu.Unlock()
	p, ok := h.acks[id]
	delete(h.acks, id)
	if !ok || time.Now().After(p.expires) {
		return "", false
	}
	return p.from, true
}

func (h *Hub) emitReceipt(to, id, target, status string) {
	env := &proto.Envelope{Op: proto.OpReceipt, ID: id, From: target, Status: status, TS: time.Now().Unix()}
	h.enqueue(to, "", time.Now().Add(h.defaultTTL), env) // best-effort: dropped if the sender's own queue is full
}

// broadcastLive fans body out live-only to every listening mailbox whose
// name has prefix as a prefix (except from itself), and reports how many
// were actually reached.
func (h *Hub) broadcastLive(from, id string, ts, expires time.Time, body, prefix string) int {
	env := &proto.Envelope{Op: proto.OpMsg, ID: id, From: from, Body: body, TS: ts.Unix(), Exp: expires.Unix()}

	h.mu.Lock()
	targets := make([]*mailbox, 0, len(h.mailboxes))
	for name, b := range h.mailboxes {
		if name != from && strings.HasPrefix(name, prefix) {
			targets = append(targets, b)
		}
	}
	h.mu.Unlock()

	n := 0
	for _, b := range targets {
		if b.pushLiveOnly(env) {
			n++
		}
	}
	return n
}

// handleSend processes one "send" envelope from the connection registered
// as from, returning one reply per "to" entry.
func (h *Hub) handleSend(from string, e *proto.Envelope) []*proto.Envelope {
	if len(e.To) == 0 || e.Body == "" {
		return []*proto.Envelope{{Op: proto.OpError, Reason: "send requires to and body"}}
	}
	ttl := h.defaultTTL
	if e.TTL > 0 {
		ttl = min(time.Duration(e.TTL)*time.Second, h.defaultTTL)
	}
	now := time.Now()
	expires := now.Add(ttl)

	replies := make([]*proto.Envelope, 0, len(e.To))
	for _, target := range e.To {
		if proto.IsBroadcast(target) {
			id := h.nextID()
			n := h.broadcastLive(from, id, now, expires, e.Body, proto.BroadcastPrefix(target))
			replies = append(replies, &proto.Envelope{Op: proto.OpSent, ID: id, Target: target, N: n})
			continue
		}

		id := h.nextID()
		msg := &proto.Envelope{Op: proto.OpMsg, ID: id, From: from, Body: e.Body, TS: now.Unix(), Exp: expires.Unix()}
		status := h.enqueue(target, id, expires, msg)
		if status == full {
			replies = append(replies, &proto.Envelope{Op: proto.OpError, ID: id, Target: target, Reason: "queue full"})
			continue
		}
		replies = append(replies, &proto.Envelope{Op: proto.OpSent, ID: id, Target: target})
		if e.Ack {
			h.trackAck(id, from, ttl)
			if status == deliveredLive {
				h.emitReceipt(from, id, target, "delivered")
			}
		}
	}
	return replies
}

func (h *Hub) handleAck(name string, e *proto.Envelope) *proto.Envelope {
	from, ok := h.takeAck(e.ID)
	if !ok {
		return &proto.Envelope{Op: proto.OpError, ID: e.ID, Reason: "unknown or already-acked id"}
	}
	h.emitReceipt(from, e.ID, name, "acked")
	return &proto.Envelope{Op: proto.OpOK, ID: e.ID}
}

// deliveredReceipts emits a "delivered" receipt for every ack-wanted message
// in msgs, now that they've actually reached name via poll or listen.
func (h *Hub) deliveredReceipts(name string, msgs []*queuedMsg) {
	for _, m := range msgs {
		if m.id == "" {
			continue
		}
		if from, ok := h.peekAck(m.id); ok {
			h.emitReceipt(from, m.id, name, "delivered")
		}
	}
}

func (h *Hub) sweepOnce() {
	now := time.Now()

	h.mu.Lock()
	for name, b := range h.mailboxes {
		b.mu.Lock()
		b.queue = slices.DeleteFunc(b.queue, func(m *queuedMsg) bool { return now.After(m.expires) })
		empty := len(b.queue) == 0 && b.live == nil
		b.mu.Unlock()
		if empty {
			delete(h.mailboxes, name)
		}
	}
	h.mu.Unlock()

	h.amu.Lock()
	maps.DeleteFunc(h.acks, func(_ string, p pendingAck) bool { return now.After(p.expires) })
	h.amu.Unlock()
}

func (h *Hub) sweepLoop() {
	t := time.NewTicker(sweepEvery)
	defer t.Stop()
	for range t.C {
		h.sweepOnce()
	}
}

func (h *Hub) handleConn(c net.Conn) {
	defer c.Close()
	r := proto.NewReader(c)
	w := proto.NewWriter(c)

	first, err := r.Read()
	if err != nil {
		return
	}
	if first.Op != proto.OpRegister || first.Name == "" {
		w.Write(&proto.Envelope{Op: proto.OpError, Reason: "first op must be register with a name"})
		return
	}
	name := first.Name
	if err := w.Write(&proto.Envelope{Op: proto.OpOK}); err != nil {
		return
	}

	for {
		e, err := r.Read()
		if err != nil {
			h.box(name).detach(w)
			return
		}
		switch e.Op {
		case proto.OpSend:
			for _, reply := range h.handleSend(name, e) {
				if w.Write(reply) != nil {
					return
				}
			}
		case proto.OpPoll:
			msgs := h.box(name).drain()
			h.deliveredReceipts(name, msgs)
			for _, m := range msgs {
				if w.Write(m.env) != nil {
					return
				}
			}
			if w.Write(&proto.Envelope{Op: proto.OpOK}) != nil {
				return
			}
		case proto.OpListen:
			msgs := h.box(name).attach(w)
			h.deliveredReceipts(name, msgs)
			for _, m := range msgs {
				if w.Write(m.env) != nil {
					h.box(name).detach(w)
					return
				}
			}
		case proto.OpAck:
			if w.Write(h.handleAck(name, e)) != nil {
				return
			}
		default:
			if w.Write(&proto.Envelope{Op: proto.OpError, Reason: "unknown op"}) != nil {
				return
			}
		}
	}
}

// Serve binds sockPath and runs the hub until the listener fails. If another
// hub is already listening on sockPath, Serve returns nil immediately: the
// caller should just proceed to dial it.
func Serve(sockPath string) error {
	if probeAlive(sockPath) {
		return nil
	}
	os.Remove(sockPath) // clear a socket file left by a crashed hub
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		if probeAlive(sockPath) {
			return nil // lost the bind race to a concurrent spawn, that's fine
		}
		return err
	}
	defer ln.Close()
	if err := os.Chmod(sockPath, 0o600); err != nil {
		return err
	}

	h := New()
	go h.sweepLoop()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go h.handleConn(conn)
	}
}

func probeAlive(sockPath string) bool {
	c, err := net.DialTimeout("unix", sockPath, dialProbe)
	if err != nil {
		return false
	}
	c.Close()
	return true
}
