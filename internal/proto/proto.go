// Package proto is the newline-delimited JSON wire protocol between sbus
// clients and the hub, spoken over a Unix domain socket. Every connection
// starts with a client "register", then exchanges any number of envelopes
// until it closes.
//
// Client -> hub:
//
//	{"op":"register","name":"sess-A"}
//	{"op":"send","to":["sess-B"],"body":"...","ttl":300,"ack":true}
//	{"op":"send","to":["sess-A"],"body":"...","reply_to":"m1"} // threaded reply to message m1
//	{"op":"send","to":["*"],"body":"..."}         // broadcast: every live listener
//	{"op":"send","to":["mountos:*"],"body":"..."} // broadcast: live listeners named "mountos:..."
//	{"op":"poll"}                                // drain my queue now, don't block
//	{"op":"listen"}                               // drain, then block for live pushes
//	{"op":"ack","id":"m1"}                        // I finished processing message m1
//
// Hub -> client:
//
//	{"op":"ok"}                                   // register accepted, or poll drained
//	{"op":"sent","id":"m1","target":"sess-B"}      // send accepted for one target
//	{"op":"error","target":"sess-B","reason":"..."}// send rejected for one target
//	{"op":"msg","id":"m2","from":"sess-B","body":"...","ts":..,"exp":..,"reply_to":"m1"}
//	{"op":"receipt","id":"m1","from":"sess-B","status":"delivered","ts":..}
package proto

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
)

// Op names an envelope's operation.
type Op string

const (
	OpRegister Op = "register"
	OpSend     Op = "send"
	OpPoll     Op = "poll"
	OpListen   Op = "listen"
	OpAck      Op = "ack"

	OpOK      Op = "ok"
	OpSent    Op = "sent"
	OpError   Op = "error"
	OpMsg     Op = "msg"
	OpReceipt Op = "receipt"
)

// A "to" entry ending in "*" is a broadcast pattern, not a name: it's
// delivered live-only to every currently-listening connection (other than
// the sender) whose name has the text before "*" as a prefix. "*" alone
// reaches everyone. Broadcast messages are never queued: a session that
// isn't listening right now simply doesn't see them.
func IsBroadcast(target string) bool { return strings.HasSuffix(target, "*") }

// BroadcastPrefix returns the name prefix a broadcast pattern scopes to;
// "" for the unscoped "*".
func BroadcastPrefix(target string) string { return strings.TrimSuffix(target, "*") }

// Envelope is the single wire type; fields unused by an op are omitted.
type Envelope struct {
	Op Op `json:"op"`

	Name    string   `json:"name,omitempty"`     // register
	To      []string `json:"to,omitempty"`       // send
	Body    string   `json:"body,omitempty"`     // send, msg
	TTL     int64    `json:"ttl,omitzero"`       // send: seconds, overrides the hub default (capped)
	Ack     bool     `json:"ack,omitzero"`       // send: request a delivery/ack receipt
	ReplyTo string   `json:"reply_to,omitempty"` // send, msg: id of the message this one replies to

	ID     string `json:"id,omitempty"`     // sent, error, msg, ack, receipt
	From   string `json:"from,omitempty"`   // msg, receipt: original sender / acker name
	Target string `json:"target,omitempty"` // sent, error: which "to" entry this reply is about
	Status string `json:"status,omitempty"` // receipt: "delivered" or "acked"
	Reason string `json:"reason,omitempty"` // error
	N      int    `json:"n,omitzero"`       // sent (broadcast only): live recipients reached
	TS     int64  `json:"ts,omitzero"`      // msg, receipt: unix seconds, when it was produced
	Exp    int64  `json:"exp,omitzero"`     // msg: unix seconds, when it stops being relevant
}

// Reader decodes one Envelope per Read call from an NDJSON stream.
type Reader struct{ dec *json.Decoder }

func NewReader(r io.Reader) *Reader { return &Reader{dec: json.NewDecoder(r)} }

func (r *Reader) Read() (*Envelope, error) {
	var e Envelope
	if err := r.dec.Decode(&e); err != nil {
		return nil, err
	}
	return &e, nil
}

// Writer encodes Envelopes as NDJSON. Safe for concurrent use: a mailbox's
// live connection can be written to by another connection's goroutine while
// its own goroutine is also replying to it.
type Writer struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewWriter(w io.Writer) *Writer { return &Writer{enc: json.NewEncoder(w)} }

func (w *Writer) Write(e *Envelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(e)
}
