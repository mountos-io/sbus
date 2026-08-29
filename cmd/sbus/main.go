// Command sbus is the CLI for the session bus: a store-and-forward router
// over a Unix domain socket that lets AI agent sessions on the same
// machine trade named messages, even when the recipient isn't up yet.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mountos-io/sbus/internal/bus"
	"github.com/mountos-io/sbus/internal/hub"
	"github.com/mountos-io/sbus/internal/proto"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "send":
		cmdSend(os.Args[2:])
	case "recv":
		cmdRecv(os.Args[2:], false)
	case "listen":
		cmdRecv(os.Args[2:], true)
	case "ack":
		cmdAck(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: sbus <command> [flags]

  send   --as NAME --to NAME[,NAME...] [--ttl DURATION] [--ack] [--reply-to ID] BODY
         --to accepts a broadcast pattern: "*" (everyone) or "prefix:*"
         (every listening name starting with "prefix:").
  recv   --as NAME [--wait DURATION]     drain queued messages, optionally
                                          blocking up to DURATION for more
  listen --as NAME                       block forever, streaming messages
                                          as they arrive (for run_in_background)
  ack    --as NAME --id ID               confirm you've processed a message
  serve  --sock PATH                     internal: run the hub in the foreground`)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "sbus:", err)
		os.Exit(1)
	}
}

func expectOK(r *proto.Reader) error {
	e, err := r.Read()
	if err != nil {
		return err
	}
	if e.Op != proto.OpOK {
		return fmt.Errorf("register failed: %s", e.Reason)
	}
	return nil
}

func register(sock, name string) (*proto.Reader, *proto.Writer, net.Conn) {
	conn, err := bus.Dial(sock)
	must(err)
	w := proto.NewWriter(conn)
	r := proto.NewReader(conn)
	must(w.Write(&proto.Envelope{Op: proto.OpRegister, Name: name}))
	must(expectOK(r))
	return r, w, conn
}

func cmdSend(args []string) {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	as := fs.String("as", "", "this session's name")
	to := fs.String("to", "", "comma-separated recipient names or broadcast pattern")
	ttl := fs.Duration("ttl", 0, "override the default queue TTL for this message")
	ack := fs.Bool("ack", false, "request a delivery/ack receipt back in my own mailbox")
	replyTo := fs.String("reply-to", "", "id of the message this one replies to")
	sock := fs.String("sock", bus.DefaultSockPath(), "hub socket path")
	must(fs.Parse(args))
	if *as == "" || *to == "" || fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "send requires --as, --to, and a message body")
		os.Exit(2)
	}
	body := strings.Join(fs.Args(), " ")
	targets := strings.Split(*to, ",")

	r, w, conn := register(*sock, *as)
	defer conn.Close()
	must(w.Write(&proto.Envelope{Op: proto.OpSend, To: targets, Body: body, TTL: int64((*ttl).Seconds()), Ack: *ack, ReplyTo: *replyTo}))

	exitCode := 0
	for range targets {
		reply, err := r.Read()
		must(err)
		switch reply.Op {
		case proto.OpSent:
			if proto.IsBroadcast(reply.Target) {
				fmt.Printf("broadcast id=%s pattern=%s reached=%d\n", reply.ID, reply.Target, reply.N)
			} else {
				fmt.Printf("sent id=%s to=%s\n", reply.ID, reply.Target)
			}
		case proto.OpError:
			fmt.Fprintf(os.Stderr, "error to=%s: %s\n", reply.Target, reply.Reason)
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func cmdRecv(args []string, blockForever bool) {
	name := "recv"
	if blockForever {
		name = "listen"
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	as := fs.String("as", "", "this session's name")
	wait := fs.Duration("wait", 0, "block up to this long for new messages (recv only)")
	sock := fs.String("sock", bus.DefaultSockPath(), "hub socket path")
	must(fs.Parse(args))
	if *as == "" {
		fmt.Fprintf(os.Stderr, "%s requires --as\n", name)
		os.Exit(2)
	}

	r, w, conn := register(*sock, *as)
	defer conn.Close()

	op := proto.OpPoll
	if blockForever || *wait > 0 {
		op = proto.OpListen
	}
	must(w.Write(&proto.Envelope{Op: op}))
	if !blockForever && *wait > 0 {
		conn.SetReadDeadline(time.Now().Add(*wait))
	}

	for {
		e, err := r.Read()
		if err != nil {
			return // EOF, deadline, or hub gone: nothing more to show
		}
		switch e.Op {
		case proto.OpMsg:
			ts := time.Unix(e.TS, 0)
			from := e.From
			if e.ReplyTo != "" {
				from = fmt.Sprintf("%s (re: %s)", e.From, e.ReplyTo)
			}
			fmt.Printf("[%s +%s] id=%s %s: %s\n", ts.Format(time.RFC3339), time.Since(ts).Round(time.Second), e.ID, from, e.Body)
		case proto.OpReceipt:
			fmt.Printf("[receipt] id=%s from=%s status=%s\n", e.ID, e.From, e.Status)
		case proto.OpOK:
			if op == proto.OpPoll {
				return // explicit "queue drained" marker
			}
		case proto.OpError:
			fmt.Fprintln(os.Stderr, "sbus:", e.Reason) // report and keep reading
		}
	}
}

func cmdAck(args []string) {
	fs := flag.NewFlagSet("ack", flag.ExitOnError)
	as := fs.String("as", "", "this session's name")
	id := fs.String("id", "", "message id to acknowledge")
	sock := fs.String("sock", bus.DefaultSockPath(), "hub socket path")
	must(fs.Parse(args))
	if *as == "" || *id == "" {
		fmt.Fprintln(os.Stderr, "ack requires --as and --id")
		os.Exit(2)
	}

	r, w, conn := register(*sock, *as)
	defer conn.Close()
	must(w.Write(&proto.Envelope{Op: proto.OpAck, ID: *id}))
	reply, err := r.Read()
	must(err)
	if reply.Op == proto.OpError {
		fmt.Fprintln(os.Stderr, "error:", reply.Reason)
		os.Exit(1)
	}
	fmt.Println("acked", *id)
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	sock := fs.String("sock", bus.DefaultSockPath(), "hub socket path")
	must(fs.Parse(args))
	if err := hub.Serve(*sock); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}
