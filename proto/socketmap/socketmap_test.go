package socketmap

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"kittiwake/rados"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

func newServer(t *testing.T) (*Server, identity.Mailbox) {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	ids := identity.New(meta)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	del := delivery.New(ids, index.New(idxp), message.New(msgp), attach.New(attp), queue.New(idxp), auditor.New(audit.New(idxp), log, nil), log, delivery.Config{})
	ctx := context.Background()
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "off.example", Enabled: false})
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{"read"}, Enabled: true})
	_, _ = ids.CreateAlias(ctx, identity.Alias{Address: "info@example.org", Enabled: true, Destinations: []identity.Destination{{MailboxID: mb.ID}, {Address: "ext@other.example"}}})
	_, _ = ids.CreateAlias(ctx, identity.Alias{Address: "fwd@example.org", Enabled: true, Destinations: []identity.Destination{{Address: "wido@example.org"}}})
	return New(ids, del, log), mb
}

func TestLookups(t *testing.T) {
	s, mb := newServer(t)
	ctx := context.Background()
	cases := map[[2]string]string{
		{"domains", "Example.ORG"}:        "OK example.org",
		{"domains", "off.example"}:        "NOTFOUND ",
		{"domains", "nope.example"}:       "NOTFOUND ",
		{"mailboxes", "wido@example.org"}: "OK " + mb.ID.String(),
		{"mailboxes", "fwd@example.org"}:  "OK " + mb.ID.String(),
		{"mailboxes", "no@example.org"}:   "NOTFOUND ",
		{"mailboxes", "x@nope.example"}:   "NOTFOUND ",
		{"aliases", "info@example.org"}:   "OK info@example.org,ext@other.example",
		{"aliases", "fwd@example.org"}:    "OK wido@example.org",
		{"aliases", "wido@example.org"}:   "NOTFOUND ",
		{"bogus", "x"}:                    "PERM unknown map bogus",
	}
	for in, want := range cases {
		if got := s.Lookup(ctx, in[0], in[1]); got != want {
			t.Errorf("%v: got %q want %q", in, got, want)
		}
	}
}

func TestMailboxStates(t *testing.T) {
	s, mb := newServer(t)
	ctx := context.Background()
	ids := s.ids
	_, _ = ids.UpdateMailbox(ctx, mb.ID, func(m *identity.Mailbox) error { m.State = identity.StateSuspended; return nil })
	if got := s.Lookup(ctx, "mailboxes", "wido@example.org"); got != "OK "+mb.ID.String() {
		t.Fatalf("suspended must still exist for Postfix (queued at LMTP): %q", got)
	}
	_, _ = ids.UpdateMailbox(ctx, mb.ID, func(m *identity.Mailbox) error { m.State = identity.StateDisabled; return nil })
	if got := s.Lookup(ctx, "mailboxes", "wido@example.org"); got != "NOTFOUND " {
		t.Fatalf("disabled must be unknown to Postfix (rejected at SMTP time): %q", got)
	}
}

func TestWireProtocol(t *testing.T) {
	s, mb := newServer(t)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = s.Serve(l) }()
	defer s.Close()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	ask := func(q string) string {
		_, _ = conn.Write(netstring(q))
		resp, err := readNetstring(r, 4096)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if got := ask("mailboxes wido@example.org"); got != "OK "+mb.ID.String() {
		t.Fatalf("got %q", got)
	}
	if got := ask("domains example.org"); got != "OK example.org" {
		t.Fatalf("got %q", got)
	}
	// Second request on the same connection works (persistent).
	if got := ask("domains nope.example"); got != "NOTFOUND " {
		t.Fatalf("got %q", got)
	}
	// Oversized request closes the connection.
	_, _ = conn.Write([]byte("99999:"))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("expected close on oversized request")
	}
}
