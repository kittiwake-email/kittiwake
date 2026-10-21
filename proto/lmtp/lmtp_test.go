package lmtp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-smtp"

	"dovetail/rados"
	"dovetail/service/auditor"
	"dovetail/service/delivery"
	"dovetail/store/attach"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/message"
	"dovetail/store/queue"
)

func newServer(t *testing.T) (*Server, *index.Store, identity.Mailbox, string) {
	t.Helper()
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	ids := identity.New(meta)
	idx := index.New(idxp)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aud := auditor.New(audit.New(idxp), log, nil)
	del := delivery.New(ids, idx, message.New(msgp), attach.New(attp), queue.New(idxp), aud, log, delivery.Config{MaxMessageSize: 4096, DefaultQuota: identity.QuotaLimits{HardMessages: 3}})
	ctx := context.Background()
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{Enabled: true})
	_, _ = ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{"read"}, Enabled: true})
	srv := New(Config{Hostname: "test", MaxMessageSize: 4096}, del, log)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv, idx, mb, l.Addr().String()
}

func dial(t *testing.T, addr string) *smtp.Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := smtp.NewClientLMTP(conn)
	if err := c.Hello("postfix.test"); err != nil {
		t.Fatal(err)
	}
	return c
}

const msg = "From: a@example.com\r\nTo: wido@example.org\r\nSubject: hi\r\n\r\nhello\r\n"

func TestDeliverAndPerRecipientStatus(t *testing.T) {
	_, idx, mb, addr := newServer(t)
	c := dial(t, addr)
	defer c.Close()
	if err := c.Mail("a@example.com", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Rcpt("wido@example.org", nil); err != nil {
		t.Fatal(err)
	}
	// Unknown recipient refused at RCPT time.
	err := c.Rcpt("nobody@example.org", nil)
	var se *smtp.SMTPError
	if !errors.As(err, &se) || se.Code != 550 || se.EnhancedCode != (smtp.EnhancedCode{5, 1, 1}) {
		t.Fatalf("unknown rcpt: %v", err)
	}
	if err := c.Rcpt("x@nodomain.example", nil); !errors.As(err, &se) || se.EnhancedCode != (smtp.EnhancedCode{5, 1, 2}) {
		t.Fatalf("unknown domain: %v", err)
	}
	d, err := c.Data()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(d, msg)
	resps, err := d.CloseWithLMTPResponse()
	if err != nil {
		t.Fatal(err)
	}
	if r := resps["wido@example.org"]; r == nil {
		t.Fatalf("per-rcpt response: %+v", resps)
	}
	root, _ := idx.GetRoot(context.Background(), mb.ID)
	if root.Usage.Messages != 1 {
		t.Fatalf("not delivered: %+v", root.Usage)
	}
	_ = c.Quit()
}

func TestQuotaAndSizeRefusal(t *testing.T) {
	_, _, _, addr := newServer(t)
	for i := 0; i < 3; i++ {
		c := dial(t, addr)
		if err := c.SendMail("a@example.com", []string{"wido@example.org"}, strings.NewReader(msg)); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
		_ = c.Close()
	}
	c := dial(t, addr)
	defer c.Close()
	_ = c.Mail("a@example.com", nil)
	err := c.Rcpt("wido@example.org", nil)
	var se *smtp.SMTPError
	if !errors.As(err, &se) || se.Code != 552 {
		t.Fatalf("over quota at RCPT: %v", err)
	}
	// Too large announced via SIZE.
	c2 := dial(t, addr)
	defer c2.Close()
	if err := c2.Mail("a@example.com", &smtp.MailOptions{Size: 1 << 20}); !errors.As(err, &se) || se.Code != 552 {
		t.Fatalf("size: %v", err)
	}
}
