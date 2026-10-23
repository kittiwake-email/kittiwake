package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"kittiwake/rados"
	"kittiwake/service/auditor"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
	"kittiwake/store/queue"
)

// failDocs is a messages pool whose message-document writes fail: the
// shape of a storage error in the middle of a delivery.
type failDocs struct {
	rados.IOContext
	fail bool
}

var errInjected = errors.New("injected storage failure")

func (p *failDocs) Write(ctx context.Context, oid string, tx *rados.WriteTx) (uint64, error) {
	if p.fail && len(oid) == 73 && !strings.HasSuffix(oid, ".raw") {
		return 0, errInjected
	}
	return p.IOContext.Write(ctx, oid, tx)
}

// A delivery whose message object cannot be written leaves nothing
// behind: no index entry, no change in counters or quota usage, no
// message or raw object, no attachment reference.
func TestFailedDeliveryLeavesNothing(t *testing.T) {
	c := rados.NewMemoryClient()
	meta, _ := c.Pool("meta")
	idxp, _ := c.Pool("index")
	msgp, _ := c.Pool("messages")
	attp, _ := c.Pool("attachments")
	pool := &failDocs{IOContext: msgp}
	ids, idx, msgs, atts := identity.New(meta), index.New(idxp), message.New(pool), attach.New(attp)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(ids, idx, msgs, atts, queue.New(idxp), auditor.New(audit.New(idxp), log, nil), log, Config{MaxMessageSize: 1 << 20, KeepRaw: "always", InlineMax: 16})
	ctx := context.Background()
	_, _ = ids.CreateDomain(ctx, identity.Domain{Name: "example.org", Enabled: true})
	mb, _ := ids.CreateMailbox(ctx, identity.Mailbox{})
	_, _ = ids.CreateAccount(ctx, identity.Account{Username: "wido@example.org", MailboxID: mb.ID, Roles: []string{"read"}, Enabled: true})

	raw := "From: a@example.com\r\nTo: wido@example.org\r\nSubject: att\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhi\r\n--B\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=\r\n--B--\r\n"
	// One good delivery first so the attachment object exists with one reference.
	out, err := svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(raw)})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := idx.GetRoot(ctx, mb.ID)

	pool.fail = true
	if _, err := svc.Deliver(ctx, Request{Recipient: "wido@example.org", Raw: []byte(raw)}); !errors.Is(err, errInjected) {
		t.Fatalf("expected the injected failure, got %v", err)
	}
	pool.fail = false

	after, _ := idx.GetRoot(ctx, mb.ID)
	if after.Usage != before.Usage || after.FolderByID(out[0].FolderID).Total != 1 {
		t.Fatalf("counters changed by a failed delivery: before %+v after %+v", before.Usage, after.Usage)
	}
	recs, _, _ := idx.ListFolder(ctx, mb.ID, out[0].FolderID, 0, 10)
	if len(recs) != 1 {
		t.Fatalf("index records: %d, want 1", len(recs))
	}
	n := 0
	_ = msgs.List(ctx, func(message.Info) error { n++; return nil })
	if n != 1 {
		t.Fatalf("message objects (docs and lone raws): %d, want 1", n)
	}
	var hash string
	_ = atts.List(ctx, func(_ int64, h string) error { hash = h; return nil })
	info, err := atts.Stat(ctx, mb.ID, hash)
	if err != nil || info.Refs != 1 {
		t.Fatalf("attachment refs after the failed delivery: %+v %v, want 1", info, err)
	}
	// A failed preparation never becomes visible in the change log.
	changes, _, _, _ := idx.Changes(ctx, mb.ID, before.ModSeq, 10)
	if len(changes) != 0 {
		t.Fatalf("change log after the failed delivery: %+v", changes)
	}
}
