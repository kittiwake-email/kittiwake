// Package pop3 is the POP3 adapter (RFC 1939, CAPA RFC 2449, SASL AUTH
// RFC 5034) over the mailbox service. It serves INBOX only; UIDLs are
// derived from the folder's UIDVALIDITY and UID; DELE is lazy: deleted
// messages are expunged into the retention set at QUIT. TLS is terminated
// upstream, so no STLS. See README.md "Protocols".
package pop3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/metrics"
	"kittiwake/proto/proxyproto"
	"kittiwake/service/actor"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/mailbox"
	"kittiwake/store/index"
)

// Config for the server.
type Config struct {
	Hostname    string
	IdleTimeout time.Duration
}

const (
	maxLine        = 4096
	maxAuthFails   = 3
	serviceTimeout = 2 * time.Minute
)

// Server is the POP3 server.
type Server struct {
	cfg  Config
	auth *authsvc.Service
	mb   *mailbox.Service
	log  *slog.Logger

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
	done  bool
}

// New returns a server.
func New(cfg Config, authSvc *authsvc.Service, mb *mailbox.Service, log *slog.Logger) *Server {
	if cfg.Hostname == "" {
		cfg.Hostname = "kittiwake"
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, auth: authSvc, mb: mb, log: log, conns: map[net.Conn]struct{}{}}
}

// Serve accepts connections until the listener is closed.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	s.ln = l
	s.mu.Unlock()
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.done {
			s.mu.Unlock()
			_ = c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(c)
	}
}

// Shutdown stops accepting, waits for sessions up to ctx, then closes them.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.done = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.mu.Unlock()
	finished := make(chan struct{})
	go func() { s.wg.Wait(); close(finished) }()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return s.Close()
	}
}

// Close stops the listener and all connections immediately.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	return err
}

type msgEntry struct {
	rec     index.Record
	deleted bool
}

type session struct {
	srv      *Server
	conn     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	clientIP string
	user     string // pending USER
	ctx      context.Context
	act      actor.Actor
	authed   bool
	fails    int
	inbox    *index.Folder
	msgs     []*msgEntry
}

func (s *Server) handle(c net.Conn) {
	metrics.Connections.WithLabelValues("pop3").Inc()
	defer func() {
		metrics.Connections.WithLabelValues("pop3").Dec()
		_ = c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	ss := &session{srv: s, conn: c, r: bufio.NewReaderSize(c, maxLine+2), w: bufio.NewWriter(c), clientIP: proxyproto.IPOf(c.RemoteAddr()), ctx: context.Background()}
	ss.ok("Kittiwake POP3 server ready")
	for {
		_ = c.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))
		line, err := ss.readLine()
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(line, " ")
		cmd = strings.ToUpper(cmd)
		arg = strings.TrimSpace(arg)
		if !ss.dispatch(cmd, arg) {
			return
		}
	}
}

func (ss *session) readLine() (string, error) {
	line, err := ss.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > maxLine {
		ss.err("line too long")
		return "", errors.New("pop3: line too long")
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (ss *session) send(line string) {
	_, _ = ss.w.WriteString(line)
	_, _ = ss.w.WriteString("\r\n")
	_ = ss.w.Flush()
}

func (ss *session) ok(msg string)  { ss.send("+OK " + msg) }
func (ss *session) err(msg string) { ss.send("-ERR " + msg) }

// dispatch handles one command; returns false to close the connection.
func (ss *session) dispatch(cmd, arg string) bool {
	switch cmd {
	case "CAPA":
		ss.send("+OK Capability list follows")
		caps := []string{"TOP", "UIDL", "RESP-CODES", "AUTH-RESP-CODE", "PIPELINING", "SASL PLAIN OAUTHBEARER XOAUTH2", "IMPLEMENTATION Kittiwake"}
		if !ss.authed {
			caps = append(caps, "USER")
		}
		for _, c := range caps {
			ss.send(c)
		}
		ss.send(".")
		return true
	case "QUIT":
		return ss.quit()
	case "NOOP":
		ss.ok("")
		return true
	}
	if !ss.authed {
		return ss.authCommand(cmd, arg)
	}
	switch cmd {
	case "STAT":
		n, size := 0, int64(0)
		for _, m := range ss.msgs {
			if !m.deleted {
				n++
				size += m.rec.Size
			}
		}
		ss.ok(fmt.Sprintf("%d %d", n, size))
	case "LIST":
		ss.listing(arg, func(i int, m *msgEntry) string { return fmt.Sprintf("%d %d", i, m.rec.Size) })
	case "UIDL":
		ss.listing(arg, func(i int, m *msgEntry) string { return fmt.Sprintf("%d %s", i, ss.uidl(m)) })
	case "RETR":
		m := ss.lookup(arg)
		if m == nil {
			return true
		}
		raw, err := ss.raw(m)
		if err != nil {
			return true
		}
		ss.ok(fmt.Sprintf("%d octets", len(raw)))
		ss.multiline(raw, -1)
	case "TOP":
		f := strings.Fields(arg)
		if len(f) != 2 {
			ss.err("syntax: TOP msg n")
			return true
		}
		n, err := strconv.Atoi(f[1])
		if err != nil || n < 0 {
			ss.err("syntax: TOP msg n")
			return true
		}
		m := ss.lookup(f[0])
		if m == nil {
			return true
		}
		raw, err := ss.raw(m)
		if err != nil {
			return true
		}
		ss.ok("")
		ss.multiline(raw, n)
	case "DELE":
		m := ss.lookup(arg)
		if m == nil {
			return true
		}
		m.deleted = true
		ss.ok("message deleted")
	case "RSET":
		for _, m := range ss.msgs {
			m.deleted = false
		}
		ss.ok("")
	case "USER", "PASS", "AUTH":
		ss.err("already authenticated")
	default:
		ss.err("unknown command")
	}
	return true
}

func (ss *session) authCommand(cmd, arg string) bool {
	switch cmd {
	case "USER":
		if arg == "" {
			ss.err("syntax: USER name")
			return true
		}
		ss.user = arg
		ss.ok("name is a valid mailbox")
		return true
	case "PASS":
		if ss.user == "" {
			ss.err("USER first")
			return true
		}
		ctx, cancel := context.WithTimeout(ss.ctx, serviceTimeout)
		act, err := ss.srv.auth.VerifyPassword(ctx, ss.user, arg, "pop3", ss.clientIP)
		cancel()
		return ss.finishAuth(act, err)
	case "AUTH":
		return ss.sasl(arg)
	case "STAT", "LIST", "UIDL", "RETR", "TOP", "DELE", "RSET":
		ss.err("[AUTH] authentication required")
		return true
	}
	ss.err("unknown command")
	return true
}

// sasl handles AUTH PLAIN / OAUTHBEARER / XOAUTH2 with an optional initial
// response (RFC 5034).
func (ss *session) sasl(arg string) bool {
	mech, ir, _ := strings.Cut(arg, " ")
	mech = strings.ToUpper(strings.TrimSpace(mech))
	switch mech {
	case "PLAIN", "OAUTHBEARER", "XOAUTH2":
	case "":
		ss.send("+OK")
		ss.send("PLAIN")
		ss.send("OAUTHBEARER")
		ss.send("XOAUTH2")
		ss.send(".")
		return true
	default:
		ss.err("[AUTH] unsupported mechanism")
		return true
	}
	ir = strings.TrimSpace(ir)
	if ir == "" {
		ss.send("+ ")
		line, err := ss.readLine()
		if err != nil {
			return false
		}
		if line == "*" {
			ss.err("[AUTH] cancelled")
			return true
		}
		ir = strings.TrimSpace(line)
	}
	if ir == "=" {
		ir = ""
	}
	data, err := base64.StdEncoding.DecodeString(ir)
	if err != nil {
		return ss.failed("[AUTH] bad base64")
	}
	ctx, cancel := context.WithTimeout(ss.ctx, serviceTimeout)
	defer cancel()
	var act actor.Actor
	switch mech {
	case "PLAIN":
		parts := bytes.Split(data, []byte{0})
		if len(parts) != 3 {
			return ss.failed("[AUTH] malformed PLAIN response")
		}
		act, err = ss.srv.auth.VerifyPassword(ctx, string(parts[1]), string(parts[2]), "pop3", ss.clientIP)
	case "OAUTHBEARER":
		user, token := parseOAuthBearer(data)
		act, err = ss.srv.auth.VerifyOAuthBearer(ctx, user, token, "pop3", ss.clientIP)
	case "XOAUTH2":
		user, token := parseXOAuth2(data)
		act, err = ss.srv.auth.VerifyOAuthBearer(ctx, user, token, "pop3", ss.clientIP)
	}
	return ss.finishAuth(act, err)
}

// parseOAuthBearer decodes RFC 7628: "n,a=user,\x01auth=Bearer tok\x01\x01".
func parseOAuthBearer(data []byte) (user, token string) {
	parts := strings.Split(string(data), "\x01")
	for _, kv := range strings.Split(parts[0], ",") {
		if strings.HasPrefix(kv, "a=") {
			user = strings.TrimPrefix(kv, "a=")
		}
	}
	for _, p := range parts[1:] {
		if strings.HasPrefix(strings.ToLower(p), "auth=bearer ") {
			token = strings.TrimSpace(p[len("auth=bearer "):])
		}
	}
	return user, token
}

// parseXOAuth2 decodes "user=u\x01auth=Bearer tok\x01\x01".
func parseXOAuth2(data []byte) (user, token string) {
	for _, p := range strings.Split(string(data), "\x01") {
		switch {
		case strings.HasPrefix(p, "user="):
			user = strings.TrimPrefix(p, "user=")
		case strings.HasPrefix(strings.ToLower(p), "auth=bearer "):
			token = strings.TrimSpace(p[len("auth=bearer "):])
		}
	}
	return user, token
}

// failed replies with msg after a failed authentication and closes the
// connection once the failure limit is reached.
func (ss *session) failed(msg string) bool {
	ss.fails++
	if ss.fails >= maxAuthFails {
		ss.err(msg + "; too many failures, closing")
		return false
	}
	ss.err(msg)
	return true
}

func (ss *session) finishAuth(act actor.Actor, err error) bool {
	ss.user = ""
	if err != nil {
		switch {
		case errors.Is(err, authsvc.ErrTOTPRequired):
			return ss.failed("[AUTH] this password requires a one-time code; use an application password or OAUTHBEARER")
		case errors.Is(err, authsvc.ErrAccountDisabled):
			return ss.failed("[AUTH] account disabled")
		case errors.Is(err, authsvc.ErrServiceDisabled):
			return ss.failed("[AUTH] POP3 is not enabled for this account")
		case errors.Is(err, authsvc.ErrNamespaceDisabled):
			return ss.failed("[AUTH] account disabled")
		case errors.Is(err, authsvc.ErrNotServed):
			return ss.failed("[SYS/TEMP] mailbox is not served by this server")
		case errors.Is(err, authsvc.ErrInvalidCredentials), errors.Is(err, authsvc.ErrInvalidToken), errors.Is(err, authsvc.ErrTokenExpired):
			return ss.failed("[AUTH] authentication failed")
		default:
			ss.srv.log.Warn("pop3 auth backend error", slog.String("error", err.Error()))
			return ss.failed("[SYS/TEMP] authentication backend error")
		}
	}
	if act.MailboxID == uuid.Nil {
		return ss.failed("[AUTH] account has no mailbox")
	}
	act.Protocol, act.ClientIP = "pop3", ss.clientIP
	ctx := actor.With(ss.ctx, act)
	if err := ss.load(ctx, act.MailboxID); err != nil {
		switch {
		case errors.Is(err, mailbox.ErrSealed):
			return ss.failed("[SYS/PERM] mailbox unavailable")
		case errors.Is(err, mailbox.ErrForbidden):
			return ss.failed("[AUTH] access denied")
		default:
			ss.srv.log.Warn("pop3 load failed", slog.String("error", err.Error()))
			return ss.failed("[SYS/TEMP] mailbox unavailable")
		}
	}
	ss.ctx, ss.act, ss.authed = ctx, act, true
	ss.ok(fmt.Sprintf("%d messages", len(ss.msgs)))
	return true
}

// load freezes the INBOX contents for the session.
func (ss *session) load(ctx context.Context, mb uuid.UUID) error {
	root, err := ss.srv.mb.Root(ctx, mb)
	if err != nil {
		return err
	}
	inbox := root.FolderByRole(index.RoleInbox)
	if inbox == nil {
		return mailbox.ErrNotFound
	}
	ss.inbox = inbox
	ss.msgs = nil
	var after uint32
	for {
		recs, more, err := ss.srv.mb.ListDesc(ctx, mb, inbox.ID, after, 1000)
		if err != nil {
			return err
		}
		for _, r := range recs {
			after = r.UID
			ss.msgs = append(ss.msgs, &msgEntry{rec: r})
		}
		if !more {
			break
		}
	}
	sort.Slice(ss.msgs, func(i, j int) bool { return ss.msgs[i].rec.UID < ss.msgs[j].rec.UID })
	return nil
}

func (ss *session) uidl(m *msgEntry) string {
	return fmt.Sprintf("%08x-%08x", ss.inbox.UIDValidity, m.rec.UID)
}

// lookup resolves a message number argument; replies -ERR itself on failure.
func (ss *session) lookup(arg string) *msgEntry {
	n, err := strconv.Atoi(strings.TrimSpace(arg))
	if err != nil || n < 1 || n > len(ss.msgs) {
		ss.err("no such message")
		return nil
	}
	m := ss.msgs[n-1]
	if m.deleted {
		ss.err("message deleted")
		return nil
	}
	return m
}

func (ss *session) listing(arg string, line func(int, *msgEntry) string) {
	if arg != "" {
		m := ss.lookup(arg)
		if m == nil {
			return
		}
		n, _ := strconv.Atoi(strings.TrimSpace(arg))
		ss.ok(line(n, m))
		return
	}
	ss.ok("")
	for i, m := range ss.msgs {
		if !m.deleted {
			ss.send(line(i+1, m))
		}
	}
	ss.send(".")
}

func (ss *session) raw(m *msgEntry) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ss.ctx, serviceTimeout)
	defer cancel()
	raw, err := ss.srv.mb.Raw(ctx, ss.act.MailboxID, m.rec.MessageID)
	if err != nil {
		if errors.Is(err, mailbox.ErrNotFound) {
			ss.err("no such message")
		} else {
			ss.srv.log.Warn("pop3 raw fetch failed", slog.String("error", err.Error()))
			ss.err("[SYS/TEMP] message unavailable")
		}
		return nil, err
	}
	return raw, nil
}

// multiline writes a byte-stuffed multi-line response. lines < 0 sends the
// whole message; otherwise the headers plus that many body lines (TOP).
func (ss *session) multiline(raw []byte, lines int) {
	body := raw
	var head []byte
	if lines >= 0 {
		sep := bytes.Index(raw, []byte("\r\n\r\n"))
		sepLen := 4
		if sep < 0 {
			sep = bytes.Index(raw, []byte("\n\n"))
			sepLen = 2
		}
		if sep < 0 {
			head, body = raw, nil
		} else {
			head, body = raw[:sep+sepLen], raw[sep+sepLen:]
		}
	}
	write := func(b []byte, limit int) {
		rd := bufio.NewReader(bytes.NewReader(b))
		n := 0
		for {
			if limit >= 0 && n >= limit {
				return
			}
			line, err := rd.ReadBytes('\n')
			if len(line) == 0 && err != nil {
				return
			}
			line = bytes.TrimRight(line, "\r\n")
			if bytes.HasPrefix(line, []byte(".")) {
				_, _ = ss.w.WriteString(".")
			}
			_, _ = ss.w.Write(line)
			_, _ = ss.w.WriteString("\r\n")
			n++
			if err != nil {
				return
			}
		}
	}
	if lines < 0 {
		write(body, -1)
	} else {
		write(head, -1)
		write(body, lines)
	}
	_, _ = ss.w.WriteString(".\r\n")
	_ = ss.w.Flush()
}

// quit applies pending deletions (UPDATE state) and closes.
func (ss *session) quit() bool {
	defer func() { _ = ss.w.Flush() }()
	if !ss.authed {
		ss.ok("bye")
		return false
	}
	var uids []uint32
	for _, m := range ss.msgs {
		if m.deleted {
			uids = append(uids, m.rec.UID)
		}
	}
	if len(uids) == 0 {
		ss.ok("bye")
		return false
	}
	ctx, cancel := context.WithTimeout(ss.ctx, serviceTimeout)
	defer cancel()
	refused := false
	for _, uid := range uids {
		if _, err := ss.srv.mb.SetFlagsByUID(ctx, ss.act.MailboxID, ss.inbox.ID, uid, mailbox.FlagOp{Add: []string{index.FlagDeleted}}); err != nil {
			if errors.Is(err, mailbox.ErrHold) || errors.Is(err, mailbox.ErrForbidden) {
				refused = true
				break
			}
			ss.srv.log.Warn("pop3 delete flag failed", slog.String("error", err.Error()))
			ss.err("[SYS/TEMP] some deleted messages not removed")
			return false
		}
	}
	if !refused {
		if _, err := ss.srv.mb.Expunge(ctx, ss.act.MailboxID, ss.inbox.ID, uids); err != nil {
			if errors.Is(err, mailbox.ErrHold) || errors.Is(err, mailbox.ErrForbidden) {
				refused = true
			} else {
				ss.srv.log.Warn("pop3 expunge failed", slog.String("error", err.Error()))
				ss.err("[SYS/TEMP] some deleted messages not removed")
				return false
			}
		}
	}
	if refused {
		// POP3 has no way to report a read-only mailbox at QUIT; the refusal
		// is audited by the mailbox service and logged here.
		ss.srv.log.Info("pop3 deletions refused (mailbox read-only)", slog.String("mailbox", ss.act.MailboxID.String()), slog.Int("messages", len(uids)))
		ss.ok("bye (deletions not permitted)")
		return false
	}
	ss.ok(fmt.Sprintf("bye, %d messages deleted", len(uids)))
	return false
}

var _ io.Closer = (*Server)(nil)
