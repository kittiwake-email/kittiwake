// Package socketmap serves Postfix lookups over the Sendmail-style socketmap
// protocol (netstrings: "<name> <key>" → "OK <value>", "NOTFOUND ",
// "TEMP <reason>", "PERM <reason>"). Maps: domains, mailboxes, aliases.
// See README.md "Postfix integration".
package socketmap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"dovetail/service/delivery"
	"dovetail/store/identity"
)

// Server answers socketmap queries.
type Server struct {
	ids *identity.Store
	del *delivery.Service
	log *slog.Logger
	// Timeout per lookup.
	Timeout time.Duration
	// MaxRequest bounds the netstring size.
	MaxRequest int

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	ln    net.Listener
}

// New returns a server.
func New(ids *identity.Store, del *delivery.Service, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{ids: ids, del: del, log: log, Timeout: 10 * time.Second, MaxRequest: 4096, conns: map[net.Conn]struct{}{}}
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
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go s.handle(c)
	}
}

// Close stops the listener and open connections.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	return err
}

func (s *Server) handle(c net.Conn) {
	defer func() {
		_ = c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()
	r := bufio.NewReader(c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Minute))
		req, err := readNetstring(r, s.MaxRequest)
		if err != nil {
			return
		}
		name, key, _ := strings.Cut(req, " ")
		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		resp := s.Lookup(ctx, name, key)
		cancel()
		_ = c.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := c.Write(netstring(resp)); err != nil {
			return
		}
	}
}

// Lookup answers one query. Exported for tests and for reuse.
func (s *Server) Lookup(ctx context.Context, name, key string) string {
	key = strings.TrimSpace(key)
	switch strings.ToLower(name) {
	case "domains":
		d, err := identity.NormalizeDomain(key)
		if err != nil {
			return "NOTFOUND "
		}
		v, err := s.ids.GetDomainCached(ctx, d)
		if err != nil {
			if errors.Is(err, identity.ErrNotFound) {
				return "NOTFOUND "
			}
			return "TEMP " + err.Error()
		}
		if !v.Enabled {
			return "NOTFOUND "
		}
		return "OK " + v.Name
	case "mailboxes":
		// Exists for Postfix = will be accepted or queued at LMTP time. A
		// disabled (hard) mailbox is reported unknown so the MTA rejects at
		// SMTP time instead of bouncing later; suspended, over quota and
		// on-hold mailboxes exist (LMTP defers them).
		targets, err := s.del.CheckRecipient(ctx, key, 0)
		if err != nil {
			switch {
			case errors.Is(err, delivery.ErrUnknownDomain), errors.Is(err, delivery.ErrUnknownRecipient),
				errors.Is(err, delivery.ErrAliasLoop), errors.Is(err, delivery.ErrMailboxDisabled):
				return "NOTFOUND "
			case errors.Is(err, delivery.ErrMailboxSuspended), errors.Is(err, delivery.ErrOverQuota), errors.Is(err, delivery.ErrHold), errors.Is(err, delivery.ErrUnavailable):
				targets, err = s.del.Resolve(ctx, key)
				if err != nil {
					return "TEMP " + err.Error()
				}
			default:
				return "TEMP " + err.Error()
			}
		}
		ids := make([]string, 0, len(targets))
		for _, t := range targets {
			ids = append(ids, t.MailboxID.String())
		}
		return "OK " + strings.Join(ids, ",")
	case "aliases":
		addr, _, err := identity.NormalizeAddress(key)
		if err != nil {
			return "NOTFOUND "
		}
		v, err := s.ids.GetAliasCached(ctx, addr)
		if err != nil {
			if errors.Is(err, identity.ErrNotFound) {
				return "NOTFOUND "
			}
			return "TEMP " + err.Error()
		}
		if !v.Enabled {
			return "NOTFOUND "
		}
		// Postfix virtual_alias_maps expects addresses. Mailbox destinations
		// have no address of their own, so the alias maps to itself (Postfix
		// then delivers it over LMTP, where Dovetail fans it out).
		var out []string
		self := false
		for _, d := range v.Destinations {
			if d.Address != "" {
				out = append(out, d.Address)
			} else {
				self = true
			}
		}
		if self {
			out = append([]string{addr}, out...)
		}
		return "OK " + strings.Join(out, ",")
	}
	return "PERM unknown map " + name
}

func netstring(s string) []byte {
	return []byte(strconv.Itoa(len(s)) + ":" + s + ",")
}

func readNetstring(r *bufio.Reader, max int) (string, error) {
	lenStr, err := r.ReadString(':')
	if err != nil {
		return "", err
	}
	n, err := strconv.Atoi(strings.TrimSuffix(lenStr, ":"))
	if err != nil || n < 0 || n > max {
		return "", fmt.Errorf("socketmap: bad netstring length %q", lenStr)
	}
	buf := make([]byte, n+1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if buf[n] != ',' {
		return "", errors.New("socketmap: missing netstring terminator")
	}
	return string(buf[:n]), nil
}
