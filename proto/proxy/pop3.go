package proxy

import (
	"context"
	"errors"
	"net"
	"strings"
)

// ServePOP3 fronts POP3: CAPA, USER/PASS and AUTH are handled until the
// login name is known, then the login is replayed against a server of
// the namespace and the connection spliced.
func (s *Server) ServePOP3(l net.Listener) error {
	return s.serveTCP(l, ProtoPOP3, s.handlePOP3)
}

var pop3Caps = []string{"TOP", "UIDL", "RESP-CODES", "AUTH-RESP-CODE", "PIPELINING", "SASL PLAIN OAUTHBEARER XOAUTH2", "IMPLEMENTATION Kittiwake", "USER"}

func (s *Server) handlePOP3(c net.Conn) {
	cl := newLineConn(c)
	info := clientOf(c)
	cl.deadline(s.cfg.PreAuthTimeout)
	if err := cl.send("+OK " + s.cfg.Hostname + " Kittiwake proxy ready"); err != nil {
		return
	}
	user := ""
	for {
		line, err := cl.readLine()
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(line, " ")
		cmd = strings.ToUpper(cmd)
		switch cmd {
		case "CAPA":
			_ = cl.send("+OK Capability list follows")
			for _, c := range pop3Caps {
				_ = cl.send(c)
			}
			_ = cl.send(".")
		case "QUIT":
			_ = cl.send("+OK Bye")
			return
		case "STLS":
			_ = cl.send("-ERR STLS is not offered; TLS is terminated in front of this server")
		case "USER":
			if arg == "" {
				_ = cl.send("-ERR syntax: USER name")
				continue
			}
			user = arg
			_ = cl.send("+OK")
		case "PASS":
			if user == "" {
				_ = cl.send("-ERR USER first")
				continue
			}
			pass := arg
			if s.pop3Login(cl, info, user, func(be *lineConn) (string, error) {
				if err := be.send("USER " + user); err != nil {
					return "", err
				}
				reply, err := be.readLine()
				if err != nil {
					return "", err
				}
				if !strings.HasPrefix(reply, "+OK") {
					return reply, nil
				}
				if err := be.send("PASS " + pass); err != nil {
					return "", err
				}
				return be.readLine()
			}) {
				return
			}
		case "AUTH":
			if arg == "" {
				_ = cl.send("+OK")
				for _, m := range []string{"PLAIN", "OAUTHBEARER", "XOAUTH2"} {
					_ = cl.send(m)
				}
				_ = cl.send(".")
				continue
			}
			mech, ir, _ := strings.Cut(arg, " ")
			mech = strings.ToUpper(mech)
			if ir == "" {
				if err := cl.send("+ "); err != nil {
					return
				}
				resp, err := cl.readLine()
				if err != nil {
					return
				}
				if resp == "*" {
					_ = cl.send("-ERR [AUTH] cancelled")
					continue
				}
				ir = resp
			}
			raw, ok := decodeB64(ir)
			if !ok {
				_ = cl.send("-ERR [AUTH] bad base64")
				continue
			}
			username, ok := saslUsername(mech, raw)
			if !ok {
				_ = cl.send("-ERR [AUTH] authentication failed")
				s.result(ProtoPOP3, "refused", "", info, Target{}, ErrBadCredentialFormat)
				continue
			}
			if s.pop3Login(cl, info, username, func(be *lineConn) (string, error) {
				if err := be.send("AUTH " + mech + " " + encodeB64(raw)); err != nil {
					return "", err
				}
				for {
					reply, err := be.readLine()
					if err != nil {
						return "", err
					}
					if !strings.HasPrefix(reply, "+ ") && reply != "+" {
						return reply, nil
					}
					if err := cl.send(reply); err != nil {
						return "", err
					}
					resp, err := cl.readLine()
					if err != nil {
						return "", err
					}
					if err := be.send(resp); err != nil {
						return "", err
					}
				}
			}) {
				return
			}
		default:
			_ = cl.send("-ERR [AUTH] authentication required")
		}
	}
}

// pop3Login routes username, opens the backend, runs login (which
// returns the backend's final reply) and splices on +OK. True means the
// client loop ends.
func (s *Server) pop3Login(cl *lineConn, info client, username string, login func(be *lineConn) (string, error)) bool {
	ctx := context.Background()
	t, err := s.Route(ctx, username)
	if err != nil {
		text, outcome := pop3RouteError(err)
		_ = cl.send(text)
		s.result(ProtoPOP3, outcome, username, info, t, err)
		return false
	}
	const unavailable = "-ERR [SYS/TEMP] no server available for this mailbox, try again later"
	conn, err := s.connect(ctx, ProtoPOP3, t, username, info)
	if err != nil {
		_ = cl.send(unavailable)
		s.result(ProtoPOP3, "no-backend", username, info, t, err)
		return false
	}
	be := newLineConn(conn)
	be.deadline(s.cfg.ConnectTimeout)
	greeting, err := be.readLine()
	if err != nil || !strings.HasPrefix(greeting, "+OK") {
		_ = be.Close()
		_ = cl.send(unavailable)
		s.result(ProtoPOP3, "no-backend", username, info, t, err)
		return false
	}
	be.deadline(s.cfg.PreAuthTimeout)
	reply, err := login(be)
	if err != nil {
		_ = be.Close()
		_ = cl.send(unavailable)
		s.result(ProtoPOP3, "error", username, info, t, err)
		return false
	}
	if err := cl.send(reply); err != nil {
		_ = be.Close()
		return true
	}
	if !strings.HasPrefix(reply, "+OK") {
		_ = be.Close()
		s.result(ProtoPOP3, "refused", username, info, t, errors.New(reply))
		return false
	}
	s.result(ProtoPOP3, "routed", username, info, t, nil)
	splice(cl, be)
	return true
}

func pop3RouteError(err error) (text, outcome string) {
	switch {
	case errors.Is(err, ErrUnknownDomain), errors.Is(err, ErrBadCredentialFormat):
		return "-ERR [AUTH] authentication failed", "unknown-domain"
	case errors.Is(err, ErrDisabled):
		return "-ERR [AUTH] account disabled", "disabled"
	}
	return "-ERR [SYS/TEMP] routing failed, try again later", "error"
}
