package proxy

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
)

// ServeSubmission fronts SMTP submission: EHLO and the odd pre-auth
// command are answered here, AUTH reads the login name and is replayed
// against a submission server of the namespace (or verified here and
// handed over). TLS is the terminator's in front; its PROXY v2 header
// says whether the client used it, and AUTH is offered only then
// (RequireTLSProxy), as the backend does.
func (s *Server) ServeSubmission(l net.Listener) error {
	return s.serveTCP(l, ProtoSubmission, s.handleSMTP)
}

const smtpUnavailable = "454 4.7.0 No server available for this account, try again later"

func (s *Server) handleSMTP(c net.Conn) {
	cl := newLineConn(c)
	info := clientOf(c)
	cl.deadline(s.cfg.PreAuthTimeout)
	if err := cl.send("220 " + s.cfg.Hostname + " ESMTP Kittiwake proxy"); err != nil {
		return
	}
	authOK := !s.cfg.RequireTLSProxy || info.tls
	// prompt sends a SASL server challenge and reads the response; ok is
	// false when the client cancelled or the connection ended (then
	// closed says which).
	prompt := func(challenge string) (resp string, ok, closed bool) {
		if err := cl.send(challenge); err != nil {
			return "", false, true
		}
		line, err := cl.readLine()
		if err != nil {
			return "", false, true
		}
		if line == "*" {
			_ = cl.send("501 5.7.0 Authentication cancelled")
			return "", false, false
		}
		return line, true, false
	}
	for {
		line, err := cl.readLine()
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(line, " ")
		cmd = strings.ToUpper(cmd)
		switch cmd {
		case "EHLO":
			_ = cl.send("250-" + s.cfg.Hostname)
			_ = cl.send("250-PIPELINING")
			_ = cl.send("250-SIZE " + strconv.FormatInt(s.cfg.MaxMessageSize, 10))
			_ = cl.send("250-8BITMIME")
			_ = cl.send("250-ENHANCEDSTATUSCODES")
			if authOK {
				_ = cl.send("250-AUTH PLAIN LOGIN OAUTHBEARER XOAUTH2")
			}
			_ = cl.send("250 SMTPUTF8")
		case "HELO":
			_ = cl.send("250 " + s.cfg.Hostname)
		case "NOOP":
			_ = cl.send("250 2.0.0 OK")
		case "RSET":
			_ = cl.send("250 2.0.0 Flushed")
		case "QUIT":
			_ = cl.send("221 2.0.0 Bye")
			return
		case "STARTTLS":
			_ = cl.send("502 5.5.1 STARTTLS is not offered; connect with TLS on port 465")
		case "MAIL", "RCPT", "DATA", "BDAT":
			_ = cl.send("530 5.7.0 Authentication required")
		case "AUTH":
			if !authOK {
				_ = cl.send("538 5.7.11 Encryption required; connect with TLS on port 465")
				continue
			}
			mech, ir, _ := strings.Cut(arg, " ")
			mech = strings.ToUpper(mech)
			switch mech {
			case "PLAIN", "LOGIN", "OAUTHBEARER", "XOAUTH2":
			default:
				_ = cl.send("504 5.5.4 Unrecognized authentication type")
				continue
			}
			if ir == "" {
				challenge := "334 "
				if mech == "LOGIN" {
					challenge = "334 VXNlcm5hbWU6"
				}
				resp, ok, closed := prompt(challenge)
				if closed {
					return
				}
				if !ok {
					continue
				}
				ir = resp
			}
			raw, ok := decodeB64(ir)
			if !ok {
				_ = cl.send("501 5.5.2 Malformed SASL response")
				continue
			}
			if s.local() && (mech == "PLAIN" || mech == "LOGIN") {
				var username, password string
				if mech == "PLAIN" {
					username, password, ok = saslPlain(raw)
					if !ok {
						_ = cl.send("535 5.7.8 Authentication failed")
						s.result(ProtoSubmission, "refused", "", info, Target{}, ErrBadCredentialFormat)
						continue
					}
				} else {
					username = string(raw)
					resp, ok, closed := prompt("334 UGFzc3dvcmQ6")
					if closed {
						return
					}
					if !ok {
						continue
					}
					pw, ok := decodeB64(resp)
					if !ok {
						_ = cl.send("501 5.5.2 Malformed SASL response")
						continue
					}
					password = string(pw)
				}
				if s.smtpLocal(cl, info, username, password) {
					return
				}
				continue
			}
			username, ok := saslUsername(mech, raw)
			if !ok {
				_ = cl.send("535 5.7.8 Authentication failed")
				s.result(ProtoSubmission, "refused", "", info, Target{}, ErrBadCredentialFormat)
				continue
			}
			if s.smtpLogin(cl, info, username, mech, raw) {
				return
			}
		default:
			_ = cl.send("502 5.5.2 Command not recognized")
		}
	}
}

// smtpLocal verifies the password here (and the send role, as the
// backend does at login) and hands a success over.
func (s *Server) smtpLocal(cl *lineConn, info client, username, password string) bool {
	ctx := context.Background()
	t, err := s.Route(ctx, username)
	if err != nil {
		return s.smtpRouteRefused(cl, info, username, t, err)
	}
	v, err := s.localVerify(ctx, ProtoSubmission, username, password, info)
	if err == nil && !canSend(v) {
		err = errNoSend
	}
	if err != nil {
		_ = cl.send(smtpAuthError(err))
		s.result(ProtoSubmission, "refused", username, info, t, err)
		return false
	}
	if v.Upgrade {
		return s.smtpRelay(cl, info, t, username, "PLAIN", []byte("\x00"+username+"\x00"+password), false)
	}
	ir, err := s.handoffIR(v, ProtoSubmission, info)
	if err != nil {
		_ = cl.send(smtpUnavailable)
		s.result(ProtoSubmission, "error", username, info, t, err)
		return false
	}
	return s.smtpRelay(cl, info, t, username, "XOAUTH2", ir, true)
}

func (s *Server) smtpRouteRefused(cl *lineConn, info client, username string, t Target, err error) bool {
	text, outcome := smtpRouteError(err)
	_ = cl.send(text)
	s.result(ProtoSubmission, outcome, username, info, t, err)
	return false
}

// smtpLogin routes and replays AUTH (pass-through).
func (s *Server) smtpLogin(cl *lineConn, info client, username, mech string, ir []byte) bool {
	t, err := s.Route(context.Background(), username)
	if err != nil {
		return s.smtpRouteRefused(cl, info, username, t, err)
	}
	return s.smtpRelay(cl, info, t, username, mech, ir, false)
}

// smtpRelay opens the backend, replays EHLO and AUTH and relays the SASL
// exchange; on 235 the connection is spliced. With handoff set the AUTH
// is the proxy's own: challenges are answered here and a refusal is
// reported as temporary.
func (s *Server) smtpRelay(cl *lineConn, info client, t Target, username, mech string, ir []byte, handoff bool) bool {
	ctx := context.Background()
	conn, err := s.connect(ctx, ProtoSubmission, t, username, info)
	if err != nil {
		_ = cl.send(smtpUnavailable)
		s.result(ProtoSubmission, "no-backend", username, info, t, err)
		return false
	}
	be := newLineConn(conn)
	be.deadline(s.cfg.ConnectTimeout)
	fail := func(outcome string, err error) bool {
		_ = be.Close()
		_ = cl.send(smtpUnavailable)
		s.result(ProtoSubmission, outcome, username, info, t, err)
		return false
	}
	if reply, err := smtpReply(be); err != nil || !strings.HasPrefix(reply[0], "220") {
		return fail("no-backend", errors.New("backend greeting: "+first(reply)+errText(err)))
	}
	if err := be.send("EHLO " + s.cfg.Hostname); err != nil {
		return fail("no-backend", err)
	}
	if reply, err := smtpReply(be); err != nil || !strings.HasPrefix(reply[0], "250") {
		return fail("no-backend", errors.New("backend EHLO: "+first(reply)+errText(err)))
	}
	be.deadline(s.cfg.PreAuthTimeout)
	if err := be.send("AUTH " + mech + " " + encodeB64(ir)); err != nil {
		return fail("no-backend", err)
	}
	for {
		reply, err := smtpReply(be)
		if err != nil {
			return fail("error", err)
		}
		last := reply[len(reply)-1]
		if handoff {
			switch {
			case strings.HasPrefix(last, "334"):
				if err := be.send(""); err != nil {
					return fail("error", err)
				}
				continue
			case strings.HasPrefix(last, "235"):
			default:
				_ = be.Close()
				_ = cl.send("454 4.7.0 The server refused the proxied login, try again later")
				s.result(ProtoSubmission, "handoff-refused", username, info, t, errors.New(last))
				return false
			}
		}
		for _, l := range reply {
			if err := cl.send(l); err != nil {
				_ = be.Close()
				return true
			}
		}
		switch {
		case strings.HasPrefix(last, "334"):
			resp, err := cl.readLine()
			if err != nil {
				_ = be.Close()
				return true
			}
			if err := be.send(resp); err != nil {
				_ = be.Close()
				return true
			}
		case strings.HasPrefix(last, "235"):
			s.result(ProtoSubmission, "routed", username, info, t, nil)
			splice(cl, be)
			return true
		default:
			_ = be.Close()
			s.result(ProtoSubmission, "refused", username, info, t, errors.New(last))
			return false
		}
	}
}

// smtpReply reads one possibly multi-line reply.
func smtpReply(be *lineConn) ([]string, error) {
	var lines []string
	for {
		l, err := be.readLine()
		if err != nil {
			return lines, err
		}
		lines = append(lines, l)
		if len(l) < 4 || l[3] != '-' {
			return lines, nil
		}
	}
}

func smtpRouteError(err error) (text, outcome string) {
	switch {
	case errors.Is(err, ErrUnknownDomain), errors.Is(err, ErrBadCredentialFormat):
		return "535 5.7.8 Authentication failed", "unknown-domain"
	case errors.Is(err, ErrDisabled):
		return "535 5.7.8 Account disabled", "disabled"
	}
	return "454 4.7.0 Routing failed, try again later", "error"
}

func first(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	return lines[0]
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return " (" + err.Error() + ")"
}
