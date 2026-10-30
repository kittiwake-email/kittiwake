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
// against a submission server of the namespace. TLS is the terminator's
// in front; its PROXY v2 header says whether the client used it, and AUTH
// is offered only then (RequireTLSProxy), as the backend does.
func (s *Server) ServeSubmission(l net.Listener) error {
	return s.serveTCP(l, ProtoSubmission, s.handleSMTP)
}

func (s *Server) handleSMTP(c net.Conn) {
	cl := newLineConn(c)
	info := clientOf(c)
	cl.deadline(s.cfg.PreAuthTimeout)
	if err := cl.send("220 " + s.cfg.Hostname + " ESMTP Kittiwake proxy"); err != nil {
		return
	}
	authOK := !s.cfg.RequireTLSProxy || info.tls
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
				prompt := "334 "
				if mech == "LOGIN" {
					prompt = "334 VXNlcm5hbWU6"
				}
				if err := cl.send(prompt); err != nil {
					return
				}
				resp, err := cl.readLine()
				if err != nil {
					return
				}
				if resp == "*" {
					_ = cl.send("501 5.7.0 Authentication cancelled")
					continue
				}
				ir = resp
			}
			raw, ok := decodeB64(ir)
			if !ok {
				_ = cl.send("501 5.5.2 Malformed SASL response")
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

// smtpLogin routes, opens the backend, replays EHLO and AUTH and relays
// the SASL exchange; on 235 the connection is spliced.
func (s *Server) smtpLogin(cl *lineConn, info client, username, mech string, ir []byte) bool {
	ctx := context.Background()
	t, err := s.Route(ctx, username)
	if err != nil {
		text, outcome := smtpRouteError(err)
		_ = cl.send(text)
		s.result(ProtoSubmission, outcome, username, info, t, err)
		return false
	}
	const unavailable = "454 4.7.0 No server available for this account, try again later"
	conn, err := s.connect(ctx, ProtoSubmission, t, username, info)
	if err != nil {
		_ = cl.send(unavailable)
		s.result(ProtoSubmission, "no-backend", username, info, t, err)
		return false
	}
	be := newLineConn(conn)
	be.deadline(s.cfg.ConnectTimeout)
	fail := func(outcome string, err error) bool {
		_ = be.Close()
		_ = cl.send(unavailable)
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
		for _, l := range reply {
			if err := cl.send(l); err != nil {
				_ = be.Close()
				return true
			}
		}
		last := reply[len(reply)-1]
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
