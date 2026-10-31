package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"kittiwake/proto/imap"
)

// ServeIMAP fronts IMAP: it answers CAPABILITY, NOOP and LOGOUT itself,
// reads the login name out of LOGIN or the AUTHENTICATE initial response,
// and replays the exact login against a server of the namespace, or
// verifies the password here and hands the client over (local mode).
func (s *Server) ServeIMAP(l net.Listener) error {
	return s.serveTCP(l, ProtoIMAP, s.handleIMAP)
}

func imapCaps() string { return strings.Join(imap.Capabilities, " ") }

const imapUnavailable = " NO [UNAVAILABLE] No server available for this mailbox, try again later"

func (s *Server) handleIMAP(c net.Conn) {
	cl := newLineConn(c)
	info := clientOf(c)
	cl.deadline(s.cfg.PreAuthTimeout)
	if err := cl.send("* OK [CAPABILITY " + imapCaps() + "] " + s.cfg.Hostname + " Kittiwake proxy ready"); err != nil {
		return
	}
	for {
		line, err := cl.readLine()
		if err != nil {
			return
		}
		tag, cmd, rest := splitIMAP(line)
		if tag == "" {
			_ = cl.send("* BAD Missing tag")
			continue
		}
		switch cmd {
		case "CAPABILITY":
			_ = cl.send("* CAPABILITY " + imapCaps())
			_ = cl.send(tag + " OK CAPABILITY completed")
		case "NOOP":
			_ = cl.send(tag + " OK NOOP completed")
		case "LOGOUT":
			_ = cl.send("* BYE Logging out")
			_ = cl.send(tag + " OK LOGOUT completed")
			return
		case "STARTTLS":
			_ = cl.send(tag + " BAD STARTTLS is not offered; TLS is terminated in front of this server")
		case "LOGIN":
			args, err := parseAstrings(cl, rest, 2)
			if err != nil {
				_ = cl.send(tag + " BAD " + err.Error())
				continue
			}
			if s.local() {
				if s.imapLocal(cl, info, tag, args[0], args[1]) {
					return
				}
				continue
			}
			if s.imapLogin(cl, info, tag, args[0], func(be *lineConn) error {
				return be.write(imapLoginCommand(tag, args[0], args[1]))
			}) {
				return
			}
		case "AUTHENTICATE":
			f := strings.Fields(rest)
			if len(f) == 0 || len(f) > 2 {
				_ = cl.send(tag + " BAD AUTHENTICATE needs a mechanism")
				continue
			}
			mech := strings.ToUpper(f[0])
			var ir string
			if len(f) == 2 {
				ir = f[1]
			} else {
				if err := cl.send("+ "); err != nil {
					return
				}
				resp, err := cl.readLine()
				if err != nil {
					return
				}
				if resp == "*" {
					_ = cl.send(tag + " BAD Authentication cancelled")
					continue
				}
				ir = resp
			}
			raw, ok := decodeB64(ir)
			if !ok {
				_ = cl.send(tag + " BAD Malformed SASL response")
				continue
			}
			if s.local() && mech == "PLAIN" {
				user, pass, ok := saslPlain(raw)
				if !ok {
					_ = cl.send(tag + " NO [AUTHENTICATIONFAILED] Authentication failed")
					s.result(ProtoIMAP, "refused", "", info, Target{}, ErrBadCredentialFormat)
					continue
				}
				if s.imapLocal(cl, info, tag, user, pass) {
					return
				}
				continue
			}
			username, ok := saslUsername(mech, raw)
			if !ok {
				_ = cl.send(tag + " NO [AUTHENTICATIONFAILED] Authentication failed")
				s.result(ProtoIMAP, "refused", "", info, Target{}, ErrBadCredentialFormat)
				continue
			}
			if s.imapLogin(cl, info, tag, username, func(be *lineConn) error {
				return be.send(tag + " AUTHENTICATE " + mech + " " + encodeB64(raw))
			}) {
				return
			}
		default:
			if cmd == "" {
				_ = cl.send(tag + " BAD Missing command")
			} else {
				_ = cl.send(tag + " BAD Command not available before login")
			}
		}
	}
}

// imapLocal verifies the password here, refuses a failure in the
// backend's words and hands a success over with a handoff token (or the
// password itself when its hash is due for an upgrade).
func (s *Server) imapLocal(cl *lineConn, info client, tag, username, password string) bool {
	ctx := context.Background()
	t, err := s.Route(ctx, username)
	if err != nil {
		return s.imapRouteRefused(cl, info, tag, username, t, err)
	}
	v, err := s.localVerify(ctx, ProtoIMAP, username, password, info)
	if err != nil {
		code, text := imapAuthError(err)
		_ = cl.send(tag + " NO [" + code + "] " + text)
		s.result(ProtoIMAP, "refused", username, info, t, err)
		return false
	}
	if v.Upgrade {
		return s.imapRelay(cl, info, t, tag, username, false, func(be *lineConn) error {
			return be.write(imapLoginCommand(tag, username, password))
		})
	}
	ir, err := s.handoffIR(v, ProtoIMAP, info)
	if err != nil {
		_ = cl.send(tag + imapUnavailable)
		s.result(ProtoIMAP, "error", username, info, t, err)
		return false
	}
	return s.imapRelay(cl, info, t, tag, username, true, func(be *lineConn) error {
		return be.send(tag + " AUTHENTICATE XOAUTH2 " + encodeB64(ir))
	})
}

func (s *Server) imapRouteRefused(cl *lineConn, info client, tag, username string, t Target, err error) bool {
	code, text, outcome := imapRouteError(err)
	_ = cl.send(tag + " NO [" + code + "] " + text)
	s.result(ProtoIMAP, outcome, username, info, t, err)
	return false
}

// imapLogin routes username and replays the login with send against the
// backend (pass-through).
func (s *Server) imapLogin(cl *lineConn, info client, tag, username string, send func(be *lineConn) error) bool {
	t, err := s.Route(context.Background(), username)
	if err != nil {
		return s.imapRouteRefused(cl, info, tag, username, t, err)
	}
	return s.imapRelay(cl, info, t, tag, username, false, send)
}

// imapRelay opens the backend, sends the login and relays the exchange
// until the tagged reply. On success the connection is spliced and true
// is returned (the client loop ends); on failure the client gets the
// backend's reply and may try again. With handoff set the login is the
// proxy's own (a handoff token): its continuation requests are answered
// here and a refusal is reported as unavailable, since it means the
// backend does not trust this proxy, not that the client was wrong.
func (s *Server) imapRelay(cl *lineConn, info client, t Target, tag, username string, handoff bool, send func(be *lineConn) error) bool {
	ctx := context.Background()
	conn, err := s.connect(ctx, ProtoIMAP, t, username, info)
	if err != nil {
		_ = cl.send(tag + imapUnavailable)
		s.result(ProtoIMAP, "no-backend", username, info, t, err)
		return false
	}
	be := newLineConn(conn)
	be.deadline(s.cfg.ConnectTimeout)
	greeting, err := be.readLine()
	if err != nil || !strings.HasPrefix(greeting, "* OK") && !strings.HasPrefix(greeting, "* PREAUTH") {
		_ = be.Close()
		_ = cl.send(tag + imapUnavailable)
		s.result(ProtoIMAP, "no-backend", username, info, t, fmt.Errorf("backend greeting %q: %v", greeting, err))
		return false
	}
	be.deadline(s.cfg.PreAuthTimeout)
	if err := send(be); err != nil {
		_ = be.Close()
		_ = cl.send(tag + imapUnavailable)
		s.result(ProtoIMAP, "no-backend", username, info, t, err)
		return false
	}
	// Relay until the tagged reply; continuation requests go to the
	// client and its answers back (OAUTHBEARER error flows do this).
	for {
		line, err := be.readLine()
		if err != nil {
			_ = be.Close()
			_ = cl.send(tag + " NO [UNAVAILABLE] Server connection lost, try again later")
			s.result(ProtoIMAP, "error", username, info, t, err)
			return false
		}
		if strings.HasPrefix(line, "+") {
			if handoff {
				if err := be.send(""); err != nil {
					_ = be.Close()
					_ = cl.send(tag + imapUnavailable)
					s.result(ProtoIMAP, "error", username, info, t, err)
					return false
				}
				continue
			}
			if err := cl.send(line); err != nil {
				_ = be.Close()
				return true
			}
			resp, err := cl.readLine()
			if err != nil {
				_ = be.Close()
				return true
			}
			if err := be.send(resp); err != nil {
				_ = be.Close()
				return true
			}
			continue
		}
		if strings.HasPrefix(line, tag+" ") {
			status := strings.ToUpper(strings.SplitN(line[len(tag)+1:], " ", 2)[0])
			if status != "OK" {
				_ = be.Close()
				if handoff {
					_ = cl.send(tag + " NO [UNAVAILABLE] The server refused the proxied login, try again later")
					s.result(ProtoIMAP, "handoff-refused", username, info, t, errors.New(line))
					return false
				}
				_ = cl.send(line)
				s.result(ProtoIMAP, "refused", username, info, t, errors.New(line))
				return false
			}
			if err := cl.send(line); err != nil {
				_ = be.Close()
				return true
			}
			s.result(ProtoIMAP, "routed", username, info, t, nil)
			splice(cl, be)
			return true
		}
		if err := cl.send(line); err != nil {
			_ = be.Close()
			return true
		}
	}
}

func imapRouteError(err error) (code, text, outcome string) {
	switch {
	case errors.Is(err, ErrUnknownDomain), errors.Is(err, ErrBadCredentialFormat):
		return "AUTHENTICATIONFAILED", "Authentication failed", "unknown-domain"
	case errors.Is(err, ErrDisabled):
		return "AUTHENTICATIONFAILED", "Account disabled", "disabled"
	}
	return "UNAVAILABLE", "Routing failed, try again later", "error"
}

// imapLoginCommand renders LOGIN with both arguments as non-synchronizing
// literals (LITERAL+): any byte is safe and no continuation is needed.
func imapLoginCommand(tag, user, pass string) []byte {
	return []byte(tag + " LOGIN {" + strconv.Itoa(len(user)) + "+}\r\n" + user + " {" + strconv.Itoa(len(pass)) + "+}\r\n" + pass + "\r\n")
}

// splitIMAP splits "tag COMMAND rest".
func splitIMAP(line string) (tag, cmd, rest string) {
	tag, after, ok := strings.Cut(line, " ")
	if !ok {
		return line, "", ""
	}
	cmd, rest, _ = strings.Cut(after, " ")
	return tag, strings.ToUpper(cmd), rest
}

// parseAstrings reads n astring arguments (atom, quoted or literal) from
// rest, fetching synchronizing literals from the client as needed.
func parseAstrings(cl *lineConn, rest string, n int) ([]string, error) {
	var out []string
	for len(out) < n {
		rest = strings.TrimLeft(rest, " ")
		if rest == "" {
			return nil, errors.New("Missing argument")
		}
		switch rest[0] {
		case '"':
			var b strings.Builder
			i := 1
			for ; i < len(rest); i++ {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
					b.WriteByte(rest[i])
					continue
				}
				if rest[i] == '"' {
					break
				}
				b.WriteByte(rest[i])
			}
			if i >= len(rest) {
				return nil, errors.New("Unterminated quoted string")
			}
			out = append(out, b.String())
			rest = rest[i+1:]
		case '{':
			end := strings.IndexByte(rest, '}')
			if end < 0 || end != len(rest)-1 {
				return nil, errors.New("Malformed literal")
			}
			spec := rest[1:end]
			sync := !strings.HasSuffix(spec, "+")
			size, err := strconv.Atoi(strings.TrimSuffix(spec, "+"))
			if err != nil || size < 0 || size > maxLine*4 {
				return nil, errors.New("Malformed literal")
			}
			if sync {
				if err := cl.send("+ Ready for literal data"); err != nil {
					return nil, err
				}
			}
			data, err := cl.read(size)
			if err != nil {
				return nil, err
			}
			out = append(out, string(data))
			line, err := cl.readLine()
			if err != nil {
				return nil, err
			}
			rest = line
		default:
			atom, after, _ := strings.Cut(rest, " ")
			out = append(out, atom)
			rest = after
		}
	}
	if strings.TrimSpace(rest) != "" {
		return nil, errors.New("Too many arguments")
	}
	return out, nil
}
