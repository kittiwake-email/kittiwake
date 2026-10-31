package proxy

import (
	"context"
	"errors"

	authsvc "kittiwake/service/auth"
	"kittiwake/store/identity"
)

// Local verification (proxy.auth local). The proxy checks the password
// against the account itself: a failure is refused here, in the
// backend's words, logged and counted at the proxy and never relayed; a
// success is handed to the backend with a handoff token signed by the
// site's key (XOAUTH2 on the TCP protocols, Bearer on HTTP), which the
// backend accepts in place of the password and audits as a login. A
// password whose hash is due for an upgrade takes the pass-through path
// once, so the backend can rewrite it.

// local reports whether passwords are verified at the proxy.
func (s *Server) local() bool { return s.cfg.Auth != nil }

// localVerify checks a password for proto; the result carries what the
// handoff needs.
func (s *Server) localVerify(ctx context.Context, proto, username, password string, info client) (authsvc.Verified, error) {
	return s.cfg.Auth.CheckPassword(ctx, username, password, proto, info.ip)
}

// handoffIR builds the XOAUTH2 initial response carrying a handoff token
// for the verified login.
func (s *Server) handoffIR(v authsvc.Verified, proto string, info client) ([]byte, error) {
	tok, err := s.cfg.Auth.Handoff(v, proto, info.ip)
	if err != nil {
		return nil, err
	}
	return []byte("user=" + v.Actor.Username + "\x01auth=Bearer " + tok + "\x01\x01"), nil
}

// saslPlain splits a PLAIN response into its authentication name and
// password; an authorization identity other than the name is refused,
// as the backends do.
func saslPlain(response []byte) (username, password string, ok bool) {
	parts := splitN(response, 0, 3)
	if len(parts) != 3 {
		return "", "", false
	}
	if parts[0] != "" && parts[0] != parts[1] {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func splitN(b []byte, sep byte, n int) []string {
	var out []string
	start := 0
	for i := 0; i < len(b) && len(out) < n-1; i++ {
		if b[i] == sep {
			out = append(out, string(b[start:i]))
			start = i + 1
		}
	}
	return append(out, string(b[start:]))
}

// The refusals below are the backends' own texts for the same errors
// (proto/imap, proto/pop3, proto/submission), so a client sees the same
// answer whether the proxy or the backend verified.

func imapAuthError(err error) (code, text string) {
	switch {
	case errors.Is(err, authsvc.ErrTOTPRequired):
		return "AUTHENTICATIONFAILED", "This password requires a one-time code. Use an application password or OAUTHBEARER"
	case errors.Is(err, authsvc.ErrAccountDisabled), errors.Is(err, authsvc.ErrNamespaceDisabled):
		return "AUTHENTICATIONFAILED", "Account disabled"
	case errors.Is(err, authsvc.ErrServiceDisabled):
		return "AUTHENTICATIONFAILED", "IMAP is not enabled for this account"
	case errors.Is(err, authsvc.ErrInvalidCredentials):
		return "AUTHENTICATIONFAILED", "Authentication failed"
	}
	return "UNAVAILABLE", "Temporary authentication failure, try again later"
}

func pop3AuthError(err error) string {
	switch {
	case errors.Is(err, authsvc.ErrTOTPRequired):
		return "-ERR [AUTH] this password requires a one-time code; use an application password or OAUTHBEARER"
	case errors.Is(err, authsvc.ErrAccountDisabled), errors.Is(err, authsvc.ErrNamespaceDisabled):
		return "-ERR [AUTH] account disabled"
	case errors.Is(err, authsvc.ErrServiceDisabled):
		return "-ERR [AUTH] POP3 is not enabled for this account"
	case errors.Is(err, authsvc.ErrInvalidCredentials):
		return "-ERR [AUTH] authentication failed"
	}
	return "-ERR [SYS/TEMP] authentication backend error"
}

// errNoSend: the submission backend refuses accounts without the send
// role at login.
var errNoSend = errors.New("proxy: account may not send mail")

func smtpAuthError(err error) string {
	switch {
	case errors.Is(err, authsvc.ErrTOTPRequired):
		return "535 5.7.8 This password requires a one-time code; use an application password or OAUTHBEARER"
	case errors.Is(err, authsvc.ErrAccountDisabled), errors.Is(err, authsvc.ErrNamespaceDisabled):
		return "535 5.7.8 Account disabled"
	case errors.Is(err, errNoSend):
		return "535 5.7.8 Account may not send mail"
	case errors.Is(err, authsvc.ErrInvalidCredentials):
		return "535 5.7.8 Authentication failed"
	}
	return "454 4.7.0 Temporary authentication failure"
}

func canSend(v authsvc.Verified) bool { return v.Actor.HasRole(identity.RoleSend) }
