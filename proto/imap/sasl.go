package imap

import (
	"bytes"
	"errors"
	"strings"

	"github.com/emersion/go-sasl"
)

const (
	saslPlain       = "PLAIN"
	saslOAuthBearer = "OAUTHBEARER"
	saslXOAuth2     = "XOAUTH2"
)

// xoauth2Server implements the server side of Google's XOAUTH2 mechanism:
// one client response "user=<u>\x01auth=Bearer <token>\x01\x01".
type xoauth2Server struct {
	done bool
	auth func(username, token string) error
}

func (x *xoauth2Server) Next(response []byte) ([]byte, bool, error) {
	if x.done {
		return nil, true, sasl.ErrUnexpectedClientResponse
	}
	if response == nil {
		return nil, false, nil // request the initial response
	}
	x.done = true
	var user, token string
	for _, kv := range bytes.Split(response, []byte{0x01}) {
		k, v, ok := strings.Cut(string(kv), "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "user":
			user = v
		case "auth":
			token = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(v, "Bearer"), "bearer"))
		}
	}
	if token == "" {
		return nil, true, errors.New("xoauth2: missing bearer token")
	}
	return nil, true, x.auth(user, token)
}
