package submission

import (
	"bytes"
	"errors"
	"strings"

	"github.com/emersion/go-sasl"
)

// loginServer is the server side of the LOGIN mechanism (draft-murchison-
// sasl-login): username and password in two responses. Outlook and many
// devices still use it.
type loginServer struct {
	auth     func(username, password string) error
	username string
	step     int
}

func (l *loginServer) Next(response []byte) ([]byte, bool, error) {
	switch l.step {
	case 0:
		l.step = 1
		if response == nil {
			return []byte("Username:"), false, nil
		}
		fallthrough // initial response carries the username
	case 1:
		l.username, l.step = string(response), 2
		return []byte("Password:"), false, nil
	case 2:
		l.step = 3
		return nil, true, l.auth(l.username, string(response))
	}
	return nil, true, sasl.ErrUnexpectedClientResponse
}

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
