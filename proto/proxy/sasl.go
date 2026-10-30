package proxy

import (
	"bytes"
	"encoding/base64"
	"strings"
)

// The proxy does not run SASL: it reads the login name out of the
// client's first response so it can route, then hands the untouched
// exchange to the backend.

// saslUsername extracts the authentication name from the initial (or
// first) client response of a mechanism. For LOGIN the first response is
// the username itself.
func saslUsername(mech string, response []byte) (string, bool) {
	switch strings.ToUpper(mech) {
	case "PLAIN":
		parts := bytes.SplitN(response, []byte{0}, 3)
		if len(parts) != 3 {
			return "", false
		}
		return string(parts[1]), true
	case "LOGIN":
		return string(response), len(response) > 0
	case "OAUTHBEARER":
		// gs2-header "n,a=user@domain,^A..." (RFC 7628)
		s := string(response)
		for _, part := range strings.Split(s, ",") {
			if strings.HasPrefix(part, "a=") {
				return strings.TrimPrefix(part, "a="), true
			}
		}
		return "", false
	case "XOAUTH2":
		for _, kv := range bytes.Split(response, []byte{0x01}) {
			k, v, ok := strings.Cut(string(kv), "=")
			if ok && strings.EqualFold(k, "user") {
				return v, true
			}
		}
		return "", false
	}
	return "", false
}

// decodeB64 decodes a SASL response; "=" is the empty response.
func decodeB64(s string) ([]byte, bool) {
	s = strings.TrimSpace(s)
	if s == "=" {
		return []byte{}, true
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	return b, true
}

func encodeB64(b []byte) string {
	if len(b) == 0 {
		return "="
	}
	return base64.StdEncoding.EncodeToString(b)
}
