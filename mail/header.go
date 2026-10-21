package mail

import (
	"bytes"
	"mime"
	"net/mail"
	"strings"
	"time"
)

// rawHeader is one header field as found in the raw bytes.
type rawHeader struct {
	name  string
	value string // unfolded, trimmed, not decoded
}

// headerBlock finds the end of the header block: the index just after the
// blank line that terminates it. If there is no blank line, len(raw).
func headerBlock(raw []byte) int {
	i := 0
	for i < len(raw) {
		j := bytes.IndexByte(raw[i:], '\n')
		if j < 0 {
			return len(raw)
		}
		line := raw[i : i+j]
		if len(line) == 0 || (len(line) == 1 && line[0] == '\r') {
			return i + j + 1
		}
		i += j + 1
	}
	return len(raw)
}

// parseHeaders tolerantly parses the header fields in block (which may
// include the terminating blank line).
func parseHeaders(block []byte) []rawHeader {
	var out []rawHeader
	var cur *rawHeader
	i := 0
	for i < len(block) {
		j := bytes.IndexByte(block[i:], '\n')
		var line []byte
		if j < 0 {
			line = block[i:]
			i = len(block)
		} else {
			line = block[i : i+j]
			i += j + 1
		}
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			break
		}
		if line[0] == ' ' || line[0] == '\t' {
			if cur != nil {
				cur.value += " " + strings.TrimSpace(string(line))
			}
			continue
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			cur = nil
			continue
		}
		name := strings.TrimSpace(string(line[:colon]))
		if name == "" || strings.ContainsAny(name, " \t") {
			cur = nil
			continue
		}
		out = append(out, rawHeader{name: name, value: strings.TrimSpace(string(line[colon+1:]))})
		cur = &out[len(out)-1]
	}
	return out
}

func (h rawHeader) is(name string) bool { return strings.EqualFold(h.name, name) }

func firstHeader(hs []rawHeader, name string) (string, bool) {
	for _, h := range hs {
		if h.is(name) {
			return h.value, true
		}
	}
	return "", false
}

func allHeaders(hs []rawHeader, name string) []string {
	var out []string
	for _, h := range hs {
		if h.is(name) {
			out = append(out, h.value)
		}
	}
	return out
}

var wordDecoder = &mime.WordDecoder{CharsetReader: charsetReader}

// decodeWords decodes RFC 2047 encoded words; on failure returns the input
// sanitized to valid UTF-8.
func decodeWords(s string) string {
	d, err := wordDecoder.DecodeHeader(s)
	if err != nil {
		return sanitizeUTF8(s)
	}
	return sanitizeUTF8(d)
}

var addrParser = &mail.AddressParser{WordDecoder: wordDecoder}

// parseAddresses parses an address list tolerantly.
func parseAddresses(v string) []Address {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	list, err := addrParser.ParseList(v)
	if err != nil {
		// Try per-item.
		var out []Address
		ok := false
		for _, item := range splitAddressList(v) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			a, err := addrParser.Parse(item)
			if err != nil {
				out = append(out, Address{Name: decodeWords(item)})
				continue
			}
			ok = true
			out = append(out, Address{Name: sanitizeUTF8(a.Name), Email: a.Address})
		}
		if !ok && len(out) > 1 {
			return []Address{{Name: decodeWords(v)}}
		}
		return out
	}
	out := make([]Address, 0, len(list))
	for _, a := range list {
		out = append(out, Address{Name: sanitizeUTF8(a.Name), Email: a.Address})
	}
	return out
}

// splitAddressList splits on commas outside quotes and angle brackets.
func splitAddressList(v string) []string {
	var out []string
	depth, quoted := 0, false
	start := 0
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '"':
			quoted = !quoted
		case '<':
			if !quoted {
				depth++
			}
		case '>':
			if !quoted && depth > 0 {
				depth--
			}
		case ',':
			if !quoted && depth == 0 {
				out = append(out, v[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, v[start:])
	return out
}

// parseMsgIDs extracts message ids (without angle brackets) from a header.
func parseMsgIDs(v string) []string {
	var out []string
	for _, f := range strings.Fields(v) {
		f = strings.TrimSpace(f)
		f = strings.TrimPrefix(f, "<")
		f = strings.TrimSuffix(f, ">")
		f = strings.TrimSuffix(f, ",")
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

var dateLayouts = []string{
	"Mon, 2 Jan 2006 15:04:05 -0700 (MST)",
	"2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"Mon, 2 Jan 2006 15:04 -0700",
	"2 Jan 2006 15:04 -0700",
	"Mon, 2 Jan 06 15:04:05 -0700",
	"Mon Jan 2 15:04:05 2006",
	"Mon Jan 2 15:04:05 -0700 2006",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02T15:04:05Z07:00",
	"Mon, 2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04:05",
}

// parseDate parses a Date header, trying common broken forms too.
func parseDate(v string) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	if t, err := mail.ParseDate(v); err == nil {
		return t
	}
	// Strip a trailing comment "(UTC)" etc.
	if i := strings.Index(v, "("); i > 0 {
		v2 := strings.TrimSpace(v[:i])
		if t, err := mail.ParseDate(v2); err == nil {
			return t
		}
		v = v2
	}
	for _, l := range dateLayouts {
		if t, err := time.Parse(l, v); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseMediaType is a tolerant mime.ParseMediaType.
func parseMediaType(v string) (mediatype string, params map[string]string) {
	mt, p, err := mime.ParseMediaType(v)
	if err == nil {
		return strings.ToLower(mt), p
	}
	// Fallback: take everything before ';' as the type, and do a naive
	// key=value scan for params.
	params = map[string]string{}
	parts := strings.Split(v, ";")
	mt = strings.ToLower(strings.TrimSpace(parts[0]))
	for _, kv := range parts[1:] {
		kv = strings.TrimSpace(kv)
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(kv[:eq]))
		val := strings.TrimSpace(kv[eq+1:])
		val = strings.Trim(val, "\"")
		if _, ok := params[k]; !ok {
			params[k] = val
		}
	}
	if mt == "" {
		mt = "application/octet-stream"
	}
	return mt, params
}
