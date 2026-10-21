package mail

import (
	"bytes"
	"encoding/base64"
	"io"
	"mime/quotedprintable"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/encoding/unicode"
)

// lookupCharset returns an encoding for an IANA charset label. Unknown
// labels fall back to ISO-8859-1. nil means UTF-8/ASCII (no conversion).
func lookupCharset(label string) encoding.Encoding {
	label = strings.ToLower(strings.TrimSpace(label))
	switch label {
	case "", "utf-8", "utf8", "us-ascii", "ascii", "ansi_x3.4-1968":
		return nil
	case "latin1", "latin-1":
		return charmap.ISO8859_1
	case "gb2312", "gbk", "gb18030":
		e, err := ianaindex.MIME.Encoding("gb18030")
		if err == nil && e != nil {
			return e
		}
	case "utf-16":
		return unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
	}
	e, err := ianaindex.MIME.Encoding(label)
	if err != nil || e == nil {
		e, err = ianaindex.IANA.Encoding(label)
	}
	if err != nil || e == nil {
		return charmap.ISO8859_1
	}
	if e == unicode.UTF8 {
		return nil
	}
	return e
}

// charsetReader is the mime.WordDecoder CharsetReader.
func charsetReader(label string, r io.Reader) (io.Reader, error) {
	e := lookupCharset(label)
	if e == nil {
		return r, nil
	}
	return e.NewDecoder().Reader(r), nil
}

// toUTF8 converts data in the given charset to a sanitized UTF-8 string.
func toUTF8(data []byte, label string) string {
	e := lookupCharset(label)
	if e == nil {
		return sanitizeUTF8(string(data))
	}
	out, err := e.NewDecoder().Bytes(data)
	if err != nil {
		// Decode as much as possible, then fall back to latin1 for the rest.
		return sanitizeUTF8(string(data))
	}
	return sanitizeUTF8(string(out))
}

// sanitizeUTF8 replaces invalid sequences with U+FFFD and drops NULs.
func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) && !strings.ContainsRune(s, 0) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			b.WriteRune(utf8.RuneError)
			i++
			continue
		}
		if r != 0 {
			b.WriteRune(r)
		}
		i += size
	}
	return b.String()
}

// decodeTransfer decodes a content-transfer-encoding tolerantly. ok reports
// whether the decode was clean (no errors); data is always best effort.
func decodeTransfer(body []byte, enc string) (data []byte, ok bool) {
	switch enc {
	case "base64":
		return decodeBase64(body)
	case "quoted-printable":
		r := quotedprintable.NewReader(bytes.NewReader(body))
		out, err := io.ReadAll(r)
		return out, err == nil
	default:
		return body, true
	}
}

func isBase64Byte(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '+' || c == '/' || c == '='
}

// decodeBase64 strips whitespace and decodes; it tolerates missing padding.
func decodeBase64(body []byte) ([]byte, bool) {
	clean := make([]byte, 0, len(body))
	dirty := false
	for _, c := range body {
		switch {
		case isBase64Byte(c):
			clean = append(clean, c)
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
		default:
			dirty = true
		}
	}
	// Strip padding and decode raw; handles both padded and unpadded.
	trimmed := bytes.TrimRight(clean, "=")
	out, err := base64.RawStdEncoding.DecodeString(string(trimmed))
	if err != nil {
		// Decode the longest valid prefix (multiple of 4 chars of unpadded).
		n := len(trimmed) - len(trimmed)%4
		for n > 0 {
			out, err = base64.RawStdEncoding.DecodeString(string(trimmed[:n]))
			if err == nil {
				break
			}
			n -= 4
		}
		if err != nil {
			return nil, false
		}
		return out, false
	}
	return out, !dirty
}
