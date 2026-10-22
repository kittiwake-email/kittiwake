package mail

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"sort"
	"strings"
)

// Render builds an RFC 5322 message from a document. It is deterministic:
// the same document (and attachment content) always yields the same bytes,
// which is what Document.Size records and what IMAP and JMAP serve.
//
// The top-level header block is emitted verbatim except for MIME-Version,
// Content-Type and Content-Transfer-Encoding, which are regenerated from the
// part tree (bodies are stored as UTF-8 and attachments are re-encoded).
// Received, Message-ID, DKIM and every other header are untouched, so the
// delivery trace survives; DKIM signatures over the body will not verify on
// the rendered form (the MTA verified them on arrival).
func Render(doc *Document, fetch func(hash string) ([]byte, error)) ([]byte, error) {
	if doc == nil || doc.Root == nil {
		return nil, errors.New("mail: empty document")
	}
	var b bytes.Buffer
	writeTopHeaders(&b, doc)
	r := &renderer{doc: doc, fetch: fetch}
	if err := r.body(&b, doc.Root, true); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// regenerated lists the top-level headers Render replaces.
var regenerated = map[string]bool{"mime-version": true, "content-type": true, "content-transfer-encoding": true, "content-disposition": true, "content-id": true}

// writeTopHeaders copies the original header lines minus the regenerated
// ones and appends the MIME headers of the root part.
func writeTopHeaders(b *bytes.Buffer, doc *Document) {
	raw := doc.RawHeaders
	raw = strings.TrimRight(raw, "\r\n")
	lines := strings.Split(raw, "\n")
	skip := false
	for _, l := range lines {
		l = strings.TrimRight(l, "\r")
		if l == "" {
			continue
		}
		if l[0] == ' ' || l[0] == '\t' {
			if !skip {
				b.WriteString(l)
				b.WriteString("\r\n")
			}
			continue
		}
		name := l
		if i := strings.IndexByte(l, ':'); i >= 0 {
			name = l[:i]
		}
		skip = regenerated[strings.ToLower(strings.TrimSpace(name))]
		if !skip {
			b.WriteString(l)
			b.WriteString("\r\n")
		}
	}
	b.WriteString("MIME-Version: 1.0\r\n")
	writePartHeaders(b, doc, doc.Root)
	b.WriteString("\r\n")
}

type renderer struct {
	doc   *Document
	fetch func(string) ([]byte, error)
}

// leafContent returns a leaf's content and the transfer encoding to use.
func (r *renderer) leafContent(p *Part) ([]byte, string, error) {
	switch {
	case p.IsBody && p.Type == "text" && p.Subtype == "plain":
		return []byte(crlf(r.doc.TextBody)), textEncoding(r.doc.TextBody), nil
	case p.IsBody && p.Type == "text" && p.Subtype == "html":
		return []byte(crlf(r.doc.HTMLBody)), textEncoding(r.doc.HTMLBody), nil
	case p.Attachment != "":
		if r.fetch == nil {
			return nil, "", fmt.Errorf("mail: no fetcher for attachment %s", p.Attachment)
		}
		data, err := r.fetch(p.Attachment)
		if err != nil {
			return nil, "", fmt.Errorf("mail: attachment %s: %w", p.Attachment, err)
		}
		return data, nonTextEncoding(p), nil
	default:
		return p.Data, nonTextEncoding(p), nil
	}
}

// nonTextEncoding picks the transfer encoding for stored content.
func nonTextEncoding(p *Part) string {
	if p.Type == "message" { // message/* must not be base64 (RFC 2046)
		return "8bit"
	}
	return "base64"
}

// textEncoding picks 7bit when the UTF-8 text is plain ASCII with short
// lines, else quoted-printable.
func textEncoding(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 || s[i] == 0 {
			return "quoted-printable"
		}
	}
	for _, l := range strings.Split(s, "\n") {
		if len(l) > 990 {
			return "quoted-printable"
		}
	}
	return "7bit"
}

func crlf(s string) string {
	if !strings.Contains(s, "\n") {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\n", "\r\n")
}

// writePartHeaders emits the MIME headers of a part.
func writePartHeaders(b *bytes.Buffer, doc *Document, p *Part) {
	params := map[string]string{}
	for k, v := range p.Params {
		params[strings.ToLower(k)] = v
	}
	if p.IsBody {
		params["charset"] = "utf-8"
	}
	if p.Type == "multipart" && params["boundary"] == "" {
		params["boundary"] = "dovetail-" + p.Path
	}
	ct := p.Type + "/" + p.Subtype
	if p.Subtype == "" {
		ct = p.Type
	}
	b.WriteString("Content-Type: " + mime.FormatMediaType(ct, params) + "\r\n")
	if p.Type != "multipart" {
		enc := "7bit"
		switch {
		case p.IsBody && p.Subtype == "plain":
			enc = textEncoding(doc.TextBody)
		case p.IsBody && p.Subtype == "html":
			enc = textEncoding(doc.HTMLBody)
		default:
			enc = nonTextEncoding(p)
		}
		b.WriteString("Content-Transfer-Encoding: " + enc + "\r\n")
	}
	if p.Disposition != "" || p.Filename != "" {
		disp := p.Disposition
		if disp == "" {
			disp = "attachment"
		}
		dp := map[string]string{}
		if p.Filename != "" {
			dp["filename"] = p.Filename
		}
		b.WriteString("Content-Disposition: " + mime.FormatMediaType(disp, dp) + "\r\n")
	}
	if p.ContentID != "" {
		b.WriteString("Content-ID: <" + p.ContentID + ">\r\n")
	}
}

// body writes the body of p (headers of nested parts are written here too).
func (r *renderer) body(b *bytes.Buffer, p *Part, root bool) error {
	if p.Type == "multipart" {
		boundary := p.Params["boundary"]
		if boundary == "" {
			boundary = "dovetail-" + p.Path
		}
		for _, c := range p.Children {
			b.WriteString("--" + boundary + "\r\n")
			writePartHeaders(b, r.doc, c)
			b.WriteString("\r\n")
			if err := r.body(b, c, false); err != nil {
				return err
			}
			b.WriteString("\r\n")
		}
		b.WriteString("--" + boundary + "--\r\n")
		return nil
	}
	data, enc, err := r.leafContent(p)
	if err != nil {
		return err
	}
	// No line break is added after the content: inside a multipart the CRLF
	// before the next boundary belongs to the boundary, and at top level the
	// body ends where the data ends, so parsing the rendered message yields
	// exactly the stored body.
	switch enc {
	case "base64":
		b.Write(encodeBase64(data))
	case "quoted-printable":
		w := quotedprintable.NewWriter(b)
		_, _ = w.Write(data)
		_ = w.Close()
	default:
		b.Write(data)
	}
	return nil
}

// encodeBase64 encodes with 76-column CRLF lines.
func encodeBase64(data []byte) []byte {
	enc := base64.StdEncoding.EncodeToString(data)
	var b bytes.Buffer
	for len(enc) > 76 {
		b.WriteString(enc[:76])
		b.WriteString("\r\n")
		enc = enc[76:]
	}
	b.WriteString(enc)
	b.WriteString("\r\n")
	return b.Bytes()
}

// AttachmentHashes returns the distinct attachment hashes referenced by the
// document's part tree, in document order (for reference counting).
func AttachmentHashes(doc *Document) []string {
	var out []string
	seen := map[string]bool{}
	var walk func(p *Part)
	walk = func(p *Part) {
		if p == nil {
			return
		}
		if p.Attachment != "" && !seen[p.Attachment] {
			seen[p.Attachment] = true
			out = append(out, p.Attachment)
		}
		for _, c := range p.Children {
			walk(c)
		}
	}
	walk(doc.Root)
	sort.Strings(out)
	return out
}
