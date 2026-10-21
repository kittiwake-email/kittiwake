package mail

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func crlf(s string) string { return strings.ReplaceAll(s, "\n", "\r\n") }

func b64Lines(data []byte, n int, eol string, trailing bool) string {
	enc := base64.StdEncoding.EncodeToString(data)
	var b strings.Builder
	for i := 0; i < len(enc); i += n {
		j := i + n
		if j > len(enc) {
			j = len(enc)
		}
		if i > 0 {
			b.WriteString(eol)
		}
		b.WriteString(enc[i:j])
	}
	if trailing {
		b.WriteString(eol)
	}
	return b.String()
}

func sha(data []byte) string { s := sha256.Sum256(data); return hex.EncodeToString(s[:]) }

func fetcher(atts []Attachment) func(string) ([]byte, error) {
	m := map[string][]byte{}
	for _, a := range atts {
		m[a.Hash] = a.Data
	}
	return func(h string) ([]byte, error) {
		d, ok := m[h]
		if !ok {
			return nil, fmt.Errorf("no attachment %s", h)
		}
		return d, nil
	}
}

func roundTrip(t *testing.T, raw []byte, p *Parsed) {
	t.Helper()
	out, err := Reconstruct(p.Skeleton, fetcher(p.Attachments))
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("reconstruct mismatch:\n got %q\nwant %q", out, raw)
	}
}

func bigData(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	r.Read(b)
	return b
}

func TestSimpleText(t *testing.T) {
	raw := []byte(crlf(`From: "Wido den Hollander" <wido@example.org>
To: Someone <someone@example.net>, other@example.net
Subject: =?UTF-8?B?SGVsbG8g8J+Ygw==?= world
Date: Tue, 21 Oct 2026 12:00:00 +0200
Message-ID: <abc@example.org>
In-Reply-To: <parent@example.org>
References: <root@example.org>
 <parent@example.org>
X-Folded: first
  second
Content-Type: text/plain; charset=utf-8

Hello,
this is the body.
`))
	p, err := Parse(raw, Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := p.Doc
	if d.Subject != "Hello 😃 world" {
		t.Errorf("subject %q", d.Subject)
	}
	if len(d.From) != 1 || d.From[0].Name != "Wido den Hollander" || d.From[0].Email != "wido@example.org" {
		t.Errorf("from %+v", d.From)
	}
	if len(d.To) != 2 || d.To[1].Email != "other@example.net" {
		t.Errorf("to %+v", d.To)
	}
	if d.MessageID != "abc@example.org" || len(d.InReplyTo) != 1 || len(d.References) != 2 || d.References[1] != "parent@example.org" {
		t.Errorf("ids %q %v %v", d.MessageID, d.InReplyTo, d.References)
	}
	if d.Date.IsZero() || d.Date.Hour() != 12 {
		t.Errorf("date %v", d.Date)
	}
	if d.TextBody != "Hello,\nthis is the body.\n" {
		t.Errorf("text %q", d.TextBody)
	}
	if d.Preview != "Hello, this is the body." {
		t.Errorf("preview %q", d.Preview)
	}
	var folded string
	for _, h := range d.Headers {
		if h.Name == "X-Folded" {
			folded = h.Value
		}
	}
	if folded != "first second" {
		t.Errorf("folded %q", folded)
	}
	if !strings.HasSuffix(d.RawHeaders, "charset=utf-8\r\n\r\n") {
		t.Errorf("raw headers %q", d.RawHeaders)
	}
	if !d.Root.IsBody || d.Root.Type != "text" || d.Root.Subtype != "plain" || d.HasAttachments || len(p.Attachments) != 0 || p.Externalized {
		t.Errorf("root %+v", d.Root)
	}
	if p.Skeleton[5] != frameLiteral || len(p.Skeleton) != 5+5+len(raw) {
		t.Errorf("skeleton should be a single literal")
	}
	roundTrip(t, raw, p)
}

func TestAlternative(t *testing.T) {
	raw := []byte(crlf(`From: a@example.org
Subject: alt
Content-Type: multipart/alternative; boundary="b1"

preamble
--b1
Content-Type: text/plain; charset=us-ascii

Plain   text
here.
--b1
Content-Type: text/html; charset=us-ascii

<html><body><p>HTML <b>text</b></p><p>here.</p></body></html>
--b1--
epilogue
`))
	p, err := Parse(raw, Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := p.Doc
	if d.TextBody != "Plain   text\nhere." {
		t.Errorf("text %q", d.TextBody)
	}
	if !strings.Contains(d.HTMLBody, "<b>text</b>") {
		t.Errorf("html %q", d.HTMLBody)
	}
	if d.Preview != "Plain text here." {
		t.Errorf("preview %q", d.Preview)
	}
	if d.HasAttachments || len(d.Root.Children) != 2 || d.Root.Children[0].Path != "1" || d.Root.Children[1].Path != "2" {
		t.Errorf("tree %+v", d.Root)
	}
	if !d.Root.Children[0].IsBody || !d.Root.Children[1].IsBody {
		t.Error("bodies not flagged")
	}
	roundTrip(t, raw, p)
}

func TestHTMLOnlyDerivesText(t *testing.T) {
	raw := []byte(crlf(`Content-Type: text/html; charset=utf-8

<html><head><style>p{}</style></head><body><h1>Title</h1><p>One &amp; two</p><script>x()</script><div>Three<br>Four</div></body></html>
`))
	p, _ := Parse(raw, Options{})
	if p.Doc.TextBody != "Title\nOne & two\nThree\nFour" {
		t.Errorf("text %q", p.Doc.TextBody)
	}
	if p.Doc.HTMLBody == "" {
		t.Error("html empty")
	}
	roundTrip(t, raw, p)
}

func TestMixedWithAttachments(t *testing.T) {
	pdf := bigData(1, 10000)
	icon := bigData(2, 100)
	raw := []byte(crlf(`From: a@example.org
Subject: files
Content-Type: multipart/mixed; boundary="outer"

--outer
Content-Type: text/plain

See attached.
--outer
Content-Type: application/pdf; name="doc.pdf"
Content-Disposition: attachment; filename="doc.pdf"
Content-Transfer-Encoding: base64

`) + b64Lines(pdf, 76, "\r\n", false) + crlf(`
--outer
Content-Type: image/png
Content-Disposition: inline; filename="icon.png"
Content-Transfer-Encoding: base64

`) + b64Lines(icon, 76, "\r\n", false) + crlf(`
--outer--
`))
	p, err := Parse(raw, Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := p.Doc
	if !d.HasAttachments || d.TextBody != "See attached." {
		t.Errorf("doc %+v", d)
	}
	if len(p.Attachments) != 1 || p.Attachments[0].Hash != sha(pdf) || p.Attachments[0].Filename != "doc.pdf" || p.Attachments[0].ContentType != "application/pdf" || p.Attachments[0].PartPath != "2" {
		t.Fatalf("attachments %+v", p.Attachments)
	}
	if !bytes.Equal(p.Attachments[0].Data, pdf) {
		t.Error("pdf data")
	}
	c := d.Root.Children
	if c[1].Attachment != sha(pdf) || c[1].Size != int64(len(pdf)) || c[1].Disposition != "attachment" {
		t.Errorf("pdf part %+v", c[1])
	}
	if c[2].Attachment != "" || c[2].Size != 100 || c[2].Filename != "icon.png" {
		t.Errorf("icon part %+v", c[2])
	}
	refs, _ := SkeletonRefs(p.Skeleton)
	if len(refs) != 1 || refs[0] != sha(pdf) {
		t.Errorf("refs %v", refs)
	}
	if !p.Externalized {
		t.Error("not externalized")
	}
	roundTrip(t, raw, p)
	// The skeleton must be much smaller than the raw message.
	if len(p.Skeleton) > len(raw)/2 {
		t.Errorf("skeleton too large: %d vs %d", len(p.Skeleton), len(raw))
	}
}

func TestBase64Variants(t *testing.T) {
	data := bigData(3, 7000)
	cases := []struct {
		name         string
		eol          string
		lineLen      int
		trailing     bool
		externalized bool
	}{
		{"crlf76", "\r\n", 76, false, true},
		{"lf76", "\n", 76, false, true},
		{"lf64", "\n", 64, false, true},
		{"crlf64trailing", "\r\n", 64, true, true},
		{"single line", "", 1 << 30, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := b64Lines(data, c.lineLen, c.eol, c.trailing)
			raw := []byte("Content-Type: multipart/mixed; boundary=x" + c.eol + c.eol +
				"--x" + c.eol + "Content-Type: text/plain" + c.eol + c.eol + "hi" + c.eol +
				"--x" + c.eol + "Content-Type: application/octet-stream" + c.eol + "Content-Transfer-Encoding: base64" + c.eol + c.eol +
				body + c.eol + "--x--" + c.eol)
			if c.eol == "" {
				raw = []byte("Content-Type: multipart/mixed; boundary=x\n\n--x\nContent-Type: text/plain\n\nhi\n--x\nContent-Type: application/octet-stream\nContent-Transfer-Encoding: base64\n\n" + body + "\n--x--\n")
			}
			p, err := Parse(raw, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if p.Externalized != c.externalized {
				t.Errorf("externalized=%v want %v", p.Externalized, c.externalized)
			}
			if c.externalized && (len(p.Attachments) != 1 || p.Attachments[0].Hash != sha(data)) {
				t.Errorf("attachments %+v", len(p.Attachments))
			}
			roundTrip(t, raw, p)
		})
	}
}

func TestIrregularBase64FallsBack(t *testing.T) {
	data := bigData(4, 6000)
	enc := base64.StdEncoding.EncodeToString(data)
	// Irregular line lengths: 76, 50, 76, ...
	var b strings.Builder
	i := 0
	for n := 0; i < len(enc); n++ {
		l := 76
		if n%2 == 1 {
			l = 50
		}
		j := i + l
		if j > len(enc) {
			j = len(enc)
		}
		b.WriteString(enc[i:j])
		b.WriteString("\r\n")
		i = j
	}
	raw := []byte(crlf(`Content-Type: multipart/mixed; boundary=x

--x
Content-Type: text/plain

hi
--x
Content-Type: application/octet-stream
Content-Transfer-Encoding: base64

`) + b.String() + crlf(`--x--
`))
	p, err := Parse(raw, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Externalized || len(p.Attachments) != 0 || p.Doc.Root.Children[1].Attachment != "" {
		t.Errorf("irregular base64 should fall back: ext=%v atts=%d", p.Externalized, len(p.Attachments))
	}
	if p.Doc.Root.Children[1].Size != int64(len(data)) || !p.Doc.HasAttachments {
		t.Errorf("part %+v", p.Doc.Root.Children[1])
	}
	roundTrip(t, raw, p)
}

func TestRelatedInlineImage(t *testing.T) {
	img := bigData(5, 5000)
	raw := []byte(crlf(`Content-Type: multipart/related; boundary=r

--r
Content-Type: text/html; charset=utf-8

<html><body><img src="cid:img1@example.org"><p>Hi</p></body></html>
--r
Content-Type: image/png
Content-ID: <img1@example.org>
Content-Transfer-Encoding: base64

`) + b64Lines(img, 76, "\r\n", false) + crlf(`
--r--
`))
	p, _ := Parse(raw, Options{})
	if p.Doc.HasAttachments {
		t.Error("cid-referenced inline image should not count as attachment")
	}
	if len(p.Attachments) != 1 || p.Attachments[0].ContentID != "img1@example.org" || p.Doc.Root.Children[1].Attachment != sha(img) {
		t.Errorf("image not externalized: %+v", p.Attachments)
	}
	if p.Doc.TextBody != "Hi" {
		t.Errorf("text %q", p.Doc.TextBody)
	}
	roundTrip(t, raw, p)

	// Same message but the html does not reference the cid: counts as attachment.
	raw2 := bytes.Replace(raw, []byte(`src="cid:img1@example.org"`), []byte(`src="x.png"`), 1)
	p2, _ := Parse(raw2, Options{})
	if !p2.Doc.HasAttachments {
		t.Error("unreferenced inline image should count")
	}
	roundTrip(t, raw2, p2)
}

func TestRFC822Attachment(t *testing.T) {
	inner := crlf(`From: inner@example.org
Subject: inner
Content-Type: multipart/mixed; boundary=in

--in
Content-Type: text/plain

inner body
--in
Content-Type: application/octet-stream
Content-Transfer-Encoding: base64

` + b64Lines(bigData(6, 5000), 76, "\n", false) + `
--in--`)
	raw := []byte(crlf(`From: outer@example.org
Content-Type: multipart/mixed; boundary=out

--out
Content-Type: text/plain

fwd
--out
Content-Type: message/rfc822
Content-Disposition: attachment; filename="fwd.eml"

`) + inner + crlf(`
--out--
`))
	p, _ := Parse(raw, Options{})
	c := p.Doc.Root.Children
	if len(c) != 2 || c[1].Type != "message" || c[1].Subtype != "rfc822" || len(c[1].Children) != 0 {
		t.Fatalf("rfc822 part should be a leaf: %+v", c)
	}
	if len(p.Attachments) != 1 || p.Attachments[0].Hash != sha([]byte(inner)) || p.Attachments[0].Filename != "fwd.eml" {
		t.Errorf("attachment %+v", p.Attachments)
	}
	if !p.Doc.HasAttachments || p.Doc.TextBody != "fwd" {
		t.Errorf("doc %+v", p.Doc)
	}
	roundTrip(t, raw, p)
}

func TestCharsets(t *testing.T) {
	raw := []byte("Subject: =?ISO-8859-15?Q?Caf=E9_=A4?=\r\nContent-Type: text/plain; charset=iso-8859-1\r\n\r\nCaf\xe9\r\n")
	p, _ := Parse(raw, Options{})
	if p.Doc.Subject != "Café €" {
		t.Errorf("subject %q", p.Doc.Subject)
	}
	if p.Doc.TextBody != "Café\n" {
		t.Errorf("latin1 body %q", p.Doc.TextBody)
	}
	roundTrip(t, raw, p)

	raw = []byte("Subject: =?utf-8?B?w6nDqMOg?=\r\nContent-Type: text/plain; charset=windows-1252\r\n\r\n\x93quoted\x94 \x96 dash\r\n")
	p, _ = Parse(raw, Options{})
	if p.Doc.Subject != "éèà" {
		t.Errorf("utf8 subject %q", p.Doc.Subject)
	}
	if p.Doc.TextBody != "“quoted” – dash\n" {
		t.Errorf("cp1252 body %q", p.Doc.TextBody)
	}
	roundTrip(t, raw, p)

	// Unknown charset falls back to latin1; invalid utf-8 is sanitized.
	raw = []byte("Content-Type: text/plain; charset=x-bogus\r\n\r\n\xe9\r\n")
	p, _ = Parse(raw, Options{})
	if p.Doc.TextBody != "é\n" {
		t.Errorf("bogus charset %q", p.Doc.TextBody)
	}
	raw = []byte("Content-Type: text/plain; charset=utf-8\r\n\r\nok \xff\xfe bad\r\n")
	p, _ = Parse(raw, Options{})
	if !strings.Contains(p.Doc.TextBody, "ok �� bad") {
		t.Errorf("sanitize %q", p.Doc.TextBody)
	}

	// Quoted-printable body with charset.
	raw = []byte("Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nna=C3=AFve=\r\n done\r\n")
	p, _ = Parse(raw, Options{})
	if p.Doc.TextBody != "naïve done\n" {
		t.Errorf("qp %q", p.Doc.TextBody)
	}
	roundTrip(t, raw, p)
}

func TestDuplicateAttachments(t *testing.T) {
	data := bigData(7, 5000)
	part := crlf(`Content-Type: application/octet-stream
Content-Transfer-Encoding: base64

`) + b64Lines(data, 76, "\r\n", false)
	raw := []byte(crlf(`Content-Type: multipart/mixed; boundary=d

--d
`) + part + crlf(`
--d
`) + part + crlf(`
--d--
`))
	p, _ := Parse(raw, Options{})
	if len(p.Attachments) != 1 {
		t.Fatalf("expected 1 deduplicated attachment, got %d", len(p.Attachments))
	}
	c := p.Doc.Root.Children
	if c[0].Attachment != sha(data) || c[1].Attachment != sha(data) {
		t.Errorf("parts %+v %+v", c[0], c[1])
	}
	refs, _ := SkeletonRefs(p.Skeleton)
	if len(refs) != 1 {
		t.Errorf("refs %v", refs)
	}
	roundTrip(t, raw, p)
}

func TestForceFullRaw(t *testing.T) {
	raw := []byte(crlf(`Content-Type: multipart/mixed; boundary=f

--f
Content-Type: application/octet-stream
Content-Transfer-Encoding: base64

`) + b64Lines(bigData(8, 9000), 76, "\r\n", false) + crlf(`
--f--
`))
	p, _ := Parse(raw, Options{ForceFullRaw: true})
	if p.Externalized || len(p.Attachments) != 0 || p.Doc.Root.Children[0].Attachment != "" {
		t.Error("ForceFullRaw externalized something")
	}
	if !p.Doc.HasAttachments || p.Doc.Root.Children[0].Size != 9000 {
		t.Errorf("doc still parsed: %+v", p.Doc.Root.Children[0])
	}
	roundTrip(t, raw, p)
}

func TestInlineMax(t *testing.T) {
	data := bigData(9, 3000)
	raw := []byte("Content-Type: multipart/mixed; boundary=f\r\n\r\n--f\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\n" +
		b64Lines(data, 76, "\r\n", false) + "\r\n--f--\r\n")
	p, _ := Parse(raw, Options{InlineMax: 100})
	if !p.Externalized {
		t.Error("should externalize above InlineMax 100")
	}
	p, _ = Parse(raw, Options{InlineMax: 1 << 20})
	if p.Externalized {
		t.Error("should stay inline below InlineMax")
	}
	roundTrip(t, raw, p)
}

func TestSevenBitAttachment(t *testing.T) {
	text := strings.Repeat("line of text\r\n", 600)
	raw := []byte("Content-Type: multipart/mixed; boundary=f\r\n\r\n--f\r\nContent-Type: text/plain\r\n\r\nbody\r\n--f\r\nContent-Type: text/csv\r\nContent-Disposition: attachment; filename=\"data.csv\"\r\n\r\n" + text + "\r\n--f--\r\n")
	p, _ := Parse(raw, Options{})
	if len(p.Attachments) != 1 || !bytes.Equal(p.Attachments[0].Data, []byte(text)) {
		t.Fatalf("7bit attachment not externalized as-is: %d", len(p.Attachments))
	}
	if p.Doc.TextBody != "body" || p.Doc.Root.Children[1].IsBody {
		t.Error("attachment text/plain must not be the body")
	}
	roundTrip(t, raw, p)
}

func TestHostileInputs(t *testing.T) {
	deep := "Content-Type: multipart/mixed; boundary=b0\r\n\r\n"
	for i := 0; i < 60; i++ {
		deep += fmt.Sprintf("--b%d\r\nContent-Type: multipart/mixed; boundary=b%d\r\n\r\n", i, i+1)
	}
	deep += "--b60\r\nContent-Type: text/plain\r\n\r\ndeep\r\n--b60--\r\n"
	r := rand.New(rand.NewSource(42))
	random := make([]byte, 5000)
	r.Read(random)
	inputs := [][]byte{
		random,
		[]byte("no headers at all, just text"),
		[]byte("\r\n\r\nbody only"),
		[]byte("Content-Type: multipart/mixed; boundary=\r\n\r\n--\r\nx\r\n----\r\n"),
		[]byte("Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nno closing boundary"),
		[]byte("Content-Type: multipart/mixed; boundary=b\r\n\r\n--b--\r\n"),
		[]byte("Content-Type: multipart/mixed; boundary=b\r\n\r\nno delimiters here\r\n"),
		[]byte("Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\n--b\r\n--b--"),
		[]byte("Subject: x\rFrom: y\rContent-Type: text/plain\r\rCR only"),
		[]byte("Subject: x\r\nFrom: y\r\nno blank line then body"),
		[]byte("Content-Type: text/plain\r\nContent-Transfer-Encoding: base64\r\n\r\n!!!not base64!!!\r\n"),
		[]byte("Content-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\n" + strings.Repeat("QUJD", 2000) + "Q\r\n"),
		[]byte("Content-Type: text/plain; charset=\"unterminated\r\n\r\nx"),
		[]byte("From: <<<>>>, \"unterminated\r\nTo: ,,,\r\nDate: not a date\r\nContent-Type: ;;;\r\n\r\nx"),
		[]byte(deep),
		[]byte(":\r\n:::\r\n \r\n\t\r\n\r\n"),
		[]byte("Content-Type: multipart/digest; boundary=d\r\n\r\n--d\r\n\r\nFrom: inner\r\n\r\nx\r\n--d--\r\n"),
		[]byte("Content-Type: text/html\r\n\r\n<html><body><p>unclosed <b>tag <!-- comment"),
		[]byte("Content-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n=ZZ =\r\n=A"),
	}
	for i, in := range inputs {
		p, err := Parse(in, Options{InlineMax: 1})
		if err != nil {
			t.Fatalf("input %d: %v", i, err)
		}
		roundTrip(t, in, p)
		if p.Doc.Root == nil {
			t.Fatalf("input %d: nil root", i)
		}
	}
	if _, err := Parse(nil, Options{}); err == nil {
		t.Error("empty input should error")
	}
}

func TestSkeletonErrors(t *testing.T) {
	bad := [][]byte{nil, []byte("nope"), []byte("DVSK1\x00"), []byte("DVSK1\x00\x00\x00\x00\x09ab"), []byte("DVSK1\x07\x00\x00\x00\x00"), []byte("DVSK1\x01\x00\x00\x00\x03{{{")}
	for _, b := range bad {
		if _, err := Reconstruct(b, func(string) ([]byte, error) { return nil, nil }); err == nil {
			t.Errorf("expected error for %q", b)
		}
		if _, err := SkeletonRefs(b); err == nil && len(b) > 5 && b[5] == 1 {
			t.Errorf("expected refs error for %q", b)
		}
	}
	// Missing attachment surfaces fetch error.
	raw := []byte("Content-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\n" + b64Lines(bigData(10, 5000), 76, "\r\n", false) + "\r\n")
	p, _ := Parse(raw, Options{})
	if !p.Externalized {
		t.Fatal("expected externalized root attachment")
	}
	if _, err := Reconstruct(p.Skeleton, func(string) ([]byte, error) { return nil, fmt.Errorf("gone") }); err == nil {
		t.Error("expected fetch error")
	}
}

// ---- generated corpus round-trip ----

type gen struct{ r *rand.Rand }

func (g *gen) eol() string {
	if g.r.Intn(4) == 0 {
		return "\n"
	}
	return "\r\n"
}

func (g *gen) leaf(eol string, depth int) string {
	var b strings.Builder
	switch g.r.Intn(5) {
	case 0:
		b.WriteString("Content-Type: text/plain; charset=utf-8" + eol + eol)
		b.WriteString(strings.Repeat("some text ", g.r.Intn(20)))
	case 1:
		b.WriteString("Content-Type: text/html" + eol + "Content-Transfer-Encoding: quoted-printable" + eol + eol)
		b.WriteString("<p>caf=C3=A9</p>")
	case 2:
		n := g.r.Intn(20000)
		data := bigData(g.r.Int63(), n)
		ll := []int{76, 64, 72, 60}[g.r.Intn(4)]
		b.WriteString("Content-Type: application/octet-stream" + eol + "Content-Disposition: attachment; filename=\"f.bin\"" + eol + "Content-Transfer-Encoding: base64" + eol + eol)
		b.WriteString(b64Lines(data, ll, eol, g.r.Intn(2) == 0))
	case 3:
		b.WriteString("Content-Type: text/csv" + eol + "Content-Disposition: attachment" + eol + eol)
		b.WriteString(strings.Repeat("a,b,c"+eol, g.r.Intn(2000)))
	case 4:
		if depth < 4 {
			return g.multipart(eol, depth+1)
		}
		b.WriteString("Content-Type: text/plain" + eol + eol + "x")
	}
	return b.String()
}

func (g *gen) multipart(eol string, depth int) string {
	bnd := fmt.Sprintf("=_b%d_%d", depth, g.r.Intn(1000))
	var b strings.Builder
	sub := []string{"mixed", "alternative", "related"}[g.r.Intn(3)]
	b.WriteString("Content-Type: multipart/" + sub + "; boundary=\"" + bnd + "\"" + eol + eol)
	if g.r.Intn(3) == 0 {
		b.WriteString("This is a multi-part message in MIME format." + eol)
	}
	n := 1 + g.r.Intn(4)
	for i := 0; i < n; i++ {
		b.WriteString("--" + bnd + eol)
		b.WriteString(g.leaf(eol, depth))
		b.WriteString(eol)
	}
	b.WriteString("--" + bnd + "--" + eol)
	if g.r.Intn(3) == 0 {
		b.WriteString("epilogue" + eol)
	}
	return b.String()
}

func (g *gen) message() []byte {
	eol := g.eol()
	hdr := "From: a@example.org" + eol + "To: b@example.org" + eol + "Subject: gen " + fmt.Sprint(g.r.Int()) + eol + "Date: Tue, 21 Oct 2026 12:00:00 +0000" + eol
	var body string
	if g.r.Intn(3) == 0 {
		body = g.leaf(eol, 0)
	} else {
		body = g.multipart(eol, 0)
	}
	return []byte(hdr + body)
}

func TestGeneratedRoundTrip(t *testing.T) {
	g := &gen{r: rand.New(rand.NewSource(2026))}
	ext := 0
	for i := 0; i < 300; i++ {
		raw := g.message()
		for _, inline := range []int64{0, 1, 1 << 30} {
			p, err := Parse(raw, Options{InlineMax: inline})
			if err != nil {
				t.Fatalf("msg %d: %v", i, err)
			}
			if p.Externalized {
				ext++
			}
			out, err := Reconstruct(p.Skeleton, fetcher(p.Attachments))
			if err != nil {
				t.Fatalf("msg %d: reconstruct: %v", i, err)
			}
			if !bytes.Equal(out, raw) {
				t.Fatalf("msg %d (inline=%d): round trip mismatch", i, inline)
			}
			// Every externalized part's hash must be fetchable and listed.
			refs, _ := SkeletonRefs(p.Skeleton)
			if len(refs) != len(p.Attachments) {
				t.Fatalf("msg %d: refs %d vs attachments %d", i, len(refs), len(p.Attachments))
			}
		}
	}
	if ext == 0 {
		t.Error("corpus never externalized anything")
	}
}

func TestDateFormats(t *testing.T) {
	for _, s := range []string{
		"Tue, 21 Oct 2026 12:00:00 +0200",
		"21 Oct 2026 12:00:00 +0200",
		"Tue, 21 Oct 2026 12:00:00 +0200 (CEST)",
		"Tue Oct 21 12:00:00 2026",
		"2026-10-21 12:00:00 +0200",
	} {
		if parseDate(s).IsZero() {
			t.Errorf("failed to parse %q", s)
		}
	}
	if !parseDate("garbage").IsZero() {
		t.Error("garbage parsed")
	}
}

func TestAddressFallback(t *testing.T) {
	a := parseAddresses("Broken Name <not an address, really>")
	if len(a) == 0 || a[0].Name == "" {
		t.Errorf("fallback %+v", a)
	}
	a = parseAddresses("=?utf-8?q?J=C3=BCrgen?= <j@example.org>, plain@example.org")
	if len(a) != 2 || a[0].Name != "Jürgen" || a[1].Email != "plain@example.org" {
		t.Errorf("list %+v", a)
	}
}
