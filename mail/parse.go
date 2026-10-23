package mail

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

const maxDepth = 50

// node is the internal MIME tree with byte offsets into the raw message.
type node struct {
	part      *Part
	headers   []rawHeader
	bodyStart int // encoded body range in raw
	bodyEnd   int
	children  []*node
	charset   string
	leaf      bool
	// decoded content for leaves (best effort)
	decoded  []byte
	decodeOK bool
}

// Parse parses a raw message. It never fails on malformed MIME; it degrades
// gracefully. err is non-nil only for empty input.
func Parse(raw []byte, opts Options) (*Parsed, error) {
	if len(raw) == 0 {
		return nil, errors.New("mail: empty message")
	}
	if opts.InlineMax <= 0 {
		opts.InlineMax = DefaultInlineMax
	}
	root := parseNode(raw, 0, len(raw), "", 0, "text/plain")
	hdrEnd := headerBlock(raw)

	doc := &Document{
		Version:    1,
		RawHeaders: string(raw[:hdrEnd]),
		Root:       root.part,
	}
	for _, h := range root.headers {
		doc.Headers = append(doc.Headers, Header{Name: h.name, Value: decodeWords(h.value)})
	}
	if v, ok := firstHeader(root.headers, "Subject"); ok {
		doc.Subject = decodeWords(v)
	}
	doc.From = parseAddresses(strings.Join(allHeaders(root.headers, "From"), ", "))
	if v, ok := firstHeader(root.headers, "Sender"); ok {
		if a := parseAddresses(v); len(a) > 0 {
			doc.Sender = &a[0]
		}
	}
	doc.ReplyTo = parseAddresses(strings.Join(allHeaders(root.headers, "Reply-To"), ", "))
	doc.To = parseAddresses(strings.Join(allHeaders(root.headers, "To"), ", "))
	doc.Cc = parseAddresses(strings.Join(allHeaders(root.headers, "Cc"), ", "))
	doc.Bcc = parseAddresses(strings.Join(allHeaders(root.headers, "Bcc"), ", "))
	if v, ok := firstHeader(root.headers, "Message-ID"); ok {
		if ids := parseMsgIDs(v); len(ids) > 0 {
			doc.MessageID = ids[0]
		}
	}
	if v, ok := firstHeader(root.headers, "In-Reply-To"); ok {
		doc.InReplyTo = parseMsgIDs(v)
	}
	if v, ok := firstHeader(root.headers, "References"); ok {
		doc.References = parseMsgIDs(v)
	}
	if v, ok := firstHeader(root.headers, "Date"); ok {
		doc.Date = parseDate(v)
	}

	// Decode leaves and pick bodies.
	var leaves []*node
	collectLeaves(root, &leaves)
	for _, n := range leaves {
		n.decoded, n.decodeOK = decodeTransfer(raw[n.bodyStart:n.bodyEnd], n.part.Encoding)
		n.part.Size = int64(len(n.decoded))
	}
	textDone, htmlDone := false, false
	for _, n := range leaves {
		p := n.part
		if p.Type != "text" || p.Disposition == "attachment" {
			continue
		}
		switch {
		case p.Subtype == "plain" && !textDone:
			textDone = true
			p.IsBody = true
			doc.TextBody = normalizeNewlines(toUTF8(n.decoded, n.charset))
		case p.Subtype == "html" && !htmlDone:
			htmlDone = true
			p.IsBody = true
			doc.HTMLBody = normalizeNewlines(toUTF8(n.decoded, n.charset))
		}
	}
	if doc.TextBody == "" && doc.HTMLBody != "" {
		doc.TextBody = htmlToText(doc.HTMLBody)
	}
	doc.Preview = preview(doc.TextBody, 200)

	// Attachments: every non-body leaf is externalized when larger than
	// InlineMax (decoded size), else its decoded content stays in the part.
	res := &Parsed{Doc: doc, Signed: root.part.Type == "multipart" && root.part.Subtype == "signed"}
	seen := map[string]bool{}
	for _, n := range leaves { // leaves are in document order
		p := n.part
		if p.IsBody {
			continue
		}
		if !(p.ContentID != "" && strings.Contains(doc.HTMLBody, "cid:"+p.ContentID)) {
			doc.HasAttachments = true
		}
		data := n.decoded
		if !n.decodeOK {
			data = raw[n.bodyStart:n.bodyEnd]
			p.Encoding = "binary"
		}
		if int64(len(data)) <= opts.InlineMax {
			p.Data = data
			continue
		}
		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])
		p.Attachment = hash
		if !seen[hash] {
			seen[hash] = true
			res.Attachments = append(res.Attachments, Attachment{
				Hash:        hash,
				Data:        data,
				ContentType: p.Type + "/" + p.Subtype,
				Filename:    p.Filename,
				ContentID:   p.ContentID,
				Disposition: p.Disposition,
				PartPath:    p.Path,
			})
		}
	}
	// Size is what we will serve: the rendered form.
	doc.OriginalSize = int64(len(raw))
	rendered, err := Render(doc, func(h string) ([]byte, error) {
		for _, a := range res.Attachments {
			if a.Hash == h {
				return a.Data, nil
			}
		}
		return nil, errors.New("mail: attachment missing during parse")
	})
	if err != nil {
		return nil, err
	}
	doc.Size = int64(len(rendered))
	doc.Wire = WireMetadata(rendered)
	return res, nil
}

func collectLeaves(n *node, out *[]*node) {
	if n.leaf {
		*out = append(*out, n)
		return
	}
	for _, c := range n.children {
		collectLeaves(c, out)
	}
}

func normalizeNewlines(s string) string {
	if !strings.Contains(s, "\r") {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "\n")
}

// parseNode parses raw[start:end] as a MIME entity.
func parseNode(raw []byte, start, end int, path string, depth int, defaultType string) *node {
	hdrEnd := start + headerBlock(raw[start:end])
	n := &node{
		headers:   parseHeaders(raw[start:hdrEnd]),
		bodyStart: hdrEnd,
		bodyEnd:   end,
		part:      &Part{Path: path},
	}
	p := n.part

	ctv, hasCT := firstHeader(n.headers, "Content-Type")
	mediatype, params := defaultType, map[string]string{}
	if hasCT {
		mediatype, params = parseMediaType(ctv)
	}
	if i := strings.IndexByte(mediatype, '/'); i > 0 {
		p.Type, p.Subtype = mediatype[:i], mediatype[i+1:]
	} else {
		p.Type, p.Subtype = mediatype, ""
		if mediatype == "text" {
			p.Subtype = "plain"
		}
	}
	if p.Type == "" {
		p.Type, p.Subtype = "application", "octet-stream"
	}
	if len(params) > 0 {
		p.Params = params
	}
	n.charset = params["charset"]

	if v, ok := firstHeader(n.headers, "Content-Transfer-Encoding"); ok {
		p.Encoding = strings.ToLower(strings.TrimSpace(v))
	}
	if v, ok := firstHeader(n.headers, "Content-Disposition"); ok {
		d, dparams := parseMediaType(v)
		p.Disposition = d
		if fn, ok := dparams["filename"]; ok {
			p.Filename = decodeWords(fn)
		}
	}
	if p.Filename == "" {
		if name, ok := params["name"]; ok {
			p.Filename = decodeWords(name)
		}
	}
	if v, ok := firstHeader(n.headers, "Content-ID"); ok {
		v = strings.TrimSpace(v)
		v = strings.TrimPrefix(v, "<")
		v = strings.TrimSuffix(v, ">")
		p.ContentID = v
	}
	p.EncodedSize = int64(end - hdrEnd)

	if p.Type == "multipart" && depth < maxDepth {
		boundary := params["boundary"]
		if boundary != "" {
			ranges, found := splitMultipart(raw[hdrEnd:end], boundary)
			if found {
				childDefault := "text/plain"
				if p.Subtype == "digest" {
					childDefault = "message/rfc822"
				}
				for i, r := range ranges {
					cpath := strconv.Itoa(i + 1)
					if path != "" {
						cpath = path + "." + cpath
					}
					c := parseNode(raw, hdrEnd+r[0], hdrEnd+r[1], cpath, depth+1, childDefault)
					n.children = append(n.children, c)
					p.Children = append(p.Children, c.part)
				}
				p.EncodedSize = 0
				return n
			}
		}
	}
	n.leaf = true
	return n
}

// splitMultipart finds the body parts delimited by boundary. Offsets are
// relative to body. found is false when no delimiter line exists.
func splitMultipart(body []byte, boundary string) (parts [][2]int, found bool) {
	delim := []byte("--" + boundary)
	type d struct {
		ls, next int
		closing  bool
	}
	var delims []d
	ls := 0
	for ls <= len(body) {
		le := bytes.IndexByte(body[ls:], '\n')
		var next int
		if le < 0 {
			le = len(body)
			next = len(body) + 1
		} else {
			le += ls
			next = le + 1
		}
		line := body[ls:le]
		if bytes.HasPrefix(line, delim) {
			rest := line[len(delim):]
			closing := bytes.HasPrefix(rest, []byte("--"))
			if closing {
				rest = rest[2:]
			}
			if len(bytes.TrimSpace(rest)) == 0 {
				delims = append(delims, d{ls: ls, next: next, closing: closing})
				if closing {
					break
				}
			}
		}
		ls = next
	}
	if len(delims) == 0 || delims[0].closing {
		return nil, false
	}
	for i := 0; i < len(delims); i++ {
		if delims[i].closing {
			break
		}
		start := delims[i].next
		if start > len(body) {
			start = len(body)
		}
		var end int
		if i+1 < len(delims) {
			end = delims[i+1].ls
			if end > start && body[end-1] == '\n' {
				end--
				if end > start && body[end-1] == '\r' {
					end--
				}
			}
		} else {
			end = len(body)
		}
		if end < start {
			end = start
		}
		parts = append(parts, [2]int{start, end})
	}
	return parts, true
}
