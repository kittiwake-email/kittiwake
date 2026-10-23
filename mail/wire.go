package mail

import (
	"bufio"
	"bytes"
	gomessage "github.com/emersion/go-message"
	"github.com/emersion/go-message/textproto"
	"io"
	"strings"
)

// WirePart describes the exact served MIME bytes without keeping payloads.
// Sizes and line counts refer to transfer-encoded bodies, including embedded
// messages. This allows metadata FETCH without reading external attachments.
type WirePart struct {
	Header   string      `json:"header"`
	Size     uint32      `json:"size"`
	Lines    int64       `json:"lines"`
	Children []*WirePart `json:"children,omitempty"`
	Message  *WirePart   `json:"message,omitempty"`
}

func WireMetadata(raw []byte) *WirePart { return wireMetadata(bytes.NewReader(raw), 0) }
func wireMetadata(r io.Reader, depth int) *WirePart {
	br := bufio.NewReader(r)
	header, _ := textproto.ReadHeader(br)
	return wirePart(header, br, depth)
}
func wirePart(header textproto.Header, r io.Reader, depth int) *WirePart {
	var h bytes.Buffer
	_ = textproto.WriteHeader(&h, header)
	p := &WirePart{Header: h.String()}
	mh := gomessage.Header{Header: header}
	typ, params, _ := mh.ContentType()
	if strings.HasPrefix(typ, "multipart/") && depth < maxDepth {
		mr := textproto.NewMultipartReader(r, params["boundary"])
		for {
			part, _ := mr.NextPart()
			if part == nil {
				break
			}
			p.Children = append(p.Children, wirePart(part.Header, part, depth+1))
		}
	} else {
		body, _ := io.ReadAll(r)
		p.Size = uint32(len(body))
		p.Lines = int64(bytes.Count(body, []byte("\n")))
		if (typ == "message/rfc822" || typ == "message/global") && depth < maxDepth {
			p.Message = wireMetadata(bytes.NewReader(body), depth+1)
		}
	}
	return p
}

// RenderHeaders returns the exact top-level headers of the rendered form.
func RenderHeaders(doc *Document) []byte {
	var out bytes.Buffer
	writeTopHeaders(&out, doc)
	return out.Bytes()
}
