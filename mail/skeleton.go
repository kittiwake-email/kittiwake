package mail

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"mime/quotedprintable"
)

// Skeleton framing: magic "DVSK1" then frames of
// [kind:1][len:4 big-endian][payload]. kind 0 = literal, kind 1 = reference.
const skeletonMagic = "DVSK1"

const (
	frameLiteral byte = 0
	frameRef     byte = 1
)

// refParams are the re-encoding parameters stored in a reference frame.
type refParams struct {
	Hash            string `json:"hash"`
	Encoding        string `json:"encoding"`
	LineLength      int    `json:"line_length,omitempty"`
	CRLF            bool   `json:"crlf,omitempty"`
	TrailingNewline bool   `json:"trailing_newline,omitempty"`
}

// ErrSkeleton is returned for malformed skeletons.
var ErrSkeleton = errors.New("mail: malformed skeleton")

type skeletonBuilder struct {
	buf bytes.Buffer
}

func newSkeletonBuilder() *skeletonBuilder {
	b := &skeletonBuilder{}
	b.buf.WriteString(skeletonMagic)
	return b
}

func (b *skeletonBuilder) frame(kind byte, payload []byte) {
	var hdr [5]byte
	hdr[0] = kind
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	b.buf.Write(hdr[:])
	b.buf.Write(payload)
}

func (b *skeletonBuilder) literal(p []byte) {
	// Split very large literals so a frame length fits in uint32.
	for len(p) > 0 {
		n := len(p)
		if n > 1<<30 {
			n = 1 << 30
		}
		b.frame(frameLiteral, p[:n])
		p = p[n:]
	}
}

func (b *skeletonBuilder) ref(p refParams) {
	j, _ := json.Marshal(p)
	b.frame(frameRef, j)
}

func (b *skeletonBuilder) bytes() []byte { return b.buf.Bytes() }

// walkSkeleton calls fn for each frame.
func walkSkeleton(skel []byte, fn func(kind byte, payload []byte) error) error {
	if len(skel) < len(skeletonMagic) || string(skel[:len(skeletonMagic)]) != skeletonMagic {
		return ErrSkeleton
	}
	i := len(skeletonMagic)
	for i < len(skel) {
		if len(skel)-i < 5 {
			return ErrSkeleton
		}
		kind := skel[i]
		n := int(binary.BigEndian.Uint32(skel[i+1 : i+5]))
		i += 5
		if n > len(skel)-i {
			return ErrSkeleton
		}
		if err := fn(kind, skel[i:i+n]); err != nil {
			return err
		}
		i += n
	}
	return nil
}

// Reconstruct rebuilds the exact original bytes from a skeleton. fetch
// returns the decoded attachment content for a hash.
func Reconstruct(skeleton []byte, fetch func(hash string) ([]byte, error)) ([]byte, error) {
	var out bytes.Buffer
	err := walkSkeleton(skeleton, func(kind byte, payload []byte) error {
		switch kind {
		case frameLiteral:
			out.Write(payload)
		case frameRef:
			var p refParams
			if err := json.Unmarshal(payload, &p); err != nil {
				return fmt.Errorf("%w: %v", ErrSkeleton, err)
			}
			data, err := fetch(p.Hash)
			if err != nil {
				return fmt.Errorf("mail: fetch attachment %s: %w", p.Hash, err)
			}
			enc, err := encodeBody(data, p)
			if err != nil {
				return err
			}
			out.Write(enc)
		default:
			return ErrSkeleton
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// SkeletonRefs returns the distinct attachment hashes referenced by a
// skeleton, in order of first appearance.
func SkeletonRefs(skeleton []byte) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	err := walkSkeleton(skeleton, func(kind byte, payload []byte) error {
		if kind != frameRef {
			return nil
		}
		var p refParams
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("%w: %v", ErrSkeleton, err)
		}
		if !seen[p.Hash] {
			seen[p.Hash] = true
			out = append(out, p.Hash)
		}
		return nil
	})
	return out, err
}

// encodeBody re-encodes decoded data with the recorded parameters.
func encodeBody(data []byte, p refParams) ([]byte, error) {
	switch p.Encoding {
	case "base64":
		return encodeBase64(data, p), nil
	case "quoted-printable":
		var b bytes.Buffer
		w := quotedprintable.NewWriter(&b)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	case "7bit", "8bit", "binary", "":
		return data, nil
	}
	return nil, fmt.Errorf("%w: unknown encoding %q", ErrSkeleton, p.Encoding)
}

func encodeBase64(data []byte, p refParams) []byte {
	enc := base64.StdEncoding.EncodeToString(data)
	eol := "\n"
	if p.CRLF {
		eol = "\r\n"
	}
	var b bytes.Buffer
	b.Grow(len(enc) + len(enc)/64*2 + 2)
	if p.LineLength <= 0 {
		b.WriteString(enc)
	} else {
		for i := 0; i < len(enc); i += p.LineLength {
			j := i + p.LineLength
			if j > len(enc) {
				j = len(enc)
			}
			if i > 0 {
				b.WriteString(eol)
			}
			b.WriteString(enc[i:j])
		}
	}
	if p.TrailingNewline {
		b.WriteString(eol)
	}
	return b.Bytes()
}

// base64Params derives re-encoding parameters from an original encoded body.
func base64Params(encoded []byte) refParams {
	p := refParams{Encoding: "base64"}
	nl := bytes.IndexByte(encoded, '\n')
	if nl < 0 {
		return p
	}
	p.CRLF = nl > 0 && encoded[nl-1] == '\r'
	p.LineLength = nl
	if p.CRLF {
		p.LineLength = nl - 1
	}
	eol := "\n"
	if p.CRLF {
		eol = "\r\n"
	}
	p.TrailingNewline = bytes.HasSuffix(encoded, []byte(eol))
	return p
}
