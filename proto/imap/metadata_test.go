package imap

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/emersion/go-imap/v2/imapserver"
	"kittiwake/mail"
)

func TestStoredWireStructureMatchesServedBytes(t *testing.T) {
	fixtures := []string{
		"From: a@example.org\r\nSubject: hello\r\n\r\nline one\r\nline two\r\n",
		"Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nhi\r\n--b\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=x\r\nContent-Transfer-Encoding: base64\r\n\r\n" + strings.Repeat("YWJj", 4096) + "\r\n--b--\r\n",
		"Content-Type: message/rfc822\r\n\r\nFrom: inner@example.org\r\nSubject: nested\r\nContent-Type: text/plain\r\n\r\nbody\r\n",
	}
	for _, raw := range fixtures {
		parsed, err := mail.Parse([]byte(raw), mail.Options{InlineMax: 8})
		if err != nil {
			t.Fatal(err)
		}
		rendered, err := mail.Render(parsed.Doc, func(hash string) ([]byte, error) {
			for _, a := range parsed.Attachments {
				if a.Hash == hash {
					return a.Data, nil
				}
			}
			t.Fatal("missing attachment")
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, served := range [][]byte{rendered, []byte(raw)} {
			got := wireStructure(mail.WireMetadata(served))
			want := imapserver.ExtractBodyStructure(bytes.NewReader(served))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("metadata structure mismatch\ngot: %#v\nwant: %#v", got, want)
			}
		}
	}
}
