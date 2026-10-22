package jmap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	dmail "kittiwake/mail"
	"kittiwake/service/actor"
	"kittiwake/service/mailbox"
	"kittiwake/store/message"
)

// Uploads is where uploaded blobs live between the upload and the
// Email/import or Email/set that uses them. The message store keeps them
// in RADOS so every JMAP node sees them; a blob id is "u-<uuid>".
type Uploads interface {
	PutUpload(ctx context.Context, mb uuid.UUID, id string, data []byte, typ string) error
	GetUpload(ctx context.Context, mb uuid.UUID, id string) ([]byte, string, error)
}

// accountFromPath validates an accountId path element against the actor.
func accountFromPath(ctx context.Context, s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, false
	}
	act := actor.From(ctx)
	if id != act.MailboxID && !act.IsAdmin() {
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) serveUpload(w http.ResponseWriter, r *http.Request, rest string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	acct, ok := accountFromPath(r.Context(), strings.Trim(rest, "/"))
	if !ok {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadSize))
	if err != nil {
		http.Error(w, "upload too large", http.StatusRequestEntityTooLarge)
		return
	}
	typ := r.Header.Get("Content-Type")
	if typ == "" {
		typ = "application/octet-stream"
	}
	id := "u-" + uuid.NewString()
	if err := s.cfg.Uploads.PutUpload(r.Context(), acct, id, data, typ); err != nil {
		s.log.Error("jmap: upload store failed", "error", err.Error())
		http.Error(w, "upload failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"accountId": acct.String(), "blobId": id, "type": typ, "size": len(data),
	})
}

func (s *Server) serveDownload(w http.ResponseWriter, r *http.Request, rest string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	acct, ok := accountFromPath(r.Context(), parts[0])
	if !ok {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	blobID := parts[1]
	name := ""
	if len(parts) == 3 {
		name = parts[2]
	}
	data, typ, err := s.blob(r.Context(), acct, blobID)
	if err != nil {
		switch {
		case errors.Is(err, mailbox.ErrNotFound), errors.Is(err, errBlobNotFound):
			http.Error(w, "blob not found", http.StatusNotFound)
		case errors.Is(err, mailbox.ErrForbidden), errors.Is(err, mailbox.ErrSealed):
			http.Error(w, "forbidden", http.StatusForbidden)
		default:
			s.log.ErrorContext(r.Context(), "blob download failed", "error", err.Error())
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	if accept := r.URL.Query().Get("accept"); accept != "" {
		typ = accept
	} else if t := r.URL.Query().Get("type"); t != "" {
		typ = t
	}
	if typ == "" {
		typ = "application/octet-stream"
	}
	w.Header().Set("Content-Type", typ)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Cache-Control", "private, immutable, max-age=31536000")
	if name != "" {
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	} else {
		w.Header().Set("Content-Disposition", "attachment")
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(data)
}

var errBlobNotFound = errors.New("jmap: blob not found")

// blob resolves a blob id to content and type.
func (s *Server) blob(ctx context.Context, acct uuid.UUID, blobID string) ([]byte, string, error) {
	switch {
	case strings.HasPrefix(blobID, "u-"):
		data, typ, err := s.cfg.Uploads.GetUpload(ctx, acct, blobID)
		if err != nil {
			if errors.Is(err, message.ErrNotFound) {
				return nil, "", errBlobNotFound
			}
			return nil, "", err
		}
		return data, typ, nil
	case strings.HasPrefix(blobID, "m-"):
		id, err := uuid.Parse(blobID[2:])
		if err != nil {
			return nil, "", errBlobNotFound
		}
		raw, err := s.mb.Raw(ctx, acct, id)
		if err != nil {
			return nil, "", err
		}
		return raw, "message/rfc822", nil
	case strings.HasPrefix(blobID, "a-"):
		data, typ, err := s.mb.Attachment(ctx, acct, blobID[2:])
		return data, typ, err
	case strings.HasPrefix(blobID, "p-"):
		rest := blobID[2:]
		if len(rest) < 38 || rest[36] != '-' {
			return nil, "", errBlobNotFound
		}
		id, err := uuid.Parse(rest[:36])
		if err != nil {
			return nil, "", errBlobNotFound
		}
		path := rest[37:]
		raw, err := s.mb.Raw(ctx, acct, id)
		if err != nil {
			return nil, "", err
		}
		return extractPart(raw, path)
	}
	return nil, "", errBlobNotFound
}

// extractPart re-parses the raw message and returns the decoded content of
// the part with the given JMAP partId.
func extractPart(raw []byte, path string) ([]byte, string, error) {
	// InlineMax 1 externalizes every non-body leaf so decoded data is available.
	parsed, err := dmail.Parse(raw, dmail.Options{InlineMax: 1})
	if err != nil {
		return nil, "", err
	}
	var target *dmail.Part
	var walk func(p *dmail.Part)
	walk = func(p *dmail.Part) {
		if p == nil || target != nil {
			return
		}
		if len(p.Children) == 0 && partID(p) == path {
			target = p
			return
		}
		for _, c := range p.Children {
			walk(c)
		}
	}
	walk(parsed.Doc.Root)
	if target == nil {
		return nil, "", errBlobNotFound
	}
	typ := strings.ToLower(target.Type + "/" + target.Subtype)
	if cs, ok := target.Params["charset"]; ok && cs != "" && target.Type == "text" {
		typ += "; charset=" + cs
	}
	if target.IsBody {
		if strings.EqualFold(target.Subtype, "html") {
			return []byte(parsed.Doc.HTMLBody), "text/html; charset=utf-8", nil
		}
		return []byte(parsed.Doc.TextBody), "text/plain; charset=utf-8", nil
	}
	for _, a := range parsed.Attachments {
		if a.PartPath == target.Path {
			return a.Data, typ, nil
		}
	}
	// Tiny part (<= 1 byte) or non-externalizable: nothing decoded available.
	return nil, "", fmt.Errorf("%w: part %s has no decodable content", errBlobNotFound, path)
}
