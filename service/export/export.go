// Package export writes a complete mailbox as a Dovecot-compatible
// Maildir++ tar archive with a manifest and the audit log, and imports a
// Maildir (directory or archive) into a mailbox. See README.md "Mailbox
// export and import (Maildir)".
package export

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/mail"
	"kittiwake/service/actor"
	"kittiwake/service/auditor"
	"kittiwake/service/delivery"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
)

// Errors.
var (
	ErrForbidden = errors.New("export: admin role required")
	ErrNotFound  = errors.New("export: mailbox not found")
)

// Service exports and imports mailboxes.
type Service struct {
	ids      *identity.Store
	idx      *index.Store
	msgs     *message.Store
	atts     *attach.Store
	auditSt  *audit.Store
	del      *delivery.Service
	audit    *auditor.Auditor
	log      *slog.Logger
	Hostname string
	Now      func() time.Time
}

// New returns the service.
func New(ids *identity.Store, idx *index.Store, msgs *message.Store, atts *attach.Store, auditSt *audit.Store, del *delivery.Service, aud *auditor.Auditor, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	h, _ := os.Hostname()
	if h == "" {
		h = "kittiwake"
	}
	return &Service{ids: ids, idx: idx, msgs: msgs, atts: atts, auditSt: auditSt, del: del, audit: aud, log: log, Hostname: h, Now: time.Now}
}

func (s *Service) authorize(ctx context.Context) error {
	a := actor.From(ctx)
	if a.IsAdmin() || (a.Protocol == audit.ProtoCLI && a.IsSystem()) {
		return nil
	}
	return ErrForbidden
}

// ---- manifest ----

// ManifestFolder describes a folder in the export.
type ManifestFolder struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Maildir     string   `json:"maildir"` // "" for INBOX, ".Sent", ".Projects.2026"
	Role        string   `json:"role,omitempty"`
	UIDValidity uint32   `json:"uidvalidity"`
	UIDNext     uint32   `json:"uidnext"`
	ModSeq      uint64   `json:"modseq"`
	Subscribed  bool     `json:"subscribed"`
	Keywords    []string `json:"keywords,omitempty"`
}

// ManifestMessage describes one exported message file.
type ManifestMessage struct {
	MessageID    uuid.UUID `json:"message_id"`
	Folder       string    `json:"folder"` // folder id
	UID          uint32    `json:"uid"`
	Flags        []string  `json:"flags,omitempty"`
	Size         int64     `json:"size"`
	InternalDate time.Time `json:"internal_date"`
	Path         string    `json:"path"`   // path inside the archive
	SHA256       string    `json:"sha256"` // of the file content
	ThreadID     string    `json:"thread_id,omitempty"`
}

// Manifest is the archive index.
type Manifest struct {
	Version    int               `json:"version"`
	Mailbox    uuid.UUID         `json:"mailbox"`
	ExportedAt time.Time         `json:"exported_at"`
	ExportedBy string            `json:"exported_by"`
	Host       string            `json:"host"`
	Usage      index.Usage       `json:"usage"`
	ModSeq     uint64            `json:"modseq"`
	Hold       *identity.Hold    `json:"hold,omitempty"`
	Folders    []ManifestFolder  `json:"folders"`
	Messages   []ManifestMessage `json:"messages"`
	Accounts   []string          `json:"accounts"`
	AuditFrom  time.Time         `json:"audit_from"`
	AuditTo    time.Time         `json:"audit_to"`
	AuditCount int               `json:"audit_count"`
	Errors     []string          `json:"errors,omitempty"`
}

// Options for an export.
type Options struct {
	Gzip bool
	// Audit period to include; zero AuditFrom = 1 year back, zero AuditTo = now.
	AuditFrom, AuditTo time.Time
}

// Summary of an export or import.
type Summary struct {
	Messages int
	Folders  int
	Bytes    int64
	Audit    int
	Errors   []string
}

// maildirName maps a folder to its Maildir++ directory ("" = INBOX root).
func maildirName(f *index.Folder) string {
	if f.Role == index.RoleInbox {
		return ""
	}
	return "." + strings.ReplaceAll(f.Name, index.Delimiter, ".")
}

// flagLetters encodes IMAP flags and keywords as a Maildir info string.
func flagLetters(flags []string, keywords []string) string {
	var sys []byte
	var kw []byte
	for _, f := range flags {
		switch strings.ToLower(f) {
		case `\draft`:
			sys = append(sys, 'D')
		case `\flagged`:
			sys = append(sys, 'F')
		case `\answered`:
			sys = append(sys, 'R')
		case `\seen`:
			sys = append(sys, 'S')
		case `\deleted`:
			sys = append(sys, 'T')
		default:
			for i, k := range keywords {
				if strings.EqualFold(k, f) && i < 26 {
					kw = append(kw, byte('a'+i))
				}
			}
		}
	}
	sort.Slice(sys, func(i, j int) bool { return sys[i] < sys[j] })
	sort.Slice(kw, func(i, j int) bool { return kw[i] < kw[j] })
	return string(sys) + string(kw)
}

// parseFlagLetters decodes a Maildir info string back to flags.
func parseFlagLetters(info string, keywords []string) []string {
	var out []string
	for _, c := range info {
		switch {
		case c == 'D':
			out = append(out, index.FlagDraft)
		case c == 'F':
			out = append(out, index.FlagFlagged)
		case c == 'R':
			out = append(out, index.FlagAnswered)
		case c == 'S':
			out = append(out, index.FlagSeen)
		case c == 'T':
			out = append(out, index.FlagDeleted)
		case c >= 'a' && c <= 'z':
			if i := int(c - 'a'); i < len(keywords) {
				out = append(out, keywords[i])
			}
		}
	}
	return out
}

// ExportMailbox writes the archive to w.
func (s *Service) ExportMailbox(ctx context.Context, mb uuid.UUID, w io.Writer, opts Options) (Summary, error) {
	if err := s.authorize(ctx); err != nil {
		return Summary{}, err
	}
	mv, err := s.ids.GetMailbox(ctx, mb)
	if err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return Summary{}, ErrNotFound
		}
		return Summary{}, err
	}
	root, err := s.idx.GetRoot(ctx, mb)
	if err != nil && !errors.Is(err, index.ErrNotFound) {
		return Summary{}, err
	}
	now := s.Now().UTC()
	man := Manifest{Version: 1, Mailbox: mb, ExportedAt: now, ExportedBy: actor.From(ctx).Username, Host: s.Hostname, Hold: mv.Value.Hold}
	man.Accounts, _, _ = s.ids.ListMailboxAccounts(ctx, mb, "", 1000)
	man.AuditFrom, man.AuditTo = opts.AuditFrom, opts.AuditTo
	if man.AuditTo.IsZero() {
		man.AuditTo = now
	}
	if man.AuditFrom.IsZero() {
		man.AuditFrom = man.AuditTo.Add(-365 * 24 * time.Hour)
	}

	var out io.Writer = w
	var gz *gzip.Writer
	if opts.Gzip {
		gz = gzip.NewWriter(w)
		out = gz
	}
	tw := tar.NewWriter(out)
	prefix := "kittiwake-" + mb.String()
	sum := Summary{}
	addFile := func(name string, data []byte, mtime time.Time) error {
		if mtime.IsZero() {
			mtime = now
		}
		h := &tar.Header{Name: path.Join(prefix, name), Mode: 0o600, Size: int64(len(data)), ModTime: mtime, Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		_, err := tw.Write(data)
		return err
	}
	addDir := func(name string) error {
		return tw.WriteHeader(&tar.Header{Name: path.Join(prefix, name) + "/", Mode: 0o700, ModTime: now, Typeflag: tar.TypeDir})
	}

	if err == nil { // root exists
		man.Usage, man.ModSeq = root.Usage, root.ModSeq
		var subs []string
		for _, f := range root.Folders {
			md := maildirName(f)
			man.Folders = append(man.Folders, ManifestFolder{ID: f.ID, Name: f.Name, Maildir: md, Role: f.Role, UIDValidity: f.UIDValidity, UIDNext: f.UIDNext, ModSeq: f.ModSeq, Subscribed: f.Subscribed, Keywords: f.Keywords})
			if f.Subscribed && md != "" {
				subs = append(subs, strings.TrimPrefix(md, "."))
			}
			base := "maildir"
			if md != "" {
				base = path.Join("maildir", md)
			}
			for _, d := range []string{"cur", "new", "tmp"} {
				if err := addDir(path.Join(base, d)); err != nil {
					return sum, err
				}
			}
			if len(f.Keywords) > 0 {
				var kw strings.Builder
				for i, k := range f.Keywords {
					if i < 26 {
						fmt.Fprintf(&kw, "%d %s\n", i, k)
					}
				}
				if err := addFile(path.Join(base, "dovecot-keywords"), []byte(kw.String()), now); err != nil {
					return sum, err
				}
			}
			var uidlist strings.Builder
			fmt.Fprintf(&uidlist, "3 V%d N%d\n", f.UIDValidity, f.UIDNext)
			var after uint32
			for {
				recs, more, err := s.idx.ListFolder(ctx, mb, f.ID, after, 500)
				if err != nil {
					return sum, err
				}
				for _, r := range recs {
					after = r.UID
					raw, err := s.rawOf(ctx, mb, r.MessageID)
					if err != nil {
						man.Errors = append(man.Errors, fmt.Sprintf("%s/%d (%s): %v", f.ID, r.UID, r.MessageID, err))
						continue
					}
					base36 := fmt.Sprintf("%d.M%dP%d.%s,S=%d", r.InternalDate.Unix(), r.UID, 1, s.Hostname, len(raw))
					fname := base36 + ":2," + flagLetters(r.Flags, f.Keywords)
					p := path.Join(base, "cur", fname)
					if err := addFile(p, raw, r.InternalDate); err != nil {
						return sum, err
					}
					fmt.Fprintf(&uidlist, "%d :%s\n", r.UID, base36)
					h := sha256.Sum256(raw)
					man.Messages = append(man.Messages, ManifestMessage{
						MessageID: r.MessageID, Folder: f.ID, UID: r.UID, Flags: r.Flags, Size: int64(len(raw)),
						InternalDate: r.InternalDate, Path: path.Join(prefix, p), SHA256: hex.EncodeToString(h[:]), ThreadID: r.ThreadID,
					})
					sum.Messages++
					sum.Bytes += int64(len(raw))
				}
				if !more {
					break
				}
			}
			if err := addFile(path.Join(base, "dovecot-uidlist"), []byte(uidlist.String()), now); err != nil {
				return sum, err
			}
			sum.Folders++
		}
		if err := addFile("maildir/subscriptions", []byte(strings.Join(subs, "\n")+"\n"), now); err != nil {
			return sum, err
		}
	}
	// Audit log.
	var auditBuf bytes.Buffer
	enc := json.NewEncoder(&auditBuf)
	cursor := ""
	for {
		recs, next, err := s.auditSt.Read(ctx, audit.Query{Mailbox: mb, From: man.AuditFrom, To: man.AuditTo, After: cursor, Max: 1000})
		if err != nil {
			man.Errors = append(man.Errors, "audit: "+err.Error())
			break
		}
		for _, r := range recs {
			_ = enc.Encode(r)
			man.AuditCount++
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if err := addFile("audit.jsonl", auditBuf.Bytes(), now); err != nil {
		return sum, err
	}
	sum.Audit = man.AuditCount
	sum.Errors = man.Errors
	manJSON, _ := json.MarshalIndent(man, "", "  ")
	if err := addFile("manifest.json", manJSON, now); err != nil {
		return sum, err
	}
	if err := tw.Close(); err != nil {
		return sum, err
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return sum, err
		}
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "export", Detail: fmt.Sprintf("messages=%d bytes=%d audit=%d errors=%d", sum.Messages, sum.Bytes, sum.Audit, len(sum.Errors))})
	return sum, nil
}

// rawOf returns the kept original bytes or renders the message.
func (s *Service) rawOf(ctx context.Context, mb, msg uuid.UUID) ([]byte, error) {
	if kept, err := s.msgs.GetRaw(ctx, mb, msg); err == nil {
		return kept, nil
	} else if !errors.Is(err, message.ErrNotFound) {
		return nil, err
	}
	data, err := s.msgs.GetDoc(ctx, mb, msg)
	if err != nil {
		return nil, err
	}
	var doc mail.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return mail.Render(&doc, func(h string) ([]byte, error) { return s.atts.Get(ctx, mb, h) })
}

// ---- import ----

// ImportMaildir imports a Maildir (or Maildir++ tree) from a directory on
// disk into the mailbox. Folders are created from subdirectory names,
// flags from filenames (and dovecot-keywords), internal dates from file
// mtimes. Messages go through the normal parse-and-store path. UIDs are
// not preserved (the index assigns them).
func (s *Service) ImportMaildir(ctx context.Context, mb uuid.UUID, dir string) (Summary, error) {
	if err := s.authorize(ctx); err != nil {
		return Summary{}, err
	}
	if _, err := s.ids.GetMailbox(ctx, mb); err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return Summary{}, ErrNotFound
		}
		return Summary{}, err
	}
	root, err := s.idx.EnsureRoot(ctx, mb)
	if err != nil {
		return Summary{}, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Summary{}, err
	}
	var sum Summary
	importDir := func(folderDir, name string) {
		fid, err := s.folderFor(ctx, mb, &root, name)
		if err != nil {
			sum.Errors = append(sum.Errors, name+": "+err.Error())
			return
		}
		keywords := readKeywords(filepath.Join(folderDir, "dovecot-keywords"))
		for _, sub := range []string{"cur", "new"} {
			files, err := os.ReadDir(filepath.Join(folderDir, sub))
			if err != nil {
				continue
			}
			for _, fe := range files {
				if fe.IsDir() {
					continue
				}
				fp := filepath.Join(folderDir, sub, fe.Name())
				raw, err := os.ReadFile(fp)
				if err != nil {
					sum.Errors = append(sum.Errors, fp+": "+err.Error())
					continue
				}
				info, _ := fe.Info()
				mtime := time.Time{}
				if info != nil {
					mtime = info.ModTime()
				}
				flags := flagsFromFilename(fe.Name(), keywords)
				if _, err := s.del.Import(ctx, mb, fid, raw, flags, mtime); err != nil {
					sum.Errors = append(sum.Errors, fp+": "+err.Error())
					continue
				}
				sum.Messages++
				sum.Bytes += int64(len(raw))
			}
		}
		sum.Folders++
	}
	importDir(dir, "INBOX")
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), ".") && e.Name() != "." && e.Name() != ".." {
			importDir(filepath.Join(dir, e.Name()), strings.ReplaceAll(strings.TrimPrefix(e.Name(), "."), ".", index.Delimiter))
		}
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "import", Detail: fmt.Sprintf("dir=%s messages=%d folders=%d errors=%d", dir, sum.Messages, sum.Folders, len(sum.Errors))})
	return sum, nil
}

// ImportArchive imports a tar (optionally gzip) produced by ExportMailbox
// or any tar holding a Maildir++ tree under a single top-level directory.
func (s *Service) ImportArchive(ctx context.Context, mb uuid.UUID, r io.Reader) (Summary, error) {
	if err := s.authorize(ctx); err != nil {
		return Summary{}, err
	}
	tmp, err := os.MkdirTemp("", "kittiwake-import-")
	if err != nil {
		return Summary{}, err
	}
	defer os.RemoveAll(tmp)
	br := newPeekReader(r)
	if isGzip(br) {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return Summary{}, err
		}
		defer gz.Close()
		r = gz
	} else {
		r = br
	}
	tr := tar.NewReader(r)
	var top string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Summary{}, err
		}
		clean := path.Clean(h.Name)
		if strings.HasPrefix(clean, "..") || strings.HasPrefix(clean, "/") {
			return Summary{}, fmt.Errorf("export: unsafe path in archive: %s", h.Name)
		}
		if top == "" {
			top = strings.SplitN(clean, "/", 2)[0]
		}
		dst := filepath.Join(tmp, filepath.FromSlash(clean))
		switch h.Typeflag {
		case tar.TypeDir:
			_ = os.MkdirAll(dst, 0o700)
		case tar.TypeReg:
			_ = os.MkdirAll(filepath.Dir(dst), 0o700)
			f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return Summary{}, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return Summary{}, err
			}
			_ = f.Close()
			_ = os.Chtimes(dst, h.ModTime, h.ModTime)
		}
	}
	md := filepath.Join(tmp, top, "maildir")
	if _, err := os.Stat(md); err != nil {
		md = filepath.Join(tmp, top) // plain Maildir tar
	}
	return s.ImportMaildir(ctx, mb, md)
}

func (s *Service) folderFor(ctx context.Context, mb uuid.UUID, root *index.Root, name string) (string, error) {
	if name == "INBOX" {
		if f := root.FolderByRole(index.RoleInbox); f != nil {
			return f.ID, nil
		}
	}
	if f := root.FolderByName(name); f != nil {
		return f.ID, nil
	}
	// Map common Dovecot names onto role folders.
	for _, f := range root.Folders {
		if strings.EqualFold(f.Name, name) {
			return f.ID, nil
		}
	}
	f, err := s.idx.CreateFolder(ctx, mb, name, "")
	if err != nil {
		return "", err
	}
	r, err := s.idx.GetRoot(ctx, mb)
	if err == nil {
		*root = r
	}
	return f.ID, nil
}

func readKeywords(p string) []string {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	kw := make([]string, 26)
	for _, line := range strings.Split(string(b), "\n") {
		var i int
		var k string
		if _, err := fmt.Sscanf(line, "%d %s", &i, &k); err == nil && i >= 0 && i < 26 {
			kw[i] = k
		}
	}
	return kw
}

func flagsFromFilename(name string, keywords []string) []string {
	i := strings.LastIndex(name, ":2,")
	if i < 0 {
		return nil
	}
	return parseFlagLetters(name[i+3:], keywords)
}

type peekReader struct {
	r    io.Reader
	head []byte
	used bool
}

func newPeekReader(r io.Reader) *peekReader {
	buf := make([]byte, 2)
	n, _ := io.ReadFull(r, buf)
	return &peekReader{r: r, head: buf[:n]}
}

func isGzip(p *peekReader) bool { return len(p.head) == 2 && p.head[0] == 0x1f && p.head[1] == 0x8b }

func (p *peekReader) Read(b []byte) (int, error) {
	if !p.used && len(p.head) > 0 {
		n := copy(b, p.head)
		p.head = p.head[n:]
		if len(p.head) == 0 {
			p.used = true
		}
		return n, nil
	}
	return p.r.Read(b)
}
