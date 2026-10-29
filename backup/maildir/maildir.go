// Package maildir writes a Dovecot-compatible Maildir++ tar archive from
// any source of folders and messages: cur/new/tmp per folder, subfolders
// as ".Folder.Sub" directories, flags in the file name, keywords in
// dovecot-keywords, UIDs in dovecot-uidlist, the internal date as mtime,
// plus a subscriptions file. The backup service feeds it from a backup
// run; the layout is what Dovecot and standard tools read.
package maildir

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"kittiwake/store/index"
)

// Folder describes one folder being written.
type Folder struct {
	Name        string
	Role        string
	UIDValidity uint32
	UIDNext     uint32
	Subscribed  bool
	Keywords    []string
}

// File is one message to write.
type File struct {
	UID          uint32
	Flags        []string
	InternalDate time.Time
	Data         []byte
}

// Written describes a written message file (for a manifest).
type Written struct {
	Path   string
	Size   int64
	SHA256 string
}

// Writer builds the archive.
type Writer struct {
	tw       *tar.Writer
	prefix   string
	hostname string
	now      time.Time
	subs     []string
	dirs     map[string]bool
}

// New starts an archive under the top-level directory prefix.
func New(tw *tar.Writer, prefix, hostname string, now time.Time) *Writer {
	return &Writer{tw: tw, prefix: prefix, hostname: hostname, now: now, dirs: map[string]bool{}}
}

// Name maps a folder to its Maildir++ directory ("" = INBOX root).
func Name(f Folder) string {
	if f.Role == index.RoleInbox {
		return ""
	}
	return "." + strings.ReplaceAll(f.Name, index.Delimiter, ".")
}

// FlagLetters encodes IMAP flags and keywords as a Maildir info string.
func FlagLetters(flags []string, keywords []string) string {
	var sys, kw []byte
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

// AddFile writes a plain file at name under the prefix.
func (w *Writer) AddFile(name string, data []byte, mtime time.Time) error {
	if mtime.IsZero() {
		mtime = w.now
	}
	h := &tar.Header{Name: path.Join(w.prefix, name), Mode: 0o600, Size: int64(len(data)), ModTime: mtime, Typeflag: tar.TypeReg}
	if err := w.tw.WriteHeader(h); err != nil {
		return err
	}
	_, err := w.tw.Write(data)
	return err
}

func (w *Writer) addDir(name string) error {
	if w.dirs[name] {
		return nil
	}
	w.dirs[name] = true
	return w.tw.WriteHeader(&tar.Header{Name: path.Join(w.prefix, name) + "/", Mode: 0o700, ModTime: w.now, Typeflag: tar.TypeDir})
}

// Folder writes one folder: directories, keywords, every message from
// next (called until it returns io.EOF) and the uidlist.
func (w *Writer) Folder(f Folder, next func() (File, error)) ([]Written, error) {
	md := Name(f)
	if f.Subscribed && md != "" {
		w.subs = append(w.subs, strings.TrimPrefix(md, "."))
	}
	base := "maildir"
	if md != "" {
		base = path.Join("maildir", md)
	}
	for _, d := range []string{"", "cur", "new", "tmp"} {
		if err := w.addDir(path.Join(base, d)); err != nil {
			return nil, err
		}
	}
	if len(f.Keywords) > 0 {
		var kw strings.Builder
		for i, k := range f.Keywords {
			if i < 26 {
				fmt.Fprintf(&kw, "%d %s\n", i, k)
			}
		}
		if err := w.AddFile(path.Join(base, "dovecot-keywords"), []byte(kw.String()), w.now); err != nil {
			return nil, err
		}
	}
	var uidlist strings.Builder
	fmt.Fprintf(&uidlist, "3 V%d N%d\n", f.UIDValidity, f.UIDNext)
	var out []Written
	for {
		m, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
		base36 := fmt.Sprintf("%d.M%dP%d.%s,S=%d", m.InternalDate.Unix(), m.UID, 1, w.hostname, len(m.Data))
		fname := base36 + ":2," + FlagLetters(m.Flags, f.Keywords)
		p := path.Join(base, "cur", fname)
		if err := w.AddFile(p, m.Data, m.InternalDate); err != nil {
			return out, err
		}
		fmt.Fprintf(&uidlist, "%d :%s\n", m.UID, base36)
		sum := sha256.Sum256(m.Data)
		out = append(out, Written{Path: path.Join(w.prefix, p), Size: int64(len(m.Data)), SHA256: hex.EncodeToString(sum[:])})
	}
	if err := w.AddFile(path.Join(base, "dovecot-uidlist"), []byte(uidlist.String()), w.now); err != nil {
		return out, err
	}
	return out, nil
}

// Finish writes the subscriptions file. The caller closes the tar.
func (w *Writer) Finish() error {
	if err := w.addDir("maildir"); err != nil {
		return err
	}
	return w.AddFile("maildir/subscriptions", []byte(strings.Join(w.subs, "\n")+"\n"), w.now)
}
