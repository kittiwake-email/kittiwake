package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup/target"
	"kittiwake/service/auditor"
)

// VerifyOptions tune a verification.
type VerifyOptions struct {
	// Sample: entries opened and hashed (0: configured default); Full
	// checks every entry of the latest run.
	Sample int
	Full   bool
}

// Report is the outcome of a verification.
type Report struct {
	Mailbox  uuid.UUID `json:"mailbox"`
	At       time.Time `json:"at"`
	Runs     int       `json:"runs"`
	Objects  int       `json:"objects"` // objects listed under the mailbox prefix
	Checked  int       `json:"checked"` // entries opened and hashed
	Missing  []string  `json:"missing,omitempty"`
	Mismatch []string  `json:"mismatch,omitempty"` // size differs from the catalog
	Corrupt  []string  `json:"corrupt,omitempty"`
	Stray    []string  `json:"stray,omitempty"`   // on the target, unknown to the catalog
	Removed  []string  `json:"removed,omitempty"` // strays past the grace period, deleted
	OK       bool      `json:"ok"`
}

func (r *Report) problem() string {
	var parts []string
	if n := len(r.Missing); n > 0 {
		parts = append(parts, fmt.Sprintf("%d missing", n))
	}
	if n := len(r.Mismatch); n > 0 {
		parts = append(parts, fmt.Sprintf("%d size mismatch", n))
	}
	if n := len(r.Corrupt); n > 0 {
		parts = append(parts, fmt.Sprintf("%d corrupt", n))
	}
	return strings.Join(parts, ", ")
}

// Verify checks the backups of a mailbox against the target: every
// manifest and pack the catalog knows must be there with the recorded
// size (one listing, no downloads), a sample of entries of the latest run
// must decrypt to the recorded hash, and objects the catalog does not
// know are reported and, past the grace period, removed. A failure forces
// the next backup run to be a full one and is recorded on the mailbox.
func (s *Service) Verify(ctx context.Context, mb uuid.UUID, opts VerifyOptions) (Report, error) {
	if err := s.authorize(ctx); err != nil {
		return Report{}, err
	}
	now := s.Now().UTC()
	rep := Report{Mailbox: mb, At: now}
	ns, err := s.namespaceOf(ctx, mb)
	if err != nil {
		return rep, err
	}
	if ctx, err = s.enter(ctx, ns); err != nil {
		return rep, err
	}
	runs, err := s.cat.ListRuns(ctx, mb, 0)
	if err != nil {
		return rep, err
	}
	rep.Runs = len(runs)
	// Presence: one listing of the mailbox prefix.
	listed := map[string]target.Object{}
	if err := s.be(ctx).tg.List(ctx, s.mailboxPrefix(mb), func(o target.Object) error {
		listed[o.Key] = o
		rep.Objects++
		return nil
	}); err != nil {
		return rep, err
	}
	known := map[string]bool{}
	var manifests []Run
	for _, r := range runs {
		known[r.ManifestKey] = true
		if o, ok := listed[r.ManifestKey]; !ok {
			rep.Missing = append(rep.Missing, r.ManifestKey)
		} else if o.Size == 0 {
			rep.Mismatch = append(rep.Mismatch, r.ManifestKey)
		}
		for _, p := range r.Packs {
			known[p] = true
			if _, ok := listed[p]; !ok {
				rep.Missing = append(rep.Missing, p)
			}
		}
		manifests = append(manifests, r)
	}
	rep.Missing = uniqueStrings(rep.Missing)
	for key, o := range listed {
		if known[key] {
			continue
		}
		rep.Stray = append(rep.Stray, key)
		if now.Sub(o.Modified) > s.cfg.StrayGrace {
			if err := s.be(ctx).tg.Delete(ctx, key); err == nil {
				rep.Removed = append(rep.Removed, key)
			}
		}
	}
	// Content: a sample of the latest run's entries.
	if len(manifests) > 0 {
		latest := manifests[0]
		if man, err := s.loadManifest(ctx, latest.ManifestKey); err != nil {
			rep.Corrupt = append(rep.Corrupt, latest.ManifestKey)
		} else {
			s.verifyEntries(ctx, man, opts, &rep)
		}
	}
	rep.OK = len(rep.Missing) == 0 && len(rep.Mismatch) == 0 && len(rep.Corrupt) == 0
	errText := ""
	if !rep.OK {
		errText = truncate(rep.problem(), 250)
		_ = s.cat.ForceFull(ctx, mb, true)
	}
	if err := s.cat.SetVerified(ctx, mb, now, errText); err != nil {
		return rep, err
	}
	res := ""
	if !rep.OK {
		res = "error"
	}
	s.audit.Log(ctx, auditor.Event{Mailbox: mb, Action: "backup-verify", Detail: fmt.Sprintf("runs=%d objects=%d checked=%d missing=%d corrupt=%d stray=%d removed=%d", rep.Runs, rep.Objects, rep.Checked, len(rep.Missing), len(rep.Corrupt), len(rep.Stray), len(rep.Removed)), Result: res})
	if !rep.OK {
		s.log.Warn("backup verification failed", slog.String("mailbox", mb.String()), slog.String("problem", errText))
	}
	return rep, nil
}

// verifyEntries opens a sample of references of a manifest.
func (s *Service) verifyEntries(ctx context.Context, man *Manifest, opts VerifyOptions, rep *Report) {
	var refs []Ref
	for _, fr := range man.Folders {
		refs = append(refs, fr.Records)
	}
	if man.Retention != nil {
		refs = append(refs, *man.Retention)
	}
	for _, d := range man.Audit {
		refs = append(refs, d.Ref)
	}
	sample := opts.Sample
	if sample <= 0 {
		sample = s.cfg.VerifySample
	}
	// Folder entries first (cheap, and they lead to the message refs).
	var msgRefs []Ref
	for _, fr := range man.Folders {
		if !opts.Full && len(msgRefs) >= sample*4 {
			break
		}
		fe, err := s.loadFolder(ctx, fr.Records)
		if err != nil {
			rep.Corrupt = append(rep.Corrupt, fmt.Sprintf("%s@%d", fr.Records.Key, fr.Records.Offset))
			continue
		}
		rep.Checked++
		for _, m := range fe.Messages {
			msgRefs = append(msgRefs, m.Doc)
			if m.Raw != nil {
				msgRefs = append(msgRefs, *m.Raw)
			}
			for _, a := range m.Attachments {
				msgRefs = append(msgRefs, a.Ref)
			}
		}
	}
	if !opts.Full && len(msgRefs) > sample {
		rand.Shuffle(len(msgRefs), func(i, j int) { msgRefs[i], msgRefs[j] = msgRefs[j], msgRefs[i] })
		msgRefs = msgRefs[:sample]
	}
	for _, ref := range append(refs[len(man.Folders):], msgRefs...) {
		if err := ctx.Err(); err != nil {
			return
		}
		if _, err := readRef(ctx, s.be(ctx).tg, s.be(ctx).ring, ref); err != nil {
			if errors.Is(err, target.ErrNotFound) {
				rep.Missing = append(rep.Missing, ref.Key)
			} else {
				rep.Corrupt = append(rep.Corrupt, fmt.Sprintf("%s@%d", ref.Key, ref.Offset))
			}
			continue
		}
		rep.Checked++
	}
}
