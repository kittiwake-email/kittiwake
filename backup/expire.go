package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup/target"
	"kittiwake/store/identity"
)

// ExpireResult counts what an expiry pass removed.
type ExpireResult struct {
	Runs    int `json:"runs"`
	Packs   int `json:"packs"`
	Blobs   int `json:"blobs"`
	Exports int `json:"exports"`
	// Mailboxes whose last run went: every object of theirs is gone.
	Mailboxes int `json:"mailboxes"`
}

// Expire removes, in one namespace, runs past their retention, packs no
// remaining run references, export archives past their expiry and blobs
// no mailbox references any more. Runs of a mailbox on hold are kept. It
// is the periodic task of the backup mode per namespace; each call is
// bounded.
func (s *Service) Expire(ctx context.Context, namespace string, limit int) (ExpireResult, error) {
	if err := s.authorize(ctx); err != nil {
		return ExpireResult{}, err
	}
	ns, err := identity.NormalizeNamespace(namespace)
	if err != nil {
		return ExpireResult{}, err
	}
	if ctx, err = s.enter(ctx, ns); err != nil {
		return ExpireResult{}, err
	}
	now := s.Now().UTC()
	var res ExpireResult
	runs, err := s.cat.ExpiredRuns(ctx, ns, now, limit)
	if err != nil {
		return res, err
	}
	touched := map[uuid.UUID]bool{}
	for _, r := range runs {
		if mv, err := s.st.Identity.GetMailbox(ctx, r.Mailbox); err == nil && mv.Value.Hold != nil {
			continue // on hold: nothing expires
		} else if err != nil && !errors.Is(err, identity.ErrNotFound) {
			return res, err
		}
		if err := s.be(ctx).tg.Delete(ctx, r.ManifestKey); err != nil && !errors.Is(err, target.ErrLocked) {
			return res, fmt.Errorf("delete %s: %w", r.ManifestKey, err)
		} else if errors.Is(err, target.ErrLocked) {
			continue // the lock outlives our retention by the write-to-expiry skew; next pass
		}
		if err := s.cat.DeleteRun(ctx, r.Mailbox, r.ID); err != nil {
			return res, err
		}
		res.Runs++
		touched[r.Mailbox] = true
	}
	for mb := range touched {
		n, gone, err := s.collectPacks(ctx, mb)
		if err != nil {
			return res, err
		}
		res.Packs += n
		if gone {
			res.Mailboxes++
		}
	}
	jobs, err := s.cat.ExpiredJobs(ctx, ns, now, limit)
	if err != nil {
		return res, err
	}
	for _, j := range jobs {
		if j.ObjectKey != "" {
			if err := s.be(ctx).tg.Delete(ctx, j.ObjectKey); err != nil {
				return res, err
			}
		}
		if err := s.cat.DeleteJob(ctx, j.ID); err != nil {
			return res, err
		}
		res.Exports++
	}
	blobs, err := s.cat.UnreferencedBlobs(ctx, ns, now.Add(-s.cfg.StrayGrace), limit)
	if err != nil {
		return res, err
	}
	for _, b := range blobs {
		if err := s.be(ctx).tg.Delete(ctx, s.blobKey(b.Name)); err != nil {
			if errors.Is(err, target.ErrLocked) {
				continue
			}
			return res, err
		}
		if err := s.cat.DeleteBlob(ctx, b.Name); err != nil {
			return res, err
		}
		res.Blobs++
	}
	if res.Runs+res.Packs+res.Blobs+res.Exports > 0 {
		s.log.Info("backup expiry", slog.String("namespace", ns), slog.Int("runs", res.Runs), slog.Int("packs", res.Packs), slog.Int("blobs", res.Blobs), slog.Int("exports", res.Exports), slog.Int("mailboxes_gone", res.Mailboxes))
	}
	return res, nil
}

// collectPacks deletes the packs under a mailbox prefix that no remaining
// run references. When no run remains, everything under the prefix goes
// and the mailbox's blob references are released. gone reports the latter.
func (s *Service) collectPacks(ctx context.Context, mb uuid.UUID) (int, bool, error) {
	runs, err := s.cat.ListRuns(ctx, mb, 0)
	if err != nil {
		return 0, false, err
	}
	referenced := map[string]bool{}
	for _, r := range runs {
		referenced[r.ManifestKey] = true
		for _, p := range r.Packs {
			referenced[p] = true
		}
	}
	var victims []string
	if err := s.be(ctx).tg.List(ctx, s.mailboxPrefix(mb), func(o target.Object) error {
		if !referenced[o.Key] && (len(runs) == 0 || strings.HasSuffix(o.Key, ".pack")) {
			victims = append(victims, o.Key)
		}
		return nil
	}); err != nil {
		return 0, false, err
	}
	n := 0
	for _, k := range victims {
		if err := s.be(ctx).tg.Delete(ctx, k); err != nil {
			if errors.Is(err, target.ErrLocked) {
				continue
			}
			return n, false, err
		}
		n++
	}
	if len(runs) == 0 {
		return n, true, s.cat.DropBlobRefs(ctx, mb)
	}
	return n, false, nil
}

// Maintain is the per-mailbox periodic task: expired-run pack collection
// (runs themselves expire globally) and verification. It schedules the
// next maintenance according to the configured interval.
func (s *Service) Maintain(ctx context.Context, mb uuid.UUID) (Report, error) {
	if err := s.authorize(ctx); err != nil {
		return Report{}, err
	}
	ns, err := s.namespaceOf(ctx, mb)
	if err != nil {
		return Report{}, err
	}
	if ctx, err = s.enter(ctx, ns); err != nil {
		return Report{}, err
	}
	if _, _, err := s.collectPacks(ctx, mb); err != nil {
		return Report{}, err
	}
	rep, err := s.Verify(ctx, mb, VerifyOptions{})
	if err != nil {
		return rep, err
	}
	return rep, s.cat.ScheduleMaintenance(ctx, mb, s.Now().UTC().Add(Jitter(s.cfg.MaintainInterval)))
}

// Forget removes every backup of a mailbox now (an explicit admin
// action, refused for mailboxes on hold).
func (s *Service) Forget(ctx context.Context, mb uuid.UUID) error {
	if err := s.authorize(ctx); err != nil {
		return err
	}
	if mv, err := s.st.Identity.GetMailbox(ctx, mb); err == nil && mv.Value.Hold != nil {
		return fmt.Errorf("%w: mailbox is on hold", ErrForbidden)
	}
	ns, err := s.namespaceOf(ctx, mb)
	if err != nil {
		return err
	}
	if ctx, err = s.enter(ctx, ns); err != nil {
		return err
	}
	runs, err := s.cat.ListRuns(ctx, mb, 0)
	if err != nil {
		return err
	}
	for _, r := range runs {
		if err := s.cat.DeleteRun(ctx, mb, r.ID); err != nil {
			return err
		}
	}
	_, _, err = s.collectPacks(ctx, mb)
	return err
}

var _ = time.Second
