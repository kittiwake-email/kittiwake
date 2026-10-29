package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup/crypto"
	"kittiwake/backup/target"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/message"
)

// Config tunes the backup service.
type Config struct {
	// Prefix is prepended to every key on the target ("kittiwake/").
	Prefix string
	// PackSize caps one pack object; a run writes several when needed.
	PackSize int64
	// BlobThreshold: attachments at least this large are stored as
	// deduplicated blobs instead of pack entries.
	BlobThreshold int64
	// SpoolDir holds packs and exports while they are being built.
	SpoolDir string
	// DefaultInterval between runs of a mailbox without its own setting;
	// MinInterval and MaxInterval bound every setting.
	DefaultInterval time.Duration
	MinInterval     time.Duration
	MaxInterval     time.Duration
	// DefaultRetention of runs; MaintainInterval between verification
	// passes of a mailbox; ExportExpiry of export archives; StrayGrace
	// before an object unknown to the catalog may be deleted.
	DefaultRetention time.Duration
	MaintainInterval time.Duration
	ExportExpiry     time.Duration
	StrayGrace       time.Duration
	// Lock asks the target to retain objects for the retention period.
	Lock bool
	// VerifySample: entries checked by content per verification pass.
	VerifySample int
	// ScanBatch is the page size of index listings.
	ScanBatch int
	// AuditDays is how far back the first run collects the audit log.
	AuditDays int
}

// Defaults fills unset fields.
func (c Config) Defaults() Config {
	if c.PackSize <= 0 {
		c.PackSize = 1 << 30
	}
	if c.BlobThreshold <= 0 {
		c.BlobThreshold = 1 << 20
	}
	if c.SpoolDir == "" {
		c.SpoolDir = os.TempDir()
	}
	if c.DefaultInterval <= 0 {
		c.DefaultInterval = 24 * time.Hour
	}
	if c.MinInterval <= 0 {
		c.MinInterval = 30 * time.Minute
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = 30 * 24 * time.Hour
	}
	if c.DefaultRetention <= 0 {
		c.DefaultRetention = 90 * 24 * time.Hour
	}
	if c.MaintainInterval <= 0 {
		c.MaintainInterval = 7 * 24 * time.Hour
	}
	if c.ExportExpiry <= 0 {
		c.ExportExpiry = 7 * 24 * time.Hour
	}
	if c.StrayGrace <= 0 {
		c.StrayGrace = 48 * time.Hour
	}
	if c.VerifySample <= 0 {
		c.VerifySample = 16
	}
	if c.ScanBatch <= 0 {
		c.ScanBatch = 500
	}
	if c.AuditDays <= 0 {
		c.AuditDays = 365
	}
	return c
}

// Stores the service reads from and restores into.
type Stores struct {
	Identity    *identity.Store
	Index       *index.Store
	Messages    *message.Store
	Attachments *attach.Store
	Audit       *audit.Store
}

// Service runs backups, restores, exports, verification and GC.
type Service struct {
	cfg   Config
	st    Stores
	cat   Catalog
	tg    target.Target
	ring  *crypto.Ring
	adm   *admin.Service
	audit *auditor.Auditor
	log   *slog.Logger
	// Now is the clock; Hostname names this node in manifests.
	Now      func() time.Time
	Hostname string
}

// New returns a service. adm creates the mailbox a restore lands in.
func New(cfg Config, st Stores, cat Catalog, tg target.Target, ring *crypto.Ring, adm *admin.Service, aud *auditor.Auditor, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	h, _ := os.Hostname()
	if h == "" {
		h = "kittiwake"
	}
	return &Service{cfg: cfg.Defaults(), st: st, cat: cat, tg: tg, ring: ring, adm: adm, audit: aud, log: log, Now: time.Now, Hostname: h}
}

// Catalog returns the catalog (the admin API reads state through it).
func (s *Service) Catalog() Catalog { return s.cat }

// Config returns the effective configuration.
func (s *Service) Config() Config { return s.cfg }

func (s *Service) authorize(ctx context.Context) error {
	a := actor.From(ctx)
	if a.IsAdmin() || a.IsSystem() {
		return nil
	}
	return ErrForbidden
}

func (s *Service) key(parts ...string) string {
	return s.cfg.Prefix + strings.Join(parts, "/")
}

func (s *Service) mailboxPrefix(mb uuid.UUID) string { return s.key("mb", mb.String()) + "/" }
func (s *Service) runPrefix(mb uuid.UUID, run string) string {
	return s.key("mb", mb.String(), run)
}
func (s *Service) manifestKey(mb uuid.UUID, run string) string {
	return s.runPrefix(mb, run) + ".manifest"
}
func (s *Service) blobKey(name string) string { return s.key("blob", name[:2], name) }
func (s *Service) exportKey(mb uuid.UUID, run string, at time.Time) string {
	return s.key("export", mb.String(), run+"-"+at.UTC().Format("20060102T150405Z")+".tar.gz")
}

// Policy returns the effective interval and retention of a mailbox: its
// own settings, else the domain settings of its first account, else the
// defaults; bounded by the configured limits. every is 0 when disabled.
func (s *Service) Policy(ctx context.Context, m identity.Mailbox) (every, retention time.Duration) {
	every, retention = m.Backup.Interval, m.Backup.Retention
	if m.Backup.Disabled {
		every = -1
	}
	if every == 0 || retention == 0 {
		if names, _, err := s.st.Identity.ListMailboxAccounts(ctx, m.ID, "", 1); err == nil && len(names) > 0 {
			if _, domain, ok := strings.Cut(names[0], "@"); ok {
				if d, err := s.st.Identity.GetDomain(ctx, domain); err == nil {
					if every == 0 {
						every = parseSetting(d.Value.Settings[identity.DomainBackupInterval])
					}
					if retention == 0 {
						retention = parseSetting(d.Value.Settings[identity.DomainBackupRetention])
					}
				}
			}
		}
	}
	if every == 0 {
		every = s.cfg.DefaultInterval
	}
	if retention == 0 {
		retention = s.cfg.DefaultRetention
	}
	if every < 0 {
		return 0, retention
	}
	return s.ClampInterval(every), retention
}

// ClampInterval bounds an interval to the configured limits.
func (s *Service) ClampInterval(d time.Duration) time.Duration {
	if d < s.cfg.MinInterval {
		return s.cfg.MinInterval
	}
	if d > s.cfg.MaxInterval {
		return s.cfg.MaxInterval
	}
	return d
}

// ValidateInterval checks a requested interval against the limits.
func (s *Service) ValidateInterval(d time.Duration) error {
	if d < 0 || (d > 0 && (d < s.cfg.MinInterval || d > s.cfg.MaxInterval)) {
		return fmt.Errorf("%w: backup interval must be between %s and %s (0 inherits)", ErrInvalid, s.cfg.MinInterval, s.cfg.MaxInterval)
	}
	return nil
}

// parseSetting reads a domain setting: a Go duration; "off" / "0" disable.
func parseSetting(v string) time.Duration {
	v = strings.TrimSpace(v)
	switch v {
	case "":
		return 0
	case "off", "0", "false", "disabled":
		return -1
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

// ParseDomainSetting validates a domain backup setting value.
func ParseDomainSetting(v string) error {
	v = strings.TrimSpace(v)
	switch v {
	case "", "off", "0", "false", "disabled":
		return nil
	}
	if _, err := time.ParseDuration(v); err != nil {
		return fmt.Errorf("%w: %q is not a duration (e.g. 24h) or off", ErrInvalid, v)
	}
	return nil
}

// Enable schedules a mailbox according to its policy, spreading the first
// run over one interval. Called when a mailbox is created or its settings
// change.
func (s *Service) Enable(ctx context.Context, m identity.Mailbox) error {
	every, _ := s.Policy(ctx, m)
	now := s.Now().UTC()
	if err := s.cat.Schedule(ctx, m.ID, every, FirstDue(now, every)); err != nil {
		return err
	}
	return s.cat.ScheduleMaintenance(ctx, m.ID, FirstDue(now, s.cfg.MaintainInterval))
}

func (s *Service) retainUntil(now time.Time, retention time.Duration) time.Time {
	if !s.cfg.Lock || !s.tg.Locking() {
		return time.Time{}
	}
	return now.Add(retention)
}

// loadManifest fetches and opens a manifest.
func (s *Service) loadManifest(ctx context.Context, key string) (*Manifest, error) {
	sealed, err := target.ReadAll(ctx, s.tg, key)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", key, err)
	}
	plain, err := s.ring.Open(sealed)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", key, err)
	}
	var m Manifest
	if err := decodeJSON(plain, &m); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", key, err)
	}
	return &m, nil
}

// loadFolder fetches a folder entry.
func (s *Service) loadFolder(ctx context.Context, ref Ref) (*FolderEntry, error) {
	b, err := readRef(ctx, s.tg, s.ring, ref)
	if err != nil {
		return nil, err
	}
	var fe FolderEntry
	if err := decodeJSON(b, &fe); err != nil {
		return nil, err
	}
	return &fe, nil
}

func (s *Service) loadRetained(ctx context.Context, ref *Ref) ([]Retained, error) {
	if ref == nil {
		return nil, nil
	}
	b, err := readRef(ctx, s.tg, s.ring, *ref)
	if err != nil {
		return nil, err
	}
	var out []Retained
	return out, decodeJSON(b, &out)
}

// Manifest returns the manifest of a run (admin inspection).
func (s *Service) Manifest(ctx context.Context, mb uuid.UUID, runID string) (*Manifest, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	run, err := s.cat.GetRun(ctx, mb, runID)
	if err != nil {
		return nil, err
	}
	return s.loadManifest(ctx, run.ManifestKey)
}

// DownloadURL returns a presigned link for an object when the target can
// issue one (S3); otherwise the empty string and the archive is fetched
// from the target by its key (a path under the directory for the
// filesystem target).
func (s *Service) DownloadURL(key string, validity time.Duration) string {
	if p, ok := s.tg.(target.Presigner); ok {
		if u, err := p.PresignGet(key, validity); err == nil {
			return u
		}
	}
	return ""
}
