// Package config loads Kittiwake's single YAML configuration file.
package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"kittiwake/auth"
)

// Config is the whole configuration file.
type Config struct {
	// Modes to run in this process: lmtp, imap, pop3, jmap, submission,
	// admin, socketmap, worker, backup, proxy. Optional: a systemd instance
	// passes its mode with --modes instead; the list is for running several
	// modes in one process. The proxy mode runs alone: it holds no mail
	// state and needs only a read-only database connection.
	Modes []string `yaml:"modes"`
	// Namespaces this process serves; empty means every namespace. A
	// restricted process connects only to the storage of its namespaces,
	// takes only their tasks and refuses logins and deliveries for
	// mailboxes elsewhere (clearly: the client or MTA retries on a server
	// that serves them). Every mode honours the list.
	Namespaces []string `yaml:"namespaces"`

	Ceph       Ceph       `yaml:"ceph"`
	Listen     Listen     `yaml:"listen"`
	Proxy      Proxy      `yaml:"proxy"`
	SMTP       SMTP       `yaml:"smtp"`
	Submission Submission `yaml:"submission"`
	Mail       Mail       `yaml:"mail"`
	Quota      Quota      `yaml:"quota"`
	Database   Database   `yaml:"database"`
	Node       Node       `yaml:"node"`
	IMAP       IMAP       `yaml:"imap"`
	Session    Session    `yaml:"session"`
	Cache      Cache      `yaml:"cache"`
	Audit      Audit      `yaml:"audit"`
	Backup     Backup     `yaml:"backup"`
	Log        Log        `yaml:"log"`
}

// Ceph is the storage connection and pool layout.
type Ceph struct {
	PlacementReserve float64 `yaml:"placement_reserve"`
	// Backend is "librados" (default) or "memory" (in-process, volatile;
	// for development and tests only).
	Backend string `yaml:"backend"`
	// ConfigFile and Options apply to every storage connection (librados
	// defaults such as ms_bind_ipv6). Monitors, credentials and pools of a
	// storage live in the registry (database), not here; with the memory
	// backend Pools names the pools of the one in-memory storage.
	ConfigFile string            `yaml:"config_file"`
	Options    map[string]string `yaml:"options"`
	Monitors   []string          `yaml:"monitors"`
	User       string            `yaml:"user"`
	Key        Secret            `yaml:"key"`
	Keyring    string            `yaml:"keyring"`
	Pools      Pools             `yaml:"pools"`
}

// Pools names the RADOS pools of the memory backend's single storage.
// With librados every storage's pools are in its registry record.
type Pools struct {
	Messages    string `yaml:"messages"`
	Attachments string `yaml:"attachments"`
	Index       string `yaml:"index"`
	Meta        string `yaml:"meta"`
}

// Listen holds one listener per mode plus the management listener.
type Listen struct {
	LMTP Listener `yaml:"lmtp"`
	IMAP Listener `yaml:"imap"`
	POP3 Listener `yaml:"pop3"`
	JMAP Listener `yaml:"jmap"` // JMAP, OAuth token and self-service endpoints
	// Submission: SMTP message submission (RFC 6409) behind HAProxy, which
	// terminates implicit TLS on port 465 (RFC 8314).
	Submission Listener `yaml:"submission"`
	Admin      Listener `yaml:"admin"`     // admin REST API, operators only
	Socketmap  Listener `yaml:"socketmap"` // MTA recipient/domain/alias lookups (Postfix socketmap protocol)
	Worker     Listener `yaml:"worker"`    // no listener of its own; only its management address
	Backup     Listener `yaml:"backup"`    // no listener of its own; only its management address
	// Proxy: the listeners of the proxy mode, one per fronted protocol,
	// and its management address. They are separate from the modes'
	// own listeners so a proxy may share a box with backends.
	Proxy ProxyListen `yaml:"proxy"`
	// Management is the metrics/health listener of a process that runs
	// several modes, or of a mode whose entry has no management address.
	Management Listener `yaml:"management"`
}

// ProxyListen holds the proxy mode's listeners.
type ProxyListen struct {
	IMAP       Listener `yaml:"imap"`
	POP3       Listener `yaml:"pop3"`
	JMAP       Listener `yaml:"jmap"`
	Submission Listener `yaml:"submission"`
	Management string   `yaml:"management"`
}

// Listener is one network listener.
type Listener struct {
	// Address in Go net form, e.g. "[::]:24" or "/run/kittiwake/lmtp.sock".
	Address string `yaml:"address"`
	// ProxyProtocol: "off", "optional" (accept with or without header) or
	// "required" (reject connections without a PROXY header).
	ProxyProtocol string `yaml:"proxy_protocol"`
	// Management is the metrics/health address of the process that runs
	// this mode on its own (one systemd instance per mode). Every process
	// needs its own, so it lives next to the mode it belongs to.
	Management string `yaml:"management"`
}

// ByMode returns the listener of a mode.
func (l Listen) ByMode(mode string) Listener {
	switch mode {
	case "lmtp":
		return l.LMTP
	case "imap":
		return l.IMAP
	case "pop3":
		return l.POP3
	case "jmap":
		return l.JMAP
	case "submission":
		return l.Submission
	case "admin":
		return l.Admin
	case "socketmap":
		return l.Socketmap
	case "worker":
		return l.Worker
	case "backup":
		return l.Backup
	case "proxy":
		return Listener{Management: l.Proxy.Management}
	}
	return Listener{}
}

// ManagementAddress returns the metrics/health address for a process
// running the given modes: the mode's own management address when the
// process runs exactly one mode that has one, else listen.management.
func (l Listen) ManagementAddress(modes []string) string {
	if len(modes) == 1 {
		if m := l.ByMode(modes[0]).Management; m != "" {
			return m
		}
	}
	return l.Management.Address
}

// Proxy: what every daemon trusts in front of it, and the settings of the
// proxy mode itself (README "Proxies").
type Proxy struct {
	// TrustedAddresses may set X-Forwarded-For on HTTP: the TLS terminator
	// in front, and the proxy mode when it is in front of this daemon.
	TrustedAddresses []string `yaml:"trusted_addresses"`
	// IngressTLS is the policy for a namespace ingress that sets none:
	// strict (certificate verified against the system roots and the
	// hostname), insecure (TLS, any certificate) or off (plain TCP).
	IngressTLS string `yaml:"ingress_tls"`
	// IngressPorts: the port per protocol on an ingress host (imap, pop3,
	// jmap, submission; lmtp for the socketmap transport map).
	IngressPorts map[string]int `yaml:"ingress_ports"`
	// ConnectTimeout bounds one backend connection attempt; PreAuthTimeout
	// how long a client may take to log in; NodeTTL how long the node
	// registry listing is reused; DownFor how long a backend that refused
	// a connection is skipped.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	PreAuthTimeout time.Duration `yaml:"pre_auth_timeout"`
	NodeTTL        time.Duration `yaml:"node_ttl"`
	DownFor        time.Duration `yaml:"down_for"`
	// Auth: "local" (default) verifies passwords at the proxy against
	// the account's hashes and hands verified clients to the backend
	// with a token signed by HandoffKey, so a wrong password never
	// reaches a backend; "passthrough" replays every credential against
	// the backend untouched and needs no hashes at the proxy.
	Auth string `yaml:"auth"`
	// HandoffKey is this proxy site's private Ed25519 key (base64 seed,
	// from `kittiwake-admin proxy-key create|generate`); its public half
	// is registered with the admin API. Required with auth local.
	HandoffKey Secret `yaml:"handoff_key"`
}

// SMTP is the outbound relay.
type SMTP struct {
	RelayHost string `yaml:"relay_host"`
	Username  string `yaml:"username"`
	Password  Secret `yaml:"password"`
	// TLS: "off" (plain), "opportunistic" (STARTTLS when the relay offers
	// it, certificate not verified: encryption against passive listeners,
	// like Postfix "may") or "required" (STARTTLS must be offered and the
	// certificate must verify against the system roots and the relay's
	// host name, or tls_server_name). Default opportunistic.
	TLS string `yaml:"tls"`
	// TLSServerName: name to verify the relay certificate against when it
	// differs from the host in relay_host.
	TLSServerName string `yaml:"tls_server_name"`
	// TLSInsecureSkipVerify: with "required", still insist on STARTTLS but
	// accept any certificate (lab relays with self-signed certificates).
	TLSInsecureSkipVerify bool `yaml:"tls_insecure_skip_verify"`
	// XClient: open every relay session with Postfix XCLIENT, passing the
	// sending account's UUID as LOGIN (Postfix sasl_username, for policy
	// services and logs) and the real client address as ADDR, so the relay
	// can rate-limit per account. The relay must list Kittiwake in
	// smtpd_authorized_xclient_hosts; it then treats the session as
	// authenticated, so username/password are not used with it.
	XClient bool `yaml:"xclient"`
}

// Submission is the SMTP submission mode.
type Submission struct {
	// SaveToSent stores a copy of every message in the Sent folder. Off by
	// default: SMTP clients save their own copy over IMAP.
	SaveToSent bool `yaml:"save_to_sent"`
	// RequireTLSProxy offers AUTH only on connections whose PROXY v2 header
	// says the client spoke TLS to the proxy (HAProxy send-proxy-v2-ssl),
	// so a password never crosses a plain-text client connection. Default
	// true; turn off for development without a proxy.
	RequireTLSProxy bool `yaml:"require_tls_proxy"`
}

// Mail holds message handling policy.
type Mail struct {
	MaxMessageSize      int64 `yaml:"max_message_size"`
	InlineAttachmentMax int64 `yaml:"inline_attachment_max"`
	// KeepRaw: when to keep the original bytes next to the parsed document:
	// "never", "signed" (multipart/signed, so PGP/MIME and S/MIME verify) or
	// "always". Messages are otherwise rendered from the document.
	KeepRaw        string        `yaml:"keep_raw"`
	TrashRetention time.Duration `yaml:"trash_retention"`
	PostHoldGrace  time.Duration `yaml:"post_hold_grace"`
	SpamHeader     string        `yaml:"spam_header"`
	SpamValue      string        `yaml:"spam_value"`
	// OverQuotaTempFail makes LMTP answer 4xx instead of 5xx when a mailbox is full.
	OverQuotaTempFail bool `yaml:"over_quota_tempfail"`
	// MaxAliasDepth bounds alias chains.
	MaxAliasDepth int `yaml:"max_alias_depth"`
}

// Quota is the default quota applied to mailboxes without limits.
type Quota struct {
	SoftBytes    int64 `yaml:"soft_bytes"`
	HardBytes    int64 `yaml:"hard_bytes"`
	SoftMessages int64 `yaml:"soft_messages"`
	HardMessages int64 `yaml:"hard_messages"`
}

// Session configures OAuth sessions.
type Session struct {
	AccessTokenLifetime  time.Duration `yaml:"access_token_lifetime"`
	RefreshTokenLifetime time.Duration `yaml:"refresh_token_lifetime"`
	KeyRotationInterval  time.Duration `yaml:"key_rotation_interval"`
	// Issuer is the JWT "iss" claim and the JMAP session URL base, e.g. "https://mail.example.org".
	Issuer string `yaml:"issuer"`
}

// Cache configures the per-process read caches. Every entry is how long a
// value read from RADOS is reused before it is read again; 0 disables that
// cache. A change made by another process shows up within the TTL; a change
// made by the same process shows up at once. Keep these short (seconds):
// they exist to collapse the repeated reads of one client's command stream
// or one MTA's recipient lookups, not to hold data.
type Cache struct {
	// Credentials: verified passwords and API tokens. Without it every
	// basic-auth request costs an argon2id verification.
	Credentials time.Duration `yaml:"credentials"`
	// Identity: domains, mailboxes (state, hold, quota limits), accounts and
	// aliases, read on every IMAP/POP3/JMAP command, every LMTP recipient
	// and every Postfix socketmap query.
	Identity time.Duration `yaml:"identity"`
	// Root: the mailbox root (folder list, counters, modseq, quota usage)
	// where a slightly old copy is fine: quota checks, folder-name
	// resolution, JMAP state strings.
	Root time.Duration `yaml:"root"`
}

// Database is the metadata database (MariaDB): domains, mailboxes,
// accounts, aliases, tokens, sessions, the worker queue and the Ceph
// storage registry. Required with the librados backend; with the memory
// backend an empty DSN keeps metadata in memory as well.
type Database struct {
	MaxOpen      int           `yaml:"max_open_connections"`
	MaxIdle      int           `yaml:"max_idle_connections"`
	ConnLifetime time.Duration `yaml:"connection_lifetime"`
	// DSN in go-sql-driver/mysql form, e.g.
	// "kittiwake:secret@tcp(db.example:3306)/kittiwake"; the usual secret
	// forms (file:, ${ENV}) apply. Every write goes here.
	DSN Secret `yaml:"dsn"`
	// ReadDSN is an optional replica (typically a local MariaDB replicating
	// from the primary) for SELECT-only operations. Empty = primary only.
	ReadDSN Secret `yaml:"read_dsn"`
	// ProxyDSN is what the proxy mode connects with instead of dsn: the
	// read-only user with the grants README "Proxies" lists, so a proxy
	// process on a box that also runs backends never holds the
	// read-write credentials. Empty = dsn.
	ProxyDSN Secret `yaml:"proxy_dsn"`
	// ReadStickiness: after a write, how long this process keeps reading
	// from the primary so it sees its own changes despite replication lag.
	ReadStickiness time.Duration `yaml:"read_stickiness"`
}

// Node configures how this process registers itself in the node registry
// (every mode it runs, with the address others reach it on).
type Node struct {
	// Advertise is the host part of the registered addresses: an IPv6 or
	// IPv4 address or a hostname other nodes can reach. Empty = the first
	// global unicast address of this machine (IPv6 preferred), else the
	// hostname. Ports come from the listeners.
	Advertise string `yaml:"advertise"`
	// Heartbeat: how often the registration is refreshed; a node is dead
	// after three missed heartbeats.
	Heartbeat time.Duration `yaml:"heartbeat"`
}

// IMAP configures the IMAP adapter beyond its listener.
type IMAP struct {
	// CompatNamespace advertises a second personal namespace mapping onto
	// the same folders, for clients configured for a Dovecot or Courier
	// layout (typically prefix "INBOX." with delimiter "."). Off when the
	// prefix is empty. The prefix must end with the delimiter.
	CompatNamespace CompatNamespace `yaml:"compat_namespace"`
}

// CompatNamespace is an IMAP namespace alias.
type CompatNamespace struct {
	Prefix    string `yaml:"prefix"`
	Delimiter string `yaml:"delimiter"`
}

// Audit configures audit retention and detail.
type Audit struct {
	Retention time.Duration `yaml:"retention"`
	// Detail per protocol: "full" (default) or "session" (login/logout only).
	Detail map[string]string `yaml:"detail"`
}

// Backup configures mailbox backups: the target they are written to, the
// keys they are sealed with, and the schedule policy. Only the `backup`
// mode needs the target and the keys; every other mode only reads the
// schedule limits (the admin API validates settings against them).
// BackupTarget is where backups go and what seals them: the default for
// every namespace, or one namespace's own.
type BackupTarget struct {
	// Target: "s3", "filesystem" or "" (no target).
	Target string `yaml:"target"`
	S3     S3     `yaml:"s3"`
	// Directory of the filesystem target (lab, small installations).
	Directory string `yaml:"directory"`
	// Keys: the pre-shared keys (hex, 32 bytes each) by id; CurrentKey
	// seals new objects, every listed key opens old ones (rotation).
	Keys       []BackupKey `yaml:"keys"`
	CurrentKey int         `yaml:"current_key"`
}

type Backup struct {
	// The default target and keys (inline: backup.target, backup.s3,
	// backup.directory, backup.keys, backup.current_key). Empty target:
	// no default; the backup mode then needs at least one namespace
	// target.
	BackupTarget `yaml:",inline"`
	// Namespaces maps a namespace to its own target and key ring, so a
	// tenant's backups land in its own bucket under its own keys.
	// Namespaces not listed use the default.
	Namespaces map[string]BackupTarget `yaml:"namespaces"`
	// Prefix prepended to every key on the target.
	Prefix string `yaml:"prefix"`
	// Interval between runs of a mailbox without its own setting;
	// MinInterval / MaxInterval bound every per-domain and per-mailbox
	// setting (the floor protects the platform; lower it with care).
	Interval    time.Duration `yaml:"interval"`
	MinInterval time.Duration `yaml:"min_interval"`
	MaxInterval time.Duration `yaml:"max_interval"`
	// Retention of runs on the target; MaintainInterval between
	// verification passes of a mailbox; ExportExpiry of export archives.
	Retention        time.Duration `yaml:"retention"`
	MaintainInterval time.Duration `yaml:"maintain_interval"`
	ExportExpiry     time.Duration `yaml:"export_expiry"`
	// Lock asks the target to retain objects for the retention period
	// (S3 Object Lock, compliance mode; the bucket must support it).
	Lock bool `yaml:"lock"`
	// PackSize caps one pack object; BlobThreshold is the attachment size
	// from which payloads are deduplicated as separate blobs; SpoolDir
	// holds packs and exports while they are built.
	PackSize      int64  `yaml:"pack_size"`
	BlobThreshold int64  `yaml:"blob_threshold"`
	SpoolDir      string `yaml:"spool_dir"`
	// Concurrency: tasks one backup process runs at once; Batch: due
	// mailboxes the scheduler enqueues per tick.
	Concurrency int `yaml:"concurrency"`
	Batch       int `yaml:"batch"`
}

// validate checks a target; required demands one (namespace entries
// always name a target, the default may be absent).
func (t *BackupTarget) validate(path string, required bool) error {
	switch t.Target {
	case "", "s3", "filesystem":
	default:
		return fmt.Errorf("config: %s.target %q must be s3, filesystem or empty", path, t.Target)
	}
	if required && t.Target == "" {
		return fmt.Errorf("config: %s.target is required (s3 or filesystem)", path)
	}
	if t.Target == "s3" && (t.S3.Endpoint == "" || t.S3.Bucket == "") {
		return fmt.Errorf("config: %s.s3.endpoint and %s.s3.bucket are required", path, path)
	}
	if t.Target == "filesystem" && t.Directory == "" {
		return fmt.Errorf("config: %s.directory is required for the filesystem target", path)
	}
	if t.Target != "" {
		if len(t.Keys) == 0 {
			return fmt.Errorf("config: %s.keys needs at least one key (32 random bytes, hex) and %s.current_key names the one that seals new backups", path, path)
		}
		seen := map[int]bool{}
		for _, k := range t.Keys {
			if k.ID < 0 || k.ID > 65535 || seen[k.ID] {
				return fmt.Errorf("config: %s.keys: id %d invalid or repeated", path, k.ID)
			}
			seen[k.ID] = true
		}
		if !seen[t.CurrentKey] {
			return fmt.Errorf("config: %s.current_key %d is not in %s.keys", path, t.CurrentKey, path)
		}
	}
	return nil
}

// BackupKey is one ring entry.
type BackupKey struct {
	ID  int    `yaml:"id"`
	Key Secret `yaml:"key"`
}

// S3 is an S3-compatible endpoint.
type S3 struct {
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	AccessKey Secret `yaml:"access_key"`
	SecretKey Secret `yaml:"secret_key"`
	// PathStyle addresses the bucket in the path (MinIO, RGW, most
	// self-hosted stores); off uses virtual-host addressing (AWS).
	PathStyle bool `yaml:"path_style"`
}

// Log configures logging.
type Log struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json or text
}

// Secret is a string that may be given inline, as "file:/path" or with
// ${ENV} substitution. Resolve() returns the actual value.
type Secret string

var envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// namespaceRe is the shape of a namespace name (a DNS label).
var namespaceRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// Resolve expands ${ENV} references and file: prefixes.
func (s Secret) Resolve() (string, error) {
	v := envRe.ReplaceAllStringFunc(string(s), func(m string) string {
		return os.Getenv(m[2 : len(m)-1])
	})
	if strings.HasPrefix(v, "file:") {
		b, err := os.ReadFile(strings.TrimPrefix(v, "file:"))
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	return v, nil
}

// Default returns the configuration defaults.
func Default() Config {
	return Config{
		Ceph: Ceph{
			PlacementReserve: 0.15,
			Backend:          "librados",
			Pools: Pools{
				Messages:    "kittiwake.messages",
				Attachments: "kittiwake.attachments",
				Index:       "kittiwake.index",
				Meta:        "kittiwake.meta",
			},
		},
		Listen: Listen{
			// Per mode: the listener and the management (metrics/health)
			// address of the process running that mode on its own. One
			// process per mode is the normal deployment, so every mode has
			// its own management port; listen.management serves the
			// one-process form.
			LMTP:       Listener{Address: "[::]:24", ProxyProtocol: "optional", Management: "[::1]:9091"},
			IMAP:       Listener{Address: "[::]:143", ProxyProtocol: "optional", Management: "[::1]:9092"},
			POP3:       Listener{Address: "[::]:110", ProxyProtocol: "optional", Management: "[::1]:9093"},
			JMAP:       Listener{Address: "[::]:8080", ProxyProtocol: "optional", Management: "[::1]:9094"},
			Submission: Listener{Address: "[::]:587", ProxyProtocol: "optional", Management: "[::1]:9098"},
			Admin:      Listener{Address: "[::1]:8081", ProxyProtocol: "off", Management: "[::1]:9095"},
			Socketmap:  Listener{Address: "[::1]:2525", ProxyProtocol: "off", Management: "[::1]:9096"},
			Worker:     Listener{Management: "[::1]:9097"},
			Backup:     Listener{Management: "[::1]:9099"},
			Proxy: ProxyListen{
				IMAP: Listener{Address: "[::]:143", ProxyProtocol: "optional"}, POP3: Listener{Address: "[::]:110", ProxyProtocol: "optional"},
				JMAP: Listener{Address: "[::]:8080", ProxyProtocol: "optional"}, Submission: Listener{Address: "[::]:587", ProxyProtocol: "optional"},
				Management: "[::1]:9100",
			},
			Management: Listener{Address: "[::1]:9090", ProxyProtocol: "off"},
		},
		Proxy: Proxy{
			IngressTLS:     "strict",
			Auth:           "local",
			IngressPorts:   map[string]int{"imap": 993, "pop3": 995, "jmap": 443, "submission": 465, "lmtp": 24},
			ConnectTimeout: 10 * time.Second, PreAuthTimeout: time.Minute, NodeTTL: 5 * time.Second, DownFor: 15 * time.Second,
		},
		SMTP:       SMTP{RelayHost: "[::1]:25", TLS: "opportunistic"},
		Submission: Submission{RequireTLSProxy: true},
		Mail: Mail{
			MaxMessageSize:      50 << 20,
			InlineAttachmentMax: 4 << 10,
			KeepRaw:             "signed",
			TrashRetention:      30 * 24 * time.Hour,
			PostHoldGrace:       7 * 24 * time.Hour,
			SpamHeader:          "X-Spam-Flag",
			SpamValue:           "YES",
			MaxAliasDepth:       5,
		},
		Session: Session{
			AccessTokenLifetime:  15 * time.Minute,
			RefreshTokenLifetime: 30 * 24 * time.Hour,
			KeyRotationInterval:  90 * 24 * time.Hour,
			Issuer:               "kittiwake",
		},
		Cache:    Cache{Credentials: 5 * time.Second, Identity: 5 * time.Second, Root: 5 * time.Second},
		Database: Database{ReadStickiness: 2 * time.Second, MaxOpen: 32, MaxIdle: 8, ConnLifetime: 30 * time.Minute},
		Node:     Node{Heartbeat: 10 * time.Second},
		Audit:    Audit{Retention: 365 * 24 * time.Hour},
		Backup: Backup{
			Prefix: "kittiwake/", BackupTarget: BackupTarget{S3: S3{Region: "us-east-1", PathStyle: true}},
			Interval: 24 * time.Hour, MinInterval: 30 * time.Minute, MaxInterval: 30 * 24 * time.Hour,
			Retention: 90 * 24 * time.Hour, MaintainInterval: 7 * 24 * time.Hour, ExportExpiry: 7 * 24 * time.Hour,
			Lock: true, PackSize: 1 << 30, BlobThreshold: 1 << 20, Concurrency: 4, Batch: 100,
		},
		Log: Log{Level: "info", Format: "json"},
	}
}

// Load reads and validates the YAML file at path.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(b)
}

// Parse parses YAML bytes on top of the defaults and validates.
func Parse(b []byte) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ValidModes is the set of accepted mode names.
var ValidModes = map[string]bool{"lmtp": true, "imap": true, "pop3": true, "jmap": true, "submission": true, "admin": true, "socketmap": true, "worker": true, "backup": true, "proxy": true}

// AllModes lists every mode name.
var AllModes = []string{"lmtp", "imap", "pop3", "jmap", "submission", "admin", "socketmap", "worker", "backup", "proxy"}

// Validate checks the configuration for consistency.
func (c *Config) Validate() error {
	switch c.SMTP.TLS {
	case "off", "opportunistic", "required":
	default:
		return fmt.Errorf("config: smtp.tls must be off, opportunistic or required, got %q", c.SMTP.TLS)
	}
	if c.SMTP.XClient && c.SMTP.Username != "" {
		return errors.New("config: smtp.xclient and smtp.username exclude each other: with XCLIENT the relay authorises Kittiwake by address and the session carries the account")
	}
	for _, m := range c.Modes {
		if !ValidModes[m] {
			return fmt.Errorf("config: unknown mode %q", m)
		}
	}
	if c.HasMode("proxy") {
		if len(c.Modes) > 1 {
			return errors.New("config: the proxy mode runs in a process of its own (it holds no mail state and connects to the database read-only)")
		}
		if c.Database.DSN == "" && c.Database.ProxyDSN == "" {
			return errors.New("config: the proxy mode needs database.proxy_dsn or database.dsn (a read-only user suffices)")
		}
		if c.Proxy.Auth == "local" {
			if c.Proxy.HandoffKey == "" {
				return errors.New("config: proxy.auth local needs proxy.handoff_key (kittiwake-admin proxy-key create <site>), or set proxy.auth to passthrough")
			}
			if _, err := auth.ParseHandoffPrivate(string(c.Proxy.HandoffKey)); err != nil {
				return fmt.Errorf("config: proxy.handoff_key: %w", err)
			}
		}
	}
	switch c.Proxy.Auth {
	case "local", "passthrough":
	default:
		return fmt.Errorf("config: proxy.auth must be local or passthrough, got %q", c.Proxy.Auth)
	}
	switch c.Proxy.IngressTLS {
	case "strict", "insecure", "off":
	default:
		return fmt.Errorf("config: proxy.ingress_tls must be strict, insecure or off, got %q", c.Proxy.IngressTLS)
	}
	for name, port := range c.Proxy.IngressPorts {
		switch name {
		case "imap", "pop3", "jmap", "submission", "lmtp":
		default:
			return fmt.Errorf("config: proxy.ingress_ports: unknown protocol %q", name)
		}
		if port <= 0 || port > 65535 {
			return fmt.Errorf("config: proxy.ingress_ports.%s %d is not a port", name, port)
		}
	}
	if c.Proxy.ConnectTimeout <= 0 || c.Proxy.PreAuthTimeout <= 0 || c.Proxy.NodeTTL <= 0 || c.Proxy.DownFor <= 0 {
		return errors.New("config: proxy.connect_timeout, pre_auth_timeout, node_ttl and down_for must be positive")
	}
	for _, ns := range c.Namespaces {
		if !namespaceRe.MatchString(strings.ToLower(strings.TrimSpace(ns))) {
			return fmt.Errorf("config: namespaces: %q is not a valid namespace name (a DNS label)", ns)
		}
	}
	for name, l := range map[string]Listener{"lmtp": c.Listen.LMTP, "imap": c.Listen.IMAP, "pop3": c.Listen.POP3, "jmap": c.Listen.JMAP, "submission": c.Listen.Submission, "admin": c.Listen.Admin, "socketmap": c.Listen.Socketmap, "management": c.Listen.Management,
		"proxy.imap": c.Listen.Proxy.IMAP, "proxy.pop3": c.Listen.Proxy.POP3, "proxy.jmap": c.Listen.Proxy.JMAP, "proxy.submission": c.Listen.Proxy.Submission} {
		switch l.ProxyProtocol {
		case "", "off", "optional", "required":
		default:
			return fmt.Errorf("config: listen.%s.proxy_protocol must be off, optional or required", name)
		}
	}
	if math.IsNaN(c.Ceph.PlacementReserve) || c.Ceph.PlacementReserve < 0 || c.Ceph.PlacementReserve >= 1 {
		return errors.New("config: ceph.placement_reserve must be in [0,1)")
	}
	switch c.Ceph.Backend {
	case "librados":
		if len(c.Ceph.Monitors) > 0 || c.Ceph.User != "" || c.Ceph.Key != "" || c.Ceph.Keyring != "" {
			return errors.New("config: ceph.monitors/user/key/keyring are not used with librados: storage entries are registered in the database (kittiwake-admin storage add); only ceph.config_file and ceph.options apply to every connection")
		}
	case "memory":
		if c.Ceph.Pools.Messages == "" || c.Ceph.Pools.Attachments == "" || c.Ceph.Pools.Index == "" {
			return errors.New("config: ceph pools messages, attachments and index must be named")
		}
	default:
		return fmt.Errorf("config: ceph.backend %q must be librados or memory", c.Ceph.Backend)
	}
	if c.Ceph.Backend == "librados" && c.Database.DSN == "" && !c.HasMode("proxy") {
		return errors.New("config: database.dsn is required with the librados backend (metadata lives in MariaDB)")
	}
	if c.Database.ReadDSN != "" && c.Database.DSN == "" {
		return errors.New("config: database.read_dsn needs database.dsn")
	}
	if p := c.IMAP.CompatNamespace.Prefix; p != "" {
		d := c.IMAP.CompatNamespace.Delimiter
		if len(d) != 1 || d == "\\" {
			return errors.New("config: imap.compat_namespace.delimiter must be one character")
		}
		if !strings.HasSuffix(p, d) {
			return fmt.Errorf("config: imap.compat_namespace.prefix %q must end with the delimiter %q", p, d)
		}
	}
	if c.Node.Heartbeat <= 0 {
		return errors.New("config: node.heartbeat must be positive")
	}
	if c.Database.MaxOpen < 1 || c.Database.MaxIdle < 0 || c.Database.MaxIdle > c.Database.MaxOpen || c.Database.ConnLifetime < 0 {
		return fmt.Errorf("invalid database connection pool limits")
	}
	if c.Database.ReadStickiness < 0 {
		return errors.New("config: database.read_stickiness must not be negative")
	}
	if c.Database.DSN == "" && c.Ceph.Pools.Meta == "" {
		return errors.New("config: ceph.pools.meta must be named when no database is configured")
	}
	if c.Mail.MaxMessageSize <= 0 {
		return errors.New("config: mail.max_message_size must be positive")
	}
	switch c.Mail.KeepRaw {
	case "never", "signed", "always":
	default:
		return fmt.Errorf("config: mail.keep_raw %q must be never, signed or always", c.Mail.KeepRaw)
	}
	if c.Mail.MaxAliasDepth <= 0 {
		return errors.New("config: mail.max_alias_depth must be positive")
	}
	if c.Cache.Credentials < 0 || c.Cache.Identity < 0 || c.Cache.Root < 0 {
		return errors.New("config: cache TTLs must not be negative")
	}
	if c.Session.AccessTokenLifetime <= 0 || c.Session.RefreshTokenLifetime <= 0 {
		return errors.New("config: session lifetimes must be positive")
	}
	for p, d := range c.Audit.Detail {
		if d != "full" && d != "session" {
			return fmt.Errorf("config: audit.detail.%s must be full or session", p)
		}
	}
	if err := c.Backup.validate(c.HasMode("backup")); err != nil {
		return err
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: log.level %q invalid", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fmt.Errorf("config: log.format %q invalid", c.Log.Format)
	}
	return nil
}

// HasMode reports whether mode is enabled.
func (c *Config) HasMode(mode string) bool {
	for _, m := range c.Modes {
		if m == mode {
			return true
		}
	}
	return false
}

// ServedNamespaces returns the normalized served-namespace list; nil
// means all.
func (c *Config) ServedNamespaces() []string {
	if len(c.Namespaces) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, ns := range c.Namespaces {
		ns = strings.ToLower(strings.TrimSpace(ns))
		if ns != "" && !seen[ns] {
			seen[ns] = true
			out = append(out, ns)
		}
	}
	return out
}

// validate checks the backup section; a process running the backup mode
// needs a target and keys, every other process only sane limits.
func (b *Backup) validate(backupMode bool) error {
	if err := b.BackupTarget.validate("backup", false); err != nil {
		return err
	}
	if backupMode && b.Target == "" && len(b.Namespaces) == 0 {
		return errors.New("config: the backup mode needs backup.target (s3 or filesystem) or at least one backup.namespaces entry")
	}
	for ns, t := range b.Namespaces {
		if !namespaceRe.MatchString(ns) {
			return fmt.Errorf("config: backup.namespaces: %q is not a valid namespace name", ns)
		}
		if err := t.validate("backup.namespaces."+ns, true); err != nil {
			return err
		}
	}
	if b.MinInterval < time.Minute || b.MinInterval > b.Interval || b.Interval > b.MaxInterval {
		return errors.New("config: backup intervals must satisfy 1m <= min_interval <= interval <= max_interval")
	}
	if b.Retention <= 0 || b.MaintainInterval <= 0 || b.ExportExpiry <= 0 || b.PackSize <= 0 || b.BlobThreshold < 0 || b.Concurrency <= 0 || b.Batch <= 0 {
		return errors.New("config: backup retention, maintain_interval, export_expiry, pack_size, concurrency and batch must be positive")
	}
	return nil
}
