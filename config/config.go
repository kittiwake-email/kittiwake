// Package config loads Kittiwake's single YAML configuration file.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole configuration file.
type Config struct {
	// Modes to run in this process: lmtp, imap, pop3, jmap, admin,
	// socketmap, worker. Optional: a systemd instance passes its mode with --modes
	// instead; the list is for running several modes in one process.
	Modes []string `yaml:"modes"`

	Ceph     Ceph     `yaml:"ceph"`
	Listen   Listen   `yaml:"listen"`
	Proxy    Proxy    `yaml:"proxy"`
	SMTP     SMTP     `yaml:"smtp"`
	Mail     Mail     `yaml:"mail"`
	Quota    Quota    `yaml:"quota"`
	Database Database `yaml:"database"`
	Node     Node     `yaml:"node"`
	IMAP     IMAP     `yaml:"imap"`
	Session  Session  `yaml:"session"`
	Cache    Cache    `yaml:"cache"`
	Audit    Audit    `yaml:"audit"`
	Export   Export   `yaml:"export"`
	Log      Log      `yaml:"log"`
}

// Ceph is the storage connection and pool layout.
type Ceph struct {
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
	LMTP      Listener `yaml:"lmtp"`
	IMAP      Listener `yaml:"imap"`
	POP3      Listener `yaml:"pop3"`
	JMAP      Listener `yaml:"jmap"`      // JMAP, OAuth token and self-service endpoints
	Admin     Listener `yaml:"admin"`     // admin REST API, operators only
	Socketmap Listener `yaml:"socketmap"` // MTA recipient/domain/alias lookups (Postfix socketmap protocol)
	Worker    Listener `yaml:"worker"`    // no listener of its own; only its management address
	// Management is the metrics/health listener of a process that runs
	// several modes, or of a mode whose entry has no management address.
	Management Listener `yaml:"management"`
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
	case "admin":
		return l.Admin
	case "socketmap":
		return l.Socketmap
	case "worker":
		return l.Worker
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

// Proxy lists trusted proxies for X-Forwarded-For fallback on HTTP.
type Proxy struct {
	TrustedAddresses []string `yaml:"trusted_addresses"`
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
	// DSN in go-sql-driver/mysql form, e.g.
	// "kittiwake:secret@tcp(db.example:3306)/kittiwake"; the usual secret
	// forms (file:, ${ENV}) apply. Every write goes here.
	DSN Secret `yaml:"dsn"`
	// ReadDSN is an optional replica (typically a local MariaDB replicating
	// from the primary) for SELECT-only operations. Empty = primary only.
	ReadDSN Secret `yaml:"read_dsn"`
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

// Export configures worker-run exports.
type Export struct {
	Directory string `yaml:"directory"`
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
			Backend: "librados",
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
			Admin:      Listener{Address: "[::1]:8081", ProxyProtocol: "off", Management: "[::1]:9095"},
			Socketmap:  Listener{Address: "[::1]:2525", ProxyProtocol: "off", Management: "[::1]:9096"},
			Worker:     Listener{Management: "[::1]:9097"},
			Management: Listener{Address: "[::1]:9090", ProxyProtocol: "off"},
		},
		SMTP: SMTP{RelayHost: "[::1]:25", TLS: "opportunistic"},
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
		Database: Database{ReadStickiness: 2 * time.Second},
		Node:     Node{Heartbeat: 10 * time.Second},
		Audit:    Audit{Retention: 365 * 24 * time.Hour},
		Log:      Log{Level: "info", Format: "json"},
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
var ValidModes = map[string]bool{"lmtp": true, "imap": true, "pop3": true, "jmap": true, "admin": true, "socketmap": true, "worker": true}

// AllModes lists every mode name.
var AllModes = []string{"lmtp", "imap", "pop3", "jmap", "admin", "socketmap", "worker"}

// Validate checks the configuration for consistency.
func (c *Config) Validate() error {
	switch c.SMTP.TLS {
	case "off", "opportunistic", "required":
	default:
		return fmt.Errorf("config: smtp.tls must be off, opportunistic or required, got %q", c.SMTP.TLS)
	}
	for _, m := range c.Modes {
		if !ValidModes[m] {
			return fmt.Errorf("config: unknown mode %q", m)
		}
	}
	for name, l := range map[string]Listener{"lmtp": c.Listen.LMTP, "imap": c.Listen.IMAP, "pop3": c.Listen.POP3, "jmap": c.Listen.JMAP, "admin": c.Listen.Admin, "socketmap": c.Listen.Socketmap, "management": c.Listen.Management} {
		switch l.ProxyProtocol {
		case "", "off", "optional", "required":
		default:
			return fmt.Errorf("config: listen.%s.proxy_protocol must be off, optional or required", name)
		}
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
	if c.Ceph.Backend == "librados" && c.Database.DSN == "" {
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
