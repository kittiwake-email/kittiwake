// Package config loads Dovetail's single YAML configuration file.
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
	// Modes to run in this process: lmtp, imap, pop3, jmap, admin, postfix, worker.
	Modes []string `yaml:"modes"`

	Ceph    Ceph    `yaml:"ceph"`
	Listen  Listen  `yaml:"listen"`
	Proxy   Proxy   `yaml:"proxy"`
	SMTP    SMTP    `yaml:"smtp"`
	Mail    Mail    `yaml:"mail"`
	Quota   Quota   `yaml:"quota"`
	Session Session `yaml:"session"`
	Audit   Audit   `yaml:"audit"`
	Export  Export  `yaml:"export"`
	Log     Log     `yaml:"log"`
}

// Ceph is the cluster connection and pool layout.
type Ceph struct {
	// Backend is "librados" (default) or "memory" (in-process, volatile;
	// for development and tests only).
	Backend    string            `yaml:"backend"`
	ConfigFile string            `yaml:"config_file"`
	Monitors   []string          `yaml:"monitors"`
	User       string            `yaml:"user"`
	Key        Secret            `yaml:"key"`
	Keyring    string            `yaml:"keyring"`
	Options    map[string]string `yaml:"options"`
	Pools      Pools             `yaml:"pools"`
}

// Pools names the four RADOS pools.
type Pools struct {
	Messages    string `yaml:"messages"`
	Attachments string `yaml:"attachments"`
	Index       string `yaml:"index"`
	Meta        string `yaml:"meta"`
}

// Listen holds one listener per mode plus the management listener.
type Listen struct {
	LMTP       Listener `yaml:"lmtp"`
	IMAP       Listener `yaml:"imap"`
	POP3       Listener `yaml:"pop3"`
	JMAP       Listener `yaml:"jmap"`  // JMAP, OAuth token and self-service endpoints
	Admin      Listener `yaml:"admin"` // admin REST API, operators only
	Postfix    Listener `yaml:"postfix"`
	Management Listener `yaml:"management"`
}

// Listener is one network listener.
type Listener struct {
	// Address in Go net form, e.g. "[::]:24" or "/run/dovetail/lmtp.sock".
	Address string `yaml:"address"`
	// ProxyProtocol: "off", "optional" (accept with or without header) or
	// "required" (reject connections without a PROXY header).
	ProxyProtocol string `yaml:"proxy_protocol"`
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
	StartTLS  bool   `yaml:"starttls"`
}

// Mail holds message handling policy.
type Mail struct {
	MaxMessageSize      int64         `yaml:"max_message_size"`
	InlineAttachmentMax int64         `yaml:"inline_attachment_max"`
	ForceFullRaw        bool          `yaml:"force_full_raw"`
	TrashRetention      time.Duration `yaml:"trash_retention"`
	PostHoldGrace       time.Duration `yaml:"post_hold_grace"`
	SpamHeader          string        `yaml:"spam_header"`
	SpamValue           string        `yaml:"spam_value"`
	// OverQuotaTempFail / DisabledTempFail make LMTP answer 4xx instead of 5xx.
	OverQuotaTempFail bool `yaml:"over_quota_tempfail"`
	DisabledTempFail  bool `yaml:"disabled_tempfail"`
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
			User:    "dovetail",
			Pools: Pools{
				Messages:    "dovetail.messages",
				Attachments: "dovetail.attachments",
				Index:       "dovetail.index",
				Meta:        "dovetail.meta",
			},
		},
		Listen: Listen{
			LMTP:       Listener{Address: "[::]:24", ProxyProtocol: "optional"},
			IMAP:       Listener{Address: "[::]:143", ProxyProtocol: "optional"},
			POP3:       Listener{Address: "[::]:110", ProxyProtocol: "optional"},
			JMAP:       Listener{Address: "[::]:8080", ProxyProtocol: "optional"},
			Admin:      Listener{Address: "[::1]:8081", ProxyProtocol: "off"},
			Postfix:    Listener{Address: "[::1]:2525", ProxyProtocol: "off"},
			Management: Listener{Address: "[::1]:9090", ProxyProtocol: "off"},
		},
		SMTP: SMTP{RelayHost: "[::1]:25"},
		Mail: Mail{
			MaxMessageSize:      50 << 20,
			InlineAttachmentMax: 4 << 10,
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
			Issuer:               "dovetail",
		},
		Audit: Audit{Retention: 365 * 24 * time.Hour},
		Log:   Log{Level: "info", Format: "json"},
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
var ValidModes = map[string]bool{"lmtp": true, "imap": true, "pop3": true, "jmap": true, "admin": true, "postfix": true, "worker": true}

// Validate checks the configuration for consistency.
func (c *Config) Validate() error {
	for _, m := range c.Modes {
		if !ValidModes[m] {
			return fmt.Errorf("config: unknown mode %q", m)
		}
	}
	for name, l := range map[string]Listener{"lmtp": c.Listen.LMTP, "imap": c.Listen.IMAP, "pop3": c.Listen.POP3, "jmap": c.Listen.JMAP, "admin": c.Listen.Admin, "postfix": c.Listen.Postfix, "management": c.Listen.Management} {
		switch l.ProxyProtocol {
		case "", "off", "optional", "required":
		default:
			return fmt.Errorf("config: listen.%s.proxy_protocol must be off, optional or required", name)
		}
	}
	switch c.Ceph.Backend {
	case "librados", "memory":
	default:
		return fmt.Errorf("config: ceph.backend %q must be librados or memory", c.Ceph.Backend)
	}
	if c.Ceph.Pools.Messages == "" || c.Ceph.Pools.Attachments == "" || c.Ceph.Pools.Index == "" || c.Ceph.Pools.Meta == "" {
		return errors.New("config: all four ceph pools must be named")
	}
	if c.Mail.MaxMessageSize <= 0 {
		return errors.New("config: mail.max_message_size must be positive")
	}
	if c.Mail.MaxAliasDepth <= 0 {
		return errors.New("config: mail.max_alias_depth must be positive")
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
