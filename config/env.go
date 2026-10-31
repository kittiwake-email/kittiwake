package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Environment overrides (README "Containers"): a set KITTIWAKE_* variable
// replaces the corresponding value of the file, so a container image
// ships one config file and takes everything deployment-specific from
// its environment. An empty variable is ignored (an image may export
// KITTIWAKE_MODES= to leave the mode to the run command). Lists are
// comma-separated; booleans are true/false. Load applies them, Parse
// (tests, check-config on bytes) does not.

// EnvVar is one override.
type EnvVar struct {
	Name string
	Doc  string
	set  func(*Config, string) error
}

// EnvVars lists every override, for `kittiwake help env`.
var EnvVars = []EnvVar{
	{"KITTIWAKE_MODES", "modes this process runs (comma-separated), as --modes", func(c *Config, v string) error { c.Modes = list(v); return nil }},
	{"KITTIWAKE_NAMESPACES", "namespaces this process serves (comma-separated; default all)", func(c *Config, v string) error { c.Namespaces = list(v); return nil }},
	{"KITTIWAKE_MANAGEMENT", "management (metrics, health) address of this process, as --management", func(c *Config, v string) error { c.setManagement(v); return nil }},
	{"KITTIWAKE_DB_DSN", "database.dsn", func(c *Config, v string) error { c.Database.DSN = Secret(v); return nil }},
	{"KITTIWAKE_DB_READ_DSN", "database.read_dsn (a replica for reads)", func(c *Config, v string) error { c.Database.ReadDSN = Secret(v); return nil }},
	{"KITTIWAKE_DB_PROXY_DSN", "database.proxy_dsn (the proxy mode's read-only user)", func(c *Config, v string) error { c.Database.ProxyDSN = Secret(v); return nil }},
	{"KITTIWAKE_CEPH_BACKEND", "ceph.backend: librados or memory", func(c *Config, v string) error { c.Ceph.Backend = v; return nil }},
	{"KITTIWAKE_CEPH_CONFIG_FILE", "ceph.config_file: librados defaults for every storage connection", func(c *Config, v string) error { c.Ceph.ConfigFile = v; return nil }},
	{"KITTIWAKE_NODE_ADVERTISE", "node.advertise: the address other nodes reach this one at", func(c *Config, v string) error { c.Node.Advertise = v; return nil }},
	{"KITTIWAKE_PROXY_PROTOCOL", "proxy_protocol of every client-facing listener: off, optional or required", func(c *Config, v string) error { c.setProxyProtocol(v); return nil }},
	{"KITTIWAKE_PROXY_TRUSTED_ADDRESSES", "proxy.trusted_addresses (comma-separated addresses or CIDRs)", func(c *Config, v string) error { c.Proxy.TrustedAddresses = list(v); return nil }},
	{"KITTIWAKE_PROXY_AUTH", "proxy.auth: local or passthrough", func(c *Config, v string) error { c.Proxy.Auth = v; return nil }},
	{"KITTIWAKE_PROXY_HANDOFF_KEY", "proxy.handoff_key: the proxy site's private key", func(c *Config, v string) error { c.Proxy.HandoffKey = Secret(v); return nil }},
	{"KITTIWAKE_PROXY_INGRESS_TLS", "proxy.ingress_tls: strict, insecure or off", func(c *Config, v string) error { c.Proxy.IngressTLS = v; return nil }},
	{"KITTIWAKE_SMTP_RELAY_HOST", "smtp.relay_host", func(c *Config, v string) error { c.SMTP.RelayHost = v; return nil }},
	{"KITTIWAKE_SMTP_TLS", "smtp.tls: off, opportunistic or required", func(c *Config, v string) error { c.SMTP.TLS = v; return nil }},
	{"KITTIWAKE_SMTP_TLS_SERVER_NAME", "smtp.tls_server_name", func(c *Config, v string) error { c.SMTP.TLSServerName = v; return nil }},
	{"KITTIWAKE_SMTP_USERNAME", "smtp.username", func(c *Config, v string) error { c.SMTP.Username = v; return nil }},
	{"KITTIWAKE_SMTP_PASSWORD", "smtp.password", func(c *Config, v string) error { c.SMTP.Password = Secret(v); return nil }},
	{"KITTIWAKE_SMTP_XCLIENT", "smtp.xclient (true/false)", boolSet(func(c *Config, b bool) { c.SMTP.XClient = b })},
	{"KITTIWAKE_SUBMISSION_REQUIRE_TLS_PROXY", "submission.require_tls_proxy (true/false)", boolSet(func(c *Config, b bool) { c.Submission.RequireTLSProxy = b })},
	{"KITTIWAKE_SESSION_ISSUER", "session.issuer: the public base URL", func(c *Config, v string) error { c.Session.Issuer = v; return nil }},
	{"KITTIWAKE_BACKUP_TARGET", "backup.target: s3, filesystem or empty", func(c *Config, v string) error { c.Backup.Target = v; return nil }},
	{"KITTIWAKE_BACKUP_S3_ENDPOINT", "backup.s3.endpoint", func(c *Config, v string) error { c.Backup.S3.Endpoint = v; return nil }},
	{"KITTIWAKE_BACKUP_S3_REGION", "backup.s3.region", func(c *Config, v string) error { c.Backup.S3.Region = v; return nil }},
	{"KITTIWAKE_BACKUP_S3_BUCKET", "backup.s3.bucket", func(c *Config, v string) error { c.Backup.S3.Bucket = v; return nil }},
	{"KITTIWAKE_BACKUP_S3_ACCESS_KEY", "backup.s3.access_key", func(c *Config, v string) error { c.Backup.S3.AccessKey = Secret(v); return nil }},
	{"KITTIWAKE_BACKUP_S3_SECRET_KEY", "backup.s3.secret_key", func(c *Config, v string) error { c.Backup.S3.SecretKey = Secret(v); return nil }},
	{"KITTIWAKE_BACKUP_S3_PATH_STYLE", "backup.s3.path_style (true/false)", boolSet(func(c *Config, b bool) { c.Backup.S3.PathStyle = b })},
	{"KITTIWAKE_BACKUP_DIRECTORY", "backup.directory (the filesystem target)", func(c *Config, v string) error { c.Backup.Directory = v; return nil }},
	{"KITTIWAKE_BACKUP_KEY", "backup.keys: one key (hex) with id 1, the current key; use the file for a ring", func(c *Config, v string) error {
		c.Backup.Keys = []BackupKey{{ID: 1, Key: Secret(v)}}
		c.Backup.CurrentKey = 1
		return nil
	}},
	{"KITTIWAKE_BACKUP_SPOOL_DIR", "backup.spool_dir", func(c *Config, v string) error { c.Backup.SpoolDir = v; return nil }},
	{"KITTIWAKE_LOG_LEVEL", "log.level: debug, info, warn or error", func(c *Config, v string) error { c.Log.Level = v; return nil }},
	{"KITTIWAKE_LOG_FORMAT", "log.format: json or text", func(c *Config, v string) error { c.Log.Format = v; return nil }},
}

// ApplyEnv applies the set, non-empty overrides lookup returns.
func (c *Config) ApplyEnv(lookup func(string) (string, bool)) error {
	for _, e := range EnvVars {
		v, ok := lookup(e.Name)
		if !ok || strings.TrimSpace(v) == "" {
			continue
		}
		if err := e.set(c, strings.TrimSpace(v)); err != nil {
			return fmt.Errorf("config: %s: %w", e.Name, err)
		}
	}
	return nil
}

// EnvOverrides reports which overrides are set in the process
// environment, sorted, for logs and check-config.
func EnvOverrides() []string {
	var out []string
	for _, e := range EnvVars {
		if v, ok := os.LookupEnv(e.Name); ok && strings.TrimSpace(v) != "" {
			out = append(out, e.Name)
		}
	}
	sort.Strings(out)
	return out
}

func list(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func boolSet(f func(*Config, bool)) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%q is not true or false", v)
		}
		f(c, b)
		return nil
	}
}

// setManagement makes addr the management address of this process
// whatever modes it runs: the per-mode addresses are cleared so
// ManagementAddress falls back to listen.management.
func (c *Config) setManagement(addr string) {
	c.Listen.Management.Address = addr
	for _, l := range []*Listener{&c.Listen.LMTP, &c.Listen.IMAP, &c.Listen.POP3, &c.Listen.JMAP, &c.Listen.Submission, &c.Listen.Admin, &c.Listen.Socketmap, &c.Listen.Worker, &c.Listen.Backup} {
		l.Management = ""
	}
	c.Listen.Proxy.Management = ""
}

// setProxyProtocol sets the PROXY protocol mode of every listener that
// clients (through a TLS terminator) reach: the mail protocols and the
// proxy mode's front ends. Admin and socketmap stay as configured.
func (c *Config) setProxyProtocol(mode string) {
	for _, l := range []*Listener{&c.Listen.LMTP, &c.Listen.IMAP, &c.Listen.POP3, &c.Listen.JMAP, &c.Listen.Submission,
		&c.Listen.Proxy.IMAP, &c.Listen.Proxy.POP3, &c.Listen.Proxy.JMAP, &c.Listen.Proxy.Submission} {
		l.ProxyProtocol = mode
	}
}
