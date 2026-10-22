// Package app wires configuration, the RADOS client, stores, services and
// protocol listeners into a running Kittiwake process. cmd/kittiwake is a
// thin command-line layer over it.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"kittiwake/config"
	"kittiwake/metrics"
	"kittiwake/proto/httpx"
	"kittiwake/proto/lmtp"
	"kittiwake/proto/proxyproto"
	"kittiwake/proto/socketmap"
	"kittiwake/rados"
	"kittiwake/service/admin"
	"kittiwake/service/auditor"
	authsvc "kittiwake/service/auth"
	"kittiwake/service/delivery"
	"kittiwake/service/export"
	"kittiwake/service/mailbox"
	"kittiwake/service/submit"
	"kittiwake/store/attach"
	"kittiwake/store/audit"
	"kittiwake/store/cluster"
	"kittiwake/store/identity"
	"kittiwake/store/index"
	"kittiwake/store/mariadb"
	"kittiwake/store/message"
	"kittiwake/store/queue"
	"kittiwake/store/session"
	"kittiwake/worker"
)

// Version is set at build time.
var Version = "dev"

// ModeFunc starts a mode. It must mark the mode handled via MarkHandled and
// register closers with AddCloser.
type ModeFunc func(ctx context.Context, a *App, start func(name string, f func() error)) error

// MarkHandled records that a mode was started by an ExtraModes func.
func (a *App) MarkHandled(mode string) {
	a.mu.Lock()
	if a.handled == nil {
		a.handled = map[string]bool{}
	}
	a.handled[mode] = true
	a.mu.Unlock()
}

// AddCloser registers a shutdown function.
func (a *App) AddCloser(f func(context.Context) error) { a.addCloser(f) }

// Listen opens a listener with the configured PROXY protocol mode.
func Listen(l config.Listener) (net.Listener, error) { return listen(l) }

// Stores bundles every store.
type Stores struct {
	Identity    *identity.Store
	Index       *index.Store
	Messages    *message.Store
	Attachments *attach.Store
	Audit       *audit.Store
	Sessions    session.Store
	Queue       queue.Store
}

// Services bundles every service.
type Services struct {
	Auditor  *auditor.Auditor
	Auth     *authsvc.Service
	Delivery *delivery.Service
	Mailbox  *mailbox.Service
	Admin    *admin.Service
	Submit   *submit.Service
	Export   *export.Service
}

// App is a configured Kittiwake process.
type App struct {
	Cfg      config.Config
	Log      *slog.Logger
	Client   rados.Client      // the memory backend's cluster; nil with librados
	DB       *mariadb.DB       // nil with the memory backend and no DSN
	Clusters *cluster.Registry // nil with the memory backend
	Resolver cluster.Resolver
	Stores   Stores
	Services Services
	// JMAPHandler and AdminHandler are mounted by the jmap and admin modes;
	// set by cmd (keeps app free of the protocol packages' construction).
	JMAPHandler  http.Handler
	AdminHandler http.Handler
	// ExtraModes lets cmd register protocol servers (IMAP, POP3) without
	// app importing them. Each is called from Run with the start helper.
	ExtraModes []ModeFunc
	handled    map[string]bool

	mu      sync.Mutex
	closers []func(context.Context) error
}

// NewLogger builds the process logger from config.
func NewLogger(cfg config.Log) *slog.Logger {
	var lvl slog.Level
	switch cfg.Level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

// Connect opens the single in-memory cluster of the memory backend. With
// librados there is no client from the config: clusters come from the
// registry in the database and are connected by New.
func Connect(cfg config.Ceph) (rados.Client, error) {
	if cfg.Backend == "memory" {
		return rados.NewMemoryClient(), nil
	}
	return nil, nil
}

// Connector returns the function that opens a registered cluster with
// librados, applying the config's ceph.config_file and ceph.options to
// every connection.
func Connector(cfg config.Ceph) cluster.Connector {
	return func(c identity.Cluster) (rados.Client, error) {
		// An unreachable cluster must fail fast, not hang a reload for the
		// librados default of five minutes; ceph.options can override.
		opts := map[string]string{"client_mount_timeout": "10", "rados_mon_op_timeout": "10"}
		for k, v := range cfg.Options {
			opts[k] = v
		}
		return rados.Connect(rados.Config{ConfigFile: cfg.ConfigFile, Monitors: c.MonHost, User: c.User, Key: c.Key, Options: opts})
	}
}

// OpenDatabase opens the metadata database when one is configured and
// checks that its schema is current. It returns nil, nil when the config
// has no DSN (memory backend: metadata stays in the RADOS memory client).
func OpenDatabase(ctx context.Context, cfg config.Database) (*mariadb.DB, error) {
	dsn, err := cfg.DSN.Resolve()
	if err != nil {
		return nil, fmt.Errorf("database dsn: %w", err)
	}
	if dsn == "" {
		return nil, nil
	}
	readDSN, err := cfg.ReadDSN.Resolve()
	if err != nil {
		return nil, fmt.Errorf("database read_dsn: %w", err)
	}
	db, err := mariadb.OpenSplit(dsn, readDSN)
	if err != nil {
		return nil, err
	}
	if cfg.ReadStickiness > 0 {
		db.ReadStickiness = cfg.ReadStickiness
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("database: %w", err)
	}
	have, err := db.SchemaVersion(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("database: %w", err)
	}
	if want := mariadb.LatestVersion(); have != want {
		db.Close()
		return nil, fmt.Errorf("database: schema version %d, this binary needs %d: run `kittiwake migrate`", have, want)
	}
	return db, nil
}

// New builds stores and services. client is the memory backend's single
// cluster (nil with librados, where the cluster registry connects); db is
// the metadata database (nil only with the memory backend).
func New(cfg config.Config, client rados.Client, db *mariadb.DB, log *slog.Logger) (*App, error) {
	pools := cfg.Ceph.Pools
	memory := cfg.Ceph.Backend == "memory"
	if db == nil && !memory {
		return nil, errors.New("app: a metadata database is required with the librados backend")
	}
	if client == nil && memory {
		return nil, errors.New("app: the memory backend needs a memory client")
	}
	a := &App{Cfg: cfg, Log: log, Client: client, DB: db}
	st := Stores{}
	var resolver cluster.Resolver
	var adminLog audit.AdminLog
	if db != nil {
		st.Identity, st.Sessions, st.Queue = identity.NewWith(db), db, db
		adminLog = db
	} else {
		meta, err := client.Pool(pools.Meta)
		if err != nil {
			return nil, fmt.Errorf("pool %s: %w", pools.Meta, err)
		}
		idxp, err := client.Pool(pools.Index)
		if err != nil {
			return nil, fmt.Errorf("pool %s: %w", pools.Index, err)
		}
		st.Identity, st.Sessions, st.Queue = identity.New(meta), session.New(idxp), queue.New(idxp)
		adminLog = audit.RadosAdminLog(idxp)
	}
	if memory {
		fixed, err := cluster.NewFixed(client, pools.Index, pools.Messages, pools.Attachments)
		if err != nil {
			return nil, err
		}
		resolver = fixed
	} else {
		reg := cluster.NewRegistry(st.Identity, Connector(cfg.Ceph), log)
		if err := reg.Reload(context.Background()); err != nil {
			return nil, fmt.Errorf("cluster registry: %w", err)
		}
		if len(reg.Statuses()) == 0 {
			log.Warn("no Ceph clusters registered; mailboxes cannot be placed or served until one is added (kittiwake-admin cluster add)")
		}
		a.Clusters = reg
		resolver = reg
	}
	a.Resolver = resolver
	st.Index, st.Messages, st.Attachments, st.Audit = index.NewMulti(resolver), message.NewMulti(resolver), attach.NewMulti(resolver), audit.NewMulti(resolver, adminLog)
	// LMTP defers at RCPT when the recipient's cluster is not connected
	// (no I/O: the registry and the cached mailbox record).
	var clusterAvailable func(context.Context, uuid.UUID) error
	if a.Clusters != nil {
		clusterAvailable = func(ctx context.Context, mb uuid.UUID) error {
			_, _, err := a.Clusters.Pool(ctx, mb, cluster.RoleIndex)
			return err
		}
	}
	st.Identity.CacheTTL = cfg.Cache.Identity
	aud := auditor.New(st.Audit, log, cfg.Audit.Detail)
	dq := identity.QuotaLimits{SoftBytes: cfg.Quota.SoftBytes, HardBytes: cfg.Quota.HardBytes, SoftMessages: cfg.Quota.SoftMessages, HardMessages: cfg.Quota.HardMessages}
	sv := Services{
		Auditor: aud,
		Auth: authsvc.New(st.Identity, st.Sessions, aud, authsvc.Config{
			AccessTokenLifetime: cfg.Session.AccessTokenLifetime, RefreshTokenLifetime: cfg.Session.RefreshTokenLifetime, Issuer: cfg.Session.Issuer,
			CacheTTL: cfg.Cache.Credentials,
		}),
		Delivery: delivery.New(st.Identity, st.Index, st.Messages, st.Attachments, st.Queue, aud, log, delivery.Config{
			MaxMessageSize: cfg.Mail.MaxMessageSize, InlineMax: cfg.Mail.InlineAttachmentMax, KeepRaw: cfg.Mail.KeepRaw,
			SpamHeader: cfg.Mail.SpamHeader, SpamValue: cfg.Mail.SpamValue, MaxAliasDepth: cfg.Mail.MaxAliasDepth,
			DefaultQuota: dq, OverQuotaTempFail: cfg.Mail.OverQuotaTempFail,
			RootTTL: cfg.Cache.Root, ClusterAvailable: clusterAvailable,
		}),
		Mailbox: mailbox.New(st.Identity, st.Index, st.Messages, st.Attachments, st.Queue, aud, log, mailbox.Config{TrashRetention: cfg.Mail.TrashRetention, DefaultQuota: dq, RootTTL: cfg.Cache.Root}),
		Admin:   admin.New(st.Identity, st.Index, st.Queue, aud),
	}
	if a.Clusters != nil {
		sv.Admin.Clusters = registryControl{reg: a.Clusters, connect: Connector(cfg.Ceph)}
	} else {
		sv.Admin.Clusters = memoryControl{}
	}
	smtpPass, err := cfg.SMTP.Password.Resolve()
	if err != nil {
		return nil, fmt.Errorf("smtp password: %w", err)
	}
	sv.Submit = submit.New(submit.Config{
		RelayHost: cfg.SMTP.RelayHost, Username: cfg.SMTP.Username, Password: smtpPass, StartTLS: cfg.SMTP.StartTLS,
		Hostname: hostname(), MaxMessageSize: cfg.Mail.MaxMessageSize,
	}, st.Identity, st.Index, sv.Delivery, aud, nil, log)
	sv.Export = export.New(st.Identity, st.Index, st.Messages, st.Attachments, st.Audit, sv.Delivery, aud, log)
	a.Stores, a.Services = st, sv
	if db != nil {
		a.addCloser(func(context.Context) error { return db.Close() })
	}
	return a, nil
}

// registryControl adapts the cluster registry for the admin service.
type registryControl struct {
	reg     *cluster.Registry
	connect cluster.Connector
}

func (r registryControl) Pick(ctx context.Context) (int64, error) { return r.reg.Pick(ctx) }
func (r registryControl) Verify(c identity.Cluster) (uuid.UUID, error) {
	return cluster.Verify(r.connect, c)
}

// memoryControl: the memory backend has one implicit cluster (id 0);
// registry entries can be created for tests but nothing connects to them.
type memoryControl struct{}

func (memoryControl) Pick(context.Context) (int64, error)        { return 0, nil }
func (memoryControl) Verify(identity.Cluster) (uuid.UUID, error) { return uuid.New(), nil }

// DefaultQuota returns the configured default limits.
func (a *App) DefaultQuota() identity.QuotaLimits {
	q := a.Cfg.Quota
	return identity.QuotaLimits{SoftBytes: q.SoftBytes, HardBytes: q.HardBytes, SoftMessages: q.SoftMessages, HardMessages: q.HardMessages}
}

// listen opens a listener (tcp or unix) with the PROXY protocol mode.
func listen(l config.Listener) (net.Listener, error) {
	mode, err := proxyproto.ParseMode(l.ProxyProtocol)
	if err != nil {
		return nil, err
	}
	network := "tcp"
	if strings.HasPrefix(l.Address, "/") {
		network = "unix"
		_ = os.Remove(l.Address)
	}
	ln, err := net.Listen(network, l.Address)
	if err != nil {
		return nil, err
	}
	return proxyproto.Wrap(ln, mode), nil
}

func (a *App) addCloser(f func(context.Context) error) {
	a.mu.Lock()
	a.closers = append(a.closers, f)
	a.mu.Unlock()
}

// Run starts every configured mode and blocks until ctx is cancelled or a
// listener fails, then shuts everything down.
func (a *App) Run(ctx context.Context) error {
	errc := make(chan error, 8)
	start := func(name string, f func() error) {
		go func() {
			a.Log.Info("listener starting", slog.String("mode", name))
			if err := f(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	// Audit records are batched into one omap write per object per second
	// (they also go to the structured log synchronously).
	a.Stores.Audit.StartBuffering(time.Second, 500, func(err error) { a.Log.Error("audit flush failed", slog.String("error", err.Error())) })
	a.addCloser(func(context.Context) error { a.Stores.Audit.Close(); return nil })

	// Management listener is always on.
	mgmt := http.NewServeMux()
	mgmt.Handle("/metrics", metrics.Handler())
	mgmt.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mgmt.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := a.Stores.Identity.ListDomains(ctx, "", 1); err != nil {
			http.Error(w, "metadata: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	// Cluster registry: state of every connection, and periodic reloads so
	// clusters added or closed for placement are picked up without restart.
	mgmt.HandleFunc("/clusters", func(w http.ResponseWriter, _ *http.Request) {
		var out []cluster.Status
		if a.Clusters != nil {
			out = a.Clusters.Statuses()
		}
		if out == nil {
			out = []cluster.Status{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	if a.Clusters != nil {
		go a.Clusters.Run(ctx, 30*time.Second)
	}
	if err := a.serveHTTP("management", a.Cfg.Listen.Management, mgmt, start); err != nil {
		return err
	}

	if a.Cfg.HasMode("lmtp") {
		srv := lmtp.New(lmtp.Config{Hostname: hostname(), MaxMessageSize: a.Cfg.Mail.MaxMessageSize}, a.Services.Delivery, a.Log)
		ln, err := listen(a.Cfg.Listen.LMTP)
		if err != nil {
			return fmt.Errorf("lmtp listen: %w", err)
		}
		a.addCloser(srv.Shutdown)
		start("lmtp", func() error { return srv.Serve(ln) })
	}
	if a.Cfg.HasMode("postfix") {
		srv := socketmap.New(a.Stores.Identity, a.Services.Delivery, a.Log)
		ln, err := listen(a.Cfg.Listen.Postfix)
		if err != nil {
			return fmt.Errorf("postfix listen: %w", err)
		}
		a.addCloser(func(context.Context) error { return srv.Close() })
		start("postfix", func() error { return srv.Serve(ln) })
	}
	if a.Cfg.HasMode("jmap") {
		if a.JMAPHandler == nil {
			return errors.New("jmap mode enabled but no JMAP handler configured")
		}
		if err := a.serveHTTP("jmap", a.Cfg.Listen.JMAP, a.JMAPHandler, start); err != nil {
			return err
		}
	}
	if a.Cfg.HasMode("admin") {
		if a.AdminHandler == nil {
			return errors.New("admin mode enabled but no admin handler configured")
		}
		if err := a.serveHTTP("admin", a.Cfg.Listen.Admin, a.AdminHandler, start); err != nil {
			return err
		}
	}
	if a.Cfg.HasMode("worker") {
		w := worker.New(worker.Config{
			PostHoldGrace: a.Cfg.Mail.PostHoldGrace, AuditRetention: a.Cfg.Audit.Retention, ExportDir: a.Cfg.Export.Directory,
		}, worker.Stores{
			Identity: a.Stores.Identity, Index: a.Stores.Index, Messages: a.Stores.Messages,
			Attachments: a.Stores.Attachments, Audit: a.Stores.Audit, Queue: a.Stores.Queue,
		}, a.Services.Auditor, a.Log)
		w.Export = a.Services.Export
		wctx, wcancel := context.WithCancel(ctx)
		a.addCloser(func(context.Context) error { wcancel(); return nil })
		start("worker", func() error { return w.Run(wctx) })
	}
	for _, m := range a.ExtraModes {
		if err := m(ctx, a, start); err != nil {
			return err
		}
	}
	for _, m := range []string{"imap", "pop3"} {
		if a.Cfg.HasMode(m) && !a.handled[m] {
			a.Log.Warn("mode not implemented yet; ignoring", slog.String("mode", m))
		}
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	a.Log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a.mu.Lock()
	closers := a.closers
	a.mu.Unlock()
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i](sctx)
	}
	return err
}

func (a *App) serveHTTP(name string, l config.Listener, h http.Handler, start func(string, func() error)) error {
	ln, err := listen(l)
	if err != nil {
		return fmt.Errorf("%s listen: %w", name, err)
	}
	srv := &http.Server{
		Handler: h, ConnContext: httpx.ConnContext,
		ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 5 * time.Minute,
	}
	a.addCloser(srv.Shutdown)
	start(name, func() error { return srv.Serve(ln) })
	return nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "kittiwake"
	}
	return h
}

// Close releases the RADOS client.
func (a *App) Close() error {
	if a.Clusters != nil {
		a.Clusters.Close()
	}
	if a.Client != nil {
		return a.Client.Close()
	}
	return nil
}
