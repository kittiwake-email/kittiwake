// Package app wires configuration, the RADOS client, stores, services and
// protocol listeners into a running Dovetail process. cmd/dovetail is a
// thin command-line layer over it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"dovetail/config"
	"dovetail/metrics"
	"dovetail/proto/httpx"
	"dovetail/proto/lmtp"
	"dovetail/proto/proxyproto"
	"dovetail/proto/socketmap"
	"dovetail/rados"
	"dovetail/service/admin"
	"dovetail/service/auditor"
	authsvc "dovetail/service/auth"
	"dovetail/service/delivery"
	"dovetail/service/mailbox"
	"dovetail/store/attach"
	"dovetail/store/audit"
	"dovetail/store/identity"
	"dovetail/store/index"
	"dovetail/store/message"
	"dovetail/store/queue"
	"dovetail/store/session"
	"dovetail/worker"
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
	Sessions    *session.Store
	Queue       *queue.Store
}

// Services bundles every service.
type Services struct {
	Auditor  *auditor.Auditor
	Auth     *authsvc.Service
	Delivery *delivery.Service
	Mailbox  *mailbox.Service
	Admin    *admin.Service
}

// App is a configured Dovetail process.
type App struct {
	Cfg      config.Config
	Log      *slog.Logger
	Client   rados.Client
	Stores   Stores
	Services Services
	// APIHandler is mounted by the api mode; set by cmd (keeps app free of
	// the JMAP/REST packages' construction details).
	APIHandler http.Handler
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

// Connect opens the RADOS client described by the config.
func Connect(cfg config.Ceph) (rados.Client, error) {
	if cfg.Backend == "memory" {
		return rados.NewMemoryClient(), nil
	}
	key, err := cfg.Key.Resolve()
	if err != nil {
		return nil, fmt.Errorf("ceph key: %w", err)
	}
	return rados.Connect(rados.Config{
		ConfigFile: cfg.ConfigFile, Monitors: strings.Join(cfg.Monitors, ","), User: cfg.User,
		Key: key, Keyring: cfg.Keyring, Options: cfg.Options,
	})
}

// New builds stores and services on an open client.
func New(cfg config.Config, client rados.Client, log *slog.Logger) (*App, error) {
	pools := cfg.Ceph.Pools
	meta, err := client.Pool(pools.Meta)
	if err != nil {
		return nil, fmt.Errorf("pool %s: %w", pools.Meta, err)
	}
	idxp, err := client.Pool(pools.Index)
	if err != nil {
		return nil, fmt.Errorf("pool %s: %w", pools.Index, err)
	}
	msgp, err := client.Pool(pools.Messages)
	if err != nil {
		return nil, fmt.Errorf("pool %s: %w", pools.Messages, err)
	}
	attp, err := client.Pool(pools.Attachments)
	if err != nil {
		return nil, fmt.Errorf("pool %s: %w", pools.Attachments, err)
	}
	st := Stores{
		Identity: identity.New(meta), Index: index.New(idxp), Messages: message.New(msgp), Attachments: attach.New(attp),
		Audit: audit.New(idxp), Sessions: session.New(idxp), Queue: queue.New(idxp),
	}
	aud := auditor.New(st.Audit, log, cfg.Audit.Detail)
	dq := identity.QuotaLimits{SoftBytes: cfg.Quota.SoftBytes, HardBytes: cfg.Quota.HardBytes, SoftMessages: cfg.Quota.SoftMessages, HardMessages: cfg.Quota.HardMessages}
	sv := Services{
		Auditor: aud,
		Auth: authsvc.New(st.Identity, st.Sessions, aud, authsvc.Config{
			AccessTokenLifetime: cfg.Session.AccessTokenLifetime, RefreshTokenLifetime: cfg.Session.RefreshTokenLifetime, Issuer: cfg.Session.Issuer,
		}),
		Delivery: delivery.New(st.Identity, st.Index, st.Messages, st.Attachments, st.Queue, aud, log, delivery.Config{
			MaxMessageSize: cfg.Mail.MaxMessageSize, InlineMax: cfg.Mail.InlineAttachmentMax, ForceFullRaw: cfg.Mail.ForceFullRaw,
			SpamHeader: cfg.Mail.SpamHeader, SpamValue: cfg.Mail.SpamValue, MaxAliasDepth: cfg.Mail.MaxAliasDepth,
			DefaultQuota: dq, OverQuotaTempFail: cfg.Mail.OverQuotaTempFail, DisabledTempFail: cfg.Mail.DisabledTempFail,
		}),
		Mailbox: mailbox.New(st.Identity, st.Index, st.Messages, st.Attachments, st.Queue, aud, log, mailbox.Config{TrashRetention: cfg.Mail.TrashRetention, DefaultQuota: dq}),
		Admin:   admin.New(st.Identity, st.Index, st.Queue, aud),
	}
	return &App{Cfg: cfg, Log: log, Client: client, Stores: st, Services: sv}, nil
}

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

	// Management listener is always on.
	mgmt := http.NewServeMux()
	mgmt.Handle("/metrics", metrics.Handler())
	mgmt.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mgmt.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := a.Stores.Identity.ListDomains(ctx, "", 1); err != nil {
			http.Error(w, "ceph: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
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
	if a.Cfg.HasMode("api") {
		if a.APIHandler == nil {
			return errors.New("api mode enabled but no API handler configured")
		}
		if err := a.serveHTTP("api", a.Cfg.Listen.API, a.APIHandler, start); err != nil {
			return err
		}
	}
	if a.Cfg.HasMode("worker") {
		idxp, err := a.Client.Pool(a.Cfg.Ceph.Pools.Index)
		if err != nil {
			return err
		}
		w := worker.New(worker.Config{
			PostHoldGrace: a.Cfg.Mail.PostHoldGrace, AuditRetention: a.Cfg.Audit.Retention,
		}, worker.Stores{
			Identity: a.Stores.Identity, Index: a.Stores.Index, Messages: a.Stores.Messages,
			Attachments: a.Stores.Attachments, Audit: a.Stores.Audit, Queue: a.Stores.Queue,
		}, a.Services.Auditor, idxp, a.Log)
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
		return "dovetail"
	}
	return h
}

// Close releases the RADOS client.
func (a *App) Close() error { return a.Client.Close() }
