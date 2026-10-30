package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kittiwake/app"
	"kittiwake/config"
	"kittiwake/proto/httpx"
	"kittiwake/proto/proxy"
	"kittiwake/store/identity"
	"kittiwake/store/storage"
)

// runProxy runs the proxy mode: no Ceph, no stores but the identity
// store over a read-only database connection, four front ends and the
// management listener. It registers nothing (a proxy never writes).
func runProxy(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	dbCfg := cfg.Database
	if dbCfg.ProxyDSN != "" {
		dbCfg.DSN, dbCfg.ReadDSN = dbCfg.ProxyDSN, ""
	}
	db, err := app.OpenDatabase(ctx, dbCfg)
	if err != nil {
		return err
	}
	if db == nil {
		return errors.New("proxy: database.dsn is required")
	}
	defer db.Close()
	ids := identity.NewWith(db)
	ids.CacheTTL = cfg.Cache.Identity
	trusted, err := httpx.ParseTrusted(cfg.Proxy.TrustedAddresses)
	if err != nil {
		log.Error("proxy.trusted_addresses invalid; ignoring", "error", err.Error())
	}
	host, _ := os.Hostname()
	px := proxy.New(proxy.Config{
		Hostname: host, ConnectTimeout: cfg.Proxy.ConnectTimeout, PreAuthTimeout: cfg.Proxy.PreAuthTimeout,
		NodeTTL: cfg.Proxy.NodeTTL, DownFor: cfg.Proxy.DownFor, IngressTLS: cfg.Proxy.IngressTLS, IngressPorts: cfg.Proxy.IngressPorts,
		MaxMessageSize: cfg.Mail.MaxMessageSize, RequireTLSProxy: cfg.Submission.RequireTLSProxy, TrustedProxies: trusted,
	}, ids, log)

	errc := make(chan error, 8)
	var closers []func(context.Context) error
	start := func(name string, f func() error) {
		go func() {
			log.Info("listener starting", slog.String("mode", "proxy-"+name))
			if err := f(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("proxy %s: %w", name, err)
			}
		}()
	}
	tcp := func(name string, l config.Listener, serve func(net.Listener) error) error {
		if l.Address == "" {
			return nil
		}
		ln, err := app.Listen(l)
		if err != nil {
			return fmt.Errorf("proxy %s listen: %w", name, err)
		}
		closers = append(closers, func(context.Context) error { return ln.Close() })
		start(name, func() error { return serve(ln) })
		return nil
	}
	pl := cfg.Listen.Proxy
	if err := tcp("imap", pl.IMAP, px.ServeIMAP); err != nil {
		return err
	}
	if err := tcp("pop3", pl.POP3, px.ServePOP3); err != nil {
		return err
	}
	if err := tcp("submission", pl.Submission, px.ServeSubmission); err != nil {
		return err
	}
	if pl.JMAP.Address != "" {
		ln, err := app.Listen(pl.JMAP)
		if err != nil {
			return fmt.Errorf("proxy jmap listen: %w", err)
		}
		srv := app.HTTPServer(px.HTTPHandler(), proxy.ConnContext)
		closers = append(closers, srv.Shutdown)
		start("jmap", func() error { return srv.Serve(ln) })
	}
	mgmt := app.ManagementHandler(func(ctx context.Context) error {
		_, err := ids.ListNodes(ctx, "imap")
		return err
	}, func() []storage.Status { return nil })
	mln, err := app.Listen(config.Listener{Address: cfg.Listen.ManagementAddress(cfg.Modes), ProxyProtocol: "off"})
	if err != nil {
		return fmt.Errorf("management listen: %w", err)
	}
	msrv := app.HTTPServer(mgmt, httpx.ConnContext)
	closers = append(closers, msrv.Shutdown)
	start("management", func() error { return msrv.Serve(mln) })
	log.Info("kittiwake starting", "version", app.Version, "modes", cfg.Modes, "ingress_tls", cfg.Proxy.IngressTLS)

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i](sctx)
	}
	_ = px.Shutdown(sctx)
	return err
}
