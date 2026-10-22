package main

import (
	"net"
	"net/http"

	"kittiwake/app"
	"kittiwake/proto/httpx"
	"kittiwake/proto/rest"
)

// restServer builds the REST adapter shared by the jmap and admin modes
// (they mount different handlers of it on different listeners).
func restServer(a *app.App) (*rest.Server, []net.IPNet) {
	trusted, err := httpx.ParseTrusted(a.Cfg.Proxy.TrustedAddresses)
	if err != nil {
		a.Log.Error("proxy.trusted_addresses invalid; ignoring", "error", err.Error())
	}
	rest.Version = app.Version
	srv := rest.New(rest.Config{
		Realm: "kittiwake", TrustedProxies: trusted, DefaultQuota: a.DefaultQuota(),
		MaxBody: 1 << 20, Identity: a.Stores.Identity,
		Export: a.Services.Export, ExportDir: a.Cfg.Export.Directory, Submit: a.Services.Submit,
	}, a.Services.Auth, a.Services.Admin, a.Stores.Audit, a.Stores.Queue, a.Log)
	return srv, trusted
}

// buildJMAP assembles the user-facing listener: JMAP plus the OAuth token
// and self-service endpoints clients need next to it.
func buildJMAP(a *app.App) http.Handler {
	srv, trusted := restServer(a)
	mux := http.NewServeMux()
	mux.Handle("/v1/", srv.AccountHandler())
	mountJMAP(a, mux, trusted)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("kittiwake " + app.Version + " (jmap)\n"))
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

// buildAdmin assembles the operator-facing listener: the admin REST API only.
func buildAdmin(a *app.App) http.Handler {
	srv, _ := restServer(a)
	return srv.AdminHandler() // /v1/admin/*, plus /openapi.yaml and / at the root
}
