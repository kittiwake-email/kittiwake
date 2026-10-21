package main

import (
	"net/http"

	"dovetail/app"
	"dovetail/proto/httpx"
	"dovetail/proto/rest"
)

// buildAPI assembles the HTTP API served by the api mode: the REST
// endpoints (OAuth, self-service, admin) and JMAP.
func buildAPI(a *app.App) http.Handler {
	trusted, err := httpx.ParseTrusted(a.Cfg.Proxy.TrustedAddresses)
	if err != nil {
		a.Log.Error("proxy.trusted_addresses invalid; ignoring", "error", err.Error())
	}
	rest.Version = app.Version
	restSrv := rest.New(rest.Config{
		Realm: "dovetail", TrustedProxies: trusted, DefaultQuota: a.DefaultQuota(),
		MaxBody: 1 << 20, Identity: a.Stores.Identity,
	}, a.Services.Auth, a.Services.Admin, a.Stores.Audit, a.Stores.Queue, a.Log)

	mux := http.NewServeMux()
	mux.Handle("/v1/", restSrv.Handler())
	mountJMAP(a, mux, trusted)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("dovetail " + app.Version + "\n"))
			return
		}
		http.NotFound(w, r)
	})
	return mux
}
