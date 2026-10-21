package main

import (
	"net/http"

	"dovetail/app"
)

// buildAPI assembles the HTTP API (JMAP + REST). Wired once the protocol
// packages land; until then the api mode serves a placeholder.
func buildAPI(a *app.App) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "API not wired yet", http.StatusNotImplemented)
	})
	return mux
}
