package main

import (
	"context"
	"net"
	"net/http"

	"github.com/google/uuid"

	"dovetail/app"
	"dovetail/proto/httpx"
	"dovetail/proto/jmap"
)

// mountJMAP adds the JMAP session resource and endpoints behind the
// bearer/basic authentication middleware.
func mountJMAP(a *app.App, mux *http.ServeMux, trusted []net.IPNet) {
	srv := jmap.New(jmap.Config{
		BaseURL: a.Cfg.Session.Issuer, MaxUploadSize: a.Cfg.Mail.MaxMessageSize,
		HoldCheck: func(ctx context.Context, mb uuid.UUID) bool {
			v, err := a.Stores.Identity.GetMailbox(ctx, mb)
			return err == nil && v.Value.Hold != nil
		},
	}, a.Services.Mailbox, a.Services.Delivery, a.Log)
	auth := httpx.Auth(a.Services.Auth, "jmap", trusted, "dovetail")
	mux.Handle("/.well-known/jmap", auth(srv.SessionHandler()))
	mux.Handle("/jmap/", auth(srv))
}
