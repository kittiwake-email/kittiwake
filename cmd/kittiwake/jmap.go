package main

import (
	"context"
	"net"
	"net/http"

	"github.com/google/uuid"

	"kittiwake/app"
	"kittiwake/proto/httpx"
	"kittiwake/proto/jmap"
)

// mountJMAP adds the JMAP session resource and endpoints behind the
// bearer/basic authentication middleware.
func mountJMAP(a *app.App, mux *http.ServeMux, trusted []net.IPNet) {
	srv := jmap.New(jmap.Config{
		BaseURL: a.Cfg.Session.Issuer, MaxUploadSize: a.Cfg.Mail.MaxMessageSize, Submit: a.Services.Submit,
		Uploads: a.Stores.Messages,
		HoldCheck: func(ctx context.Context, mb uuid.UUID) bool {
			v, err := a.Stores.Identity.GetMailbox(ctx, mb)
			return err == nil && v.Value.Hold != nil
		},
	}, a.Services.Mailbox, a.Services.Delivery, a.Log)
	auth := httpx.Auth(a.Services.Auth, "jmap", trusted, "kittiwake")
	mux.Handle("/.well-known/jmap", auth(srv.SessionHandler()))
	mux.Handle("/jmap/", auth(srv))
}
