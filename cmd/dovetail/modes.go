package main

import (
	"context"
	"fmt"
	"os"

	"dovetail/app"
	"dovetail/proto/imap"
)

// protocolModes registers the IMAP (and later POP3) servers with the app.
func protocolModes() []app.ModeFunc {
	return []app.ModeFunc{imapMode}
}

func imapMode(ctx context.Context, a *app.App, start func(string, func() error)) error {
	if !a.Cfg.HasMode("imap") {
		return nil
	}
	host, _ := os.Hostname()
	srv := imap.New(imap.Config{Hostname: host, MaxLiteralSize: a.Cfg.Mail.MaxMessageSize}, a.Services.Auth, a.Services.Mailbox, a.Services.Delivery, a.Log)
	ln, err := app.Listen(a.Cfg.Listen.IMAP)
	if err != nil {
		return fmt.Errorf("imap listen: %w", err)
	}
	a.AddCloser(srv.Shutdown)
	a.MarkHandled("imap")
	start("imap", func() error { return srv.Serve(ln) })
	return nil
}
