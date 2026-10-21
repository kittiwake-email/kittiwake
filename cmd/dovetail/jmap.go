package main

import (
	"net"
	"net/http"

	"dovetail/app"
)

// mountJMAP adds the JMAP endpoints; filled in when proto/jmap lands.
func mountJMAP(a *app.App, mux *http.ServeMux, trusted []net.IPNet) {}
