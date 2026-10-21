package jmap

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/service/actor"
)

// serveEventSource streams StateChange events (RFC 8620 §7.3).
func (s *Server) serveEventSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	act := actor.From(r.Context())
	if act.MailboxID == uuid.Nil {
		http.Error(w, "no mailbox for this account", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	q := r.URL.Query()
	types := map[string]bool{}
	for _, t := range strings.Split(q.Get("types"), ",") {
		if t = strings.TrimSpace(t); t != "" && t != "*" {
			types[t] = true
		}
	}
	closeAfter := q.Get("closeafter") == "state"
	ping, _ := strconv.Atoi(q.Get("ping"))

	ctx := r.Context()
	events, stop, err := s.mb.Watch(ctx, act.MailboxID)
	if err != nil {
		http.Error(w, "cannot watch mailbox", http.StatusForbidden)
		return
	}
	defer stop()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	var pingC <-chan time.Time
	if ping > 0 {
		t := time.NewTicker(time.Duration(ping) * time.Second)
		defer t.Stop()
		pingC = t.C
	}
	emit := func() bool {
		state := s.state(ctx, act.MailboxID)
		changed := map[string]any{}
		for _, t := range []string{"Email", "Mailbox", "Thread"} {
			if len(types) == 0 || types[t] {
				changed[t] = state
			}
		}
		if len(changed) == 0 {
			return true
		}
		body, _ := json.Marshal(map[string]any{"@type": "StateChange", "changed": map[string]any{act.MailboxID.String(): changed}})
		if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", body); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-pingC:
			if _, err := fmt.Fprintf(w, "event: ping\ndata: {\"interval\": %d}\n\n", ping); err != nil {
				return
			}
			flusher.Flush()
		case _, ok := <-events:
			if !ok {
				return
			}
			// Coalesce bursts.
			drain := true
			for drain {
				select {
				case <-events:
				default:
					drain = false
				}
			}
			if !emit() {
				return
			}
			if closeAfter {
				return
			}
		}
	}
}
