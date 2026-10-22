package jmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"kittiwake/service/actor"
)

// methodError is a JMAP method-level error (RFC 8620 §3.6.2).
type methodError struct {
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Extra       map[string]any `json:"-"`
}

func (e *methodError) Error() string { return e.Type + ": " + e.Description }

func (e *methodError) toMap() map[string]any {
	m := map[string]any{"type": e.Type}
	if e.Description != "" {
		m["description"] = e.Description
	}
	for k, v := range e.Extra {
		m[k] = v
	}
	return m
}

func invalidArguments(desc string) *methodError {
	return &methodError{Type: "invalidArguments", Description: desc}
}

// methodFunc handles one method call.
type methodFunc func(ctx context.Context, req *requestState, args map[string]any) (map[string]any, *methodError)

// requestState carries per-request data: created ids and prior responses.
type requestState struct {
	createdIds map[string]string
	responses  []invocation
	// after holds implicit responses a method wants emitted right after its
	// own (e.g. the Email/set that EmailSubmission/set triggers).
	after []invocation
}

type invocation struct {
	name   string
	args   map[string]any
	callID string
}

// Invocation JSON form: [name, args, callId].
func (i invocation) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{i.name, i.args, i.callID})
}

func (i *invocation) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return errors.New("invocation must have three elements")
	}
	if err := json.Unmarshal(raw[0], &i.name); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &i.args); err != nil {
		return err
	}
	if i.args == nil {
		i.args = map[string]any{}
	}
	return json.Unmarshal(raw[2], &i.callID)
}

type request struct {
	Using       []string          `json:"using"`
	MethodCalls []invocation      `json:"methodCalls"`
	CreatedIds  map[string]string `json:"createdIds"`
}

type response struct {
	MethodResponses []invocation      `json:"methodResponses"`
	CreatedIds      map[string]string `json:"createdIds,omitempty"`
	SessionState    string            `json:"sessionState"`
}

var knownCapabilities = map[string]bool{CapCore: true, CapMail: true, CapQuota: true}

// writeRequestError writes an RFC 8620 §3.6.1 request-level error.
func writeRequestError(w http.ResponseWriter, status int, typ, detail string, extra map[string]any) {
	body := map[string]any{"type": "urn:ietf:params:jmap:error:" + typ, "status": status}
	if detail != "" {
		body["detail"] = detail
	}
	for k, v := range extra {
		body[k] = v
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) serveAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 10<<20))
	if err != nil {
		writeRequestError(w, http.StatusBadRequest, "limit", "request too large", map[string]any{"limit": "maxSizeRequest"})
		return
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		writeRequestError(w, http.StatusBadRequest, "notJSON", err.Error(), nil)
		return
	}
	if req.MethodCalls == nil {
		writeRequestError(w, http.StatusBadRequest, "notRequest", "methodCalls is required", nil)
		return
	}
	for _, c := range req.Using {
		if !s.knownCapability(c) {
			writeRequestError(w, http.StatusBadRequest, "unknownCapability", "unknown capability "+c, nil)
			return
		}
	}
	if len(req.MethodCalls) > s.cfg.MaxCallsInRequest {
		writeRequestError(w, http.StatusBadRequest, "limit", "too many method calls", map[string]any{"limit": "maxCallsInRequest"})
		return
	}
	st := &requestState{createdIds: map[string]string{}}
	for k, v := range req.CreatedIds {
		st.createdIds[k] = v
	}
	ctx := r.Context()
	for _, call := range req.MethodCalls {
		args, merr := resolveReferences(call.args, st.responses)
		var result map[string]any
		if merr == nil {
			fn, ok := s.methods[call.name]
			if !ok {
				merr = &methodError{Type: "unknownMethod"}
			} else {
				func() {
					defer func() {
						if rec := recover(); rec != nil {
							s.log.ErrorContext(ctx, "jmap method panic", "method", call.name, "panic", fmt.Sprint(rec))
							merr = serverFail("internal error")
						}
					}()
					result, merr = fn(ctx, st, args)
				}()
			}
		}
		if merr != nil {
			st.responses = append(st.responses, invocation{name: "error", args: merr.toMap(), callID: call.callID})
			continue
		}
		st.responses = append(st.responses, invocation{name: call.name, args: result, callID: call.callID})
		for _, extra := range st.after {
			extra.callID = call.callID
			st.responses = append(st.responses, extra)
		}
		st.after = nil
	}
	act := actor.From(ctx)
	resp := response{MethodResponses: st.responses, SessionState: "0"}
	if act.MailboxID != [16]byte{} {
		resp.SessionState = s.state(ctx, act.MailboxID)
	}
	if len(st.createdIds) > 0 {
		resp.CreatedIds = st.createdIds
	}
	writeJSON(w, http.StatusOK, resp)
}

// resolveReferences replaces "#key" result references (RFC 8620 §3.7).
func resolveReferences(args map[string]any, prior []invocation) (map[string]any, *methodError) {
	out := make(map[string]any, len(args))
	for k, v := range args {
		if !strings.HasPrefix(k, "#") {
			out[k] = v
			continue
		}
		ref, ok := v.(map[string]any)
		if !ok {
			return nil, invalidArguments("result reference must be an object")
		}
		resultOf, _ := ref["resultOf"].(string)
		name, _ := ref["name"].(string)
		path, _ := ref["path"].(string)
		var found *invocation
		for i := range prior {
			if prior[i].callID == resultOf && prior[i].name == name {
				found = &prior[i]
				break
			}
		}
		if found == nil {
			return nil, &methodError{Type: "invalidResultReference", Description: "no prior response " + name + "/" + resultOf}
		}
		val, err := jsonPointer(normalizeJSON(found.args), path)
		if err != nil {
			return nil, &methodError{Type: "invalidResultReference", Description: err.Error()}
		}
		out[k[1:]] = val
	}
	return out, nil
}

// normalizeJSON converts typed Go values (e.g. []string) into the generic
// JSON shapes (map[string]any, []any, float64, ...) via a round trip.
func normalizeJSON(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return v
	}
	return out
}

// jsonPointer evaluates an RFC 6901 pointer with the RFC 8620 "*" extension.
func jsonPointer(v any, path string) (any, error) {
	if path == "" {
		return v, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, errors.New("path must start with /")
	}
	parts := strings.Split(path[1:], "/")
	return walkPointer(v, parts)
}

func walkPointer(v any, parts []string) (any, error) {
	if len(parts) == 0 {
		return v, nil
	}
	tok := strings.ReplaceAll(strings.ReplaceAll(parts[0], "~1", "/"), "~0", "~")
	switch cur := v.(type) {
	case map[string]any:
		next, ok := cur[tok]
		if !ok {
			return nil, fmt.Errorf("path element %q not found", tok)
		}
		return walkPointer(next, parts[1:])
	case []any:
		if tok == "*" {
			var out []any
			for _, item := range cur {
				r, err := walkPointer(item, parts[1:])
				if err != nil {
					return nil, err
				}
				if arr, ok := r.([]any); ok {
					out = append(out, arr...)
				} else {
					out = append(out, r)
				}
			}
			if out == nil {
				out = []any{}
			}
			return out, nil
		}
		i, err := strconv.Atoi(tok)
		if err != nil || i < 0 || i >= len(cur) {
			return nil, fmt.Errorf("bad array index %q", tok)
		}
		return walkPointer(cur[i], parts[1:])
	}
	return nil, fmt.Errorf("cannot descend into %q", tok)
}

// ---- argument helpers ----

func argString(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func argStrings(args map[string]any, key string) ([]string, bool, *methodError) {
	v, ok := args[key]
	if !ok || v == nil {
		return nil, false, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, true, invalidArguments(key + " must be an array")
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		s, ok := x.(string)
		if !ok {
			return nil, true, invalidArguments(key + " must contain strings")
		}
		out = append(out, s)
	}
	return out, true, nil
}

func argMap(args map[string]any, key string) (map[string]any, *methodError) {
	v, ok := args[key]
	if !ok || v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, invalidArguments(key + " must be an object")
	}
	return m, nil
}

func argInt(args map[string]any, key string, def int) (int, *methodError) {
	v, ok := args[key]
	if !ok || v == nil {
		return def, nil
	}
	f, ok := v.(float64)
	if !ok || f != float64(int64(f)) {
		return 0, invalidArguments(key + " must be an integer")
	}
	return int(f), nil
}

func argBool(args map[string]any, key string, def bool) bool {
	if b, ok := args[key].(bool); ok {
		return b
	}
	return def
}

// resolveCreatedID maps "#creationId" to the real id.
func (st *requestState) resolveCreatedID(id string) string {
	if strings.HasPrefix(id, "#") {
		if real, ok := st.createdIds[id[1:]]; ok {
			return real
		}
	}
	return id
}

func (s *Server) coreEcho(_ context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	return args, nil
}

// getArgs is the common shape of a /get call.
type getArgs struct {
	ids        []string
	allIDs     bool
	properties map[string]bool // nil = all
}

func parseGetArgs(args map[string]any) (getArgs, *methodError) {
	var g getArgs
	ids, present, merr := argStrings(args, "ids")
	if merr != nil {
		return g, merr
	}
	g.ids, g.allIDs = ids, !present
	props, present, merr := argStrings(args, "properties")
	if merr != nil {
		return g, merr
	}
	if present {
		g.properties = map[string]bool{"id": true}
		for _, p := range props {
			g.properties[p] = true
		}
	}
	return g, nil
}

func (g getArgs) want(prop string) bool {
	if g.properties == nil {
		return true
	}
	if g.properties[prop] {
		return true
	}
	// header:... properties are matched by prefix.
	for p := range g.properties {
		if p == prop {
			return true
		}
	}
	return false
}

func (g getArgs) headerProps() []string {
	var out []string
	for p := range g.properties {
		if strings.HasPrefix(p, "header:") {
			out = append(out, p)
		}
	}
	return out
}

// stateString formats a modseq.
func stateString(m uint64) string { return strconv.FormatUint(m, 10) }

func parseState(s string) (uint64, *methodError) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, invalidArguments("bad state")
	}
	return n, nil
}
