package jmap

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"

	"dovetail/service/submit"
)

// CapSubmission is the JMAP submission capability (RFC 8621 §7).
const CapSubmission = "urn:ietf:params:jmap:submission"

// submissionEnabled reports whether sending is configured.
func (s *Server) submissionEnabled() bool { return s.cfg.Submit != nil }

// knownCapability reports whether a "using" entry is served.
func (s *Server) knownCapability(c string) bool {
	if c == CapSubmission {
		return s.submissionEnabled()
	}
	return knownCapabilities[c]
}

// registerSubmission adds the submission methods when a submit service is
// configured. Without one the methods are unknown (unknownMethod).
func (s *Server) registerSubmission() {
	if !s.submissionEnabled() {
		return
	}
	s.methods["Identity/changes"] = s.identityChanges
	s.methods["Identity/set"] = s.identitySet
	s.methods["EmailSubmission/get"] = s.emailSubmissionGet
	s.methods["EmailSubmission/changes"] = s.emailSubmissionChanges
	s.methods["EmailSubmission/query"] = s.emailSubmissionQuery
	s.methods["EmailSubmission/set"] = s.emailSubmissionSet
}

// identities returns the JMAP Identity objects of a mailbox.
func (s *Server) identities(ctx context.Context, mb uuid.UUID) ([]map[string]any, error) {
	list, err := s.cfg.Submit.Identities(ctx, mb)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(list))
	for _, id := range list {
		out = append(out, map[string]any{
			"id": id.ID, "name": id.Name, "email": id.Email, "replyTo": nil, "bcc": nil,
			"textSignature": "", "htmlSignature": "", "mayDelete": false,
		})
	}
	return out, nil
}

func (s *Server) identityChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	if _, merr := s.accountFor(ctx, args); merr != nil {
		return nil, merr
	}
	return nil, &methodError{Type: "cannotCalculateChanges"}
}

// identitySet: identities come from the account and its aliases, so
// creating or destroying one is refused. Updating is allowed for "name"
// only, which sets the account's display name (shared by all its
// identities).
func (s *Server) identitySet(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	notCreated := map[string]any{}
	if create, _ := args["create"].(map[string]any); create != nil {
		for cid := range create {
			notCreated[cid] = setError("forbidden", "identities come from the account and its aliases")
		}
	}
	notDestroyed := map[string]any{}
	if destroy, _ := args["destroy"].([]any); destroy != nil {
		for _, d := range destroy {
			notDestroyed[fmt.Sprint(d)] = setError("forbidden", "identities come from the account and its aliases")
		}
	}
	updated := map[string]any{}
	notUpdated := map[string]any{}
	if update, _ := args["update"].(map[string]any); update != nil {
		known := map[string]bool{}
		if list, err := s.cfg.Submit.Identities(ctx, mb); err == nil {
			for _, id := range list {
				known[id.ID] = true
			}
		}
		for id, patchAny := range update {
			patch, _ := patchAny.(map[string]any)
			if !known[id] {
				notUpdated[id] = setError("notFound", "")
				continue
			}
			name, hasName := patch["name"].(string)
			var bad []string
			for k := range patch {
				if k != "name" {
					bad = append(bad, k)
				}
			}
			if len(bad) > 0 || !hasName {
				notUpdated[id] = setError("invalidProperties", "only name can be changed", bad...)
				continue
			}
			if _, err := s.cfg.Submit.SetName(ctx, name); err != nil {
				notUpdated[id] = mapErr(err).toMap()
				continue
			}
			updated[id] = nil
		}
	}
	return map[string]any{
		"accountId": mb.String(), "oldState": "0", "newState": "0",
		"created": map[string]any{}, "updated": updated, "destroyed": []any{},
		"notCreated": notCreated, "notUpdated": notUpdated, "notDestroyed": notDestroyed,
	}, nil
}

func (s *Server) emailSubmissionGet(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	// Submissions are not persisted: a submission exists only in the
	// response that created it, so every lookup is notFound.
	notFound := []string{}
	if !g.allIDs {
		notFound = append(notFound, g.ids...)
	}
	return map[string]any{"accountId": mb.String(), "state": "0", "list": []any{}, "notFound": notFound}, nil
}

func (s *Server) emailSubmissionChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	if _, merr := s.accountFor(ctx, args); merr != nil {
		return nil, merr
	}
	return nil, &methodError{Type: "cannotCalculateChanges"}
}

func (s *Server) emailSubmissionQuery(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	return map[string]any{
		"accountId": mb.String(), "queryState": "0", "canCalculateChanges": false,
		"position": 0, "ids": []string{}, "total": 0,
	}, nil
}

var relayPermanentRe = regexp.MustCompile(`\b5\d\d\b`)

// submitError maps a submit service error to a SetError.
func submitError(err error) map[string]any {
	switch {
	case errors.Is(err, submit.ErrBadSender):
		return setError("invalidProperties", err.Error(), "identityId", "envelope")
	case errors.Is(err, submit.ErrNoRecipients):
		return setError("invalidProperties", err.Error(), "envelope")
	case errors.Is(err, submit.ErrInvalid):
		return setError("invalidEmail", err.Error())
	case errors.Is(err, submit.ErrHold), errors.Is(err, submit.ErrForbidden):
		return setError("forbidden", err.Error())
	case errors.Is(err, submit.ErrTooLarge):
		return setError("tooLarge", err.Error())
	case errors.Is(err, submit.ErrRelay):
		// A permanent (5xx) relay rejection means the message itself is
		// unacceptable; anything else is a transient server-side problem.
		if relayPermanentRe.MatchString(err.Error()) {
			return setError("invalidEmail", err.Error())
		}
		return setError("serverFail", err.Error())
	}
	return setError("serverFail", err.Error())
}

// emailSubmissionSet sends emails (RFC 8621 §7.5). Only create is
// supported; update/destroy (undo) are refused because submissions are
// final and not persisted.
func (s *Server) emailSubmissionSet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	if ifIn, ok := argString(args, "ifInState"); ok && ifIn != "0" {
		return nil, &methodError{Type: "stateMismatch"}
	}
	create, merr := argMap(args, "create")
	if merr != nil {
		return nil, merr
	}
	update, merr := argMap(args, "update")
	if merr != nil {
		return nil, merr
	}
	destroy, _, merr := argStrings(args, "destroy")
	if merr != nil {
		return nil, merr
	}
	if len(create)+len(update)+len(destroy) > s.cfg.MaxObjectsInSet {
		return nil, &methodError{Type: "requestTooLarge"}
	}
	onUpdate, merr := argMap(args, "onSuccessUpdateEmail")
	if merr != nil {
		return nil, merr
	}
	onDestroy, _, merr := argStrings(args, "onSuccessDestroyEmail")
	if merr != nil {
		return nil, merr
	}
	idents, err := s.identities(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	identByID := map[string]string{} // id -> email
	for _, id := range idents {
		identByID[id["id"].(string)] = id["email"].(string)
	}

	created, notCreated := map[string]any{}, map[string]any{}
	createdEmail := map[string]string{} // "#cid" and submission id -> emailId
	for cid, objAny := range create {
		obj, ok := objAny.(map[string]any)
		if !ok {
			notCreated[cid] = setError("invalidProperties", "must be an object")
			continue
		}
		identityID, _ := argString(obj, "identityId")
		email, ok := identByID[identityID]
		if !ok {
			notCreated[cid] = setError("invalidProperties", "unknown identityId", "identityId")
			continue
		}
		emailIDStr, _ := argString(obj, "emailId")
		emailIDStr = st.resolveCreatedID(emailIDStr)
		emailID, err := uuid.Parse(emailIDStr)
		if err != nil {
			notCreated[cid] = setError("invalidProperties", "emailId is required", "emailId")
			continue
		}
		// Envelope: default mailFrom is the identity's address; rcptTo is
		// derived from the headers by the submit service when absent.
		req := submit.Request{MailboxID: mb, From: email}
		envelope, merr := argMap(obj, "envelope")
		if merr != nil {
			notCreated[cid] = setError("invalidProperties", "envelope must be an object", "envelope")
			continue
		}
		if envelope != nil {
			if mf, merr := argMap(envelope, "mailFrom"); merr == nil && mf != nil {
				if e, ok := argString(mf, "email"); ok {
					req.From = e
				}
			}
			if rt, ok := envelope["rcptTo"].([]any); ok {
				for _, r := range rt {
					if m, ok := r.(map[string]any); ok {
						if e, ok := argString(m, "email"); ok {
							req.To = append(req.To, e)
						}
					}
				}
			}
		}
		_, rec, err := s.mb.Get(ctx, mb, emailID)
		if err != nil {
			notCreated[cid] = setError("invalidProperties", "emailId not found", "emailId")
			continue
		}
		raw, err := s.mb.Raw(ctx, mb, emailID)
		if err != nil {
			notCreated[cid] = mapErr(err).toMap()
			continue
		}
		req.Raw = raw
		// With JMAP the client decides where the sent email lives through
		// onSuccessUpdateEmail (typically moving it from Drafts to Sent), so
		// the submit service must not store its own Sent copy.
		noCopy := false
		req.SaveToSent = &noCopy
		res, err := s.cfg.Submit.Send(ctx, req)
		if err != nil {
			notCreated[cid] = submitError(err)
			continue
		}
		rcpt := make([]map[string]any, 0, len(res.Recipients))
		for _, r := range res.Recipients {
			rcpt = append(rcpt, map[string]any{"email": r, "parameters": nil})
		}
		subID := uuid.NewString()
		created[cid] = map[string]any{
			"id": subID, "identityId": identityID, "emailId": emailIDStr, "threadId": rec.ThreadID,
			"envelope":       map[string]any{"mailFrom": map[string]any{"email": res.EnvelopeFrom, "parameters": nil}, "rcptTo": rcpt},
			"sendAt":         res.SentAt.UTC().Format(time.RFC3339),
			"undoStatus":     "final",
			"deliveryStatus": nil,
			"dsnBlobIds":     []string{},
			"mdnBlobIds":     []string{},
		}
		st.createdIds[cid] = subID
		createdEmail["#"+cid] = emailIDStr
		createdEmail[subID] = emailIDStr
	}
	notUpdated, notDestroyed := map[string]any{}, map[string]any{}
	for id := range update {
		notUpdated[id] = setError("notFound", "submissions cannot be updated")
	}
	for _, id := range destroy {
		notDestroyed[id] = setError("notFound", "submissions cannot be destroyed")
	}
	result := map[string]any{
		"accountId":    mb.String(),
		"oldState":     "0",
		"newState":     "0",
		"created":      created,
		"updated":      map[string]any{},
		"destroyed":    []string{},
		"notCreated":   notCreated,
		"notUpdated":   notUpdated,
		"notDestroyed": notDestroyed,
	}

	// onSuccess*: apply as an implicit Email/set after this response.
	emailUpdate := map[string]any{}
	var emailDestroy []string
	for key, patch := range onUpdate {
		if eid, ok := createdEmail[key]; ok {
			emailUpdate[eid] = patch
		}
	}
	for _, key := range onDestroy {
		if eid, ok := createdEmail[key]; ok {
			emailDestroy = append(emailDestroy, eid)
		}
	}
	if len(emailUpdate) > 0 || len(emailDestroy) > 0 {
		setArgs := map[string]any{"accountId": mb.String()}
		if len(emailUpdate) > 0 {
			setArgs["update"] = emailUpdate
		}
		if len(emailDestroy) > 0 {
			arr := make([]any, 0, len(emailDestroy))
			for _, d := range emailDestroy {
				arr = append(arr, d)
			}
			setArgs["destroy"] = arr
		}
		res, merr := s.emailSet(ctx, st, setArgs)
		if merr != nil {
			st.after = append(st.after, invocation{name: "error", args: merr.toMap()})
		} else {
			st.after = append(st.after, invocation{name: "Email/set", args: res})
		}
	}
	return result, nil
}
