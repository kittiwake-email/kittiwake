package jmap

import (
	"context"
	"fmt"
	"net/mail"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	dmail "dovetail/mail"
	"dovetail/service/actor"
	"dovetail/service/mailbox"
	"dovetail/store/index"
)

// keyword <-> IMAP flag mapping.
var keywordToFlag = map[string]string{
	"$seen":     index.FlagSeen,
	"$flagged":  index.FlagFlagged,
	"$answered": index.FlagAnswered,
	"$draft":    index.FlagDraft,
}

func flagToKeyword(f string) string {
	for k, v := range keywordToFlag {
		if strings.EqualFold(v, f) {
			return k
		}
	}
	if strings.HasPrefix(f, `\`) {
		return "" // other system flags (\Deleted) are not JMAP keywords
	}
	return f
}

func keywordFlag(k string) string {
	if f, ok := keywordToFlag[strings.ToLower(k)]; ok {
		return f
	}
	return k
}

func keywordsOf(rec *index.Record) map[string]any {
	out := map[string]any{}
	for _, f := range rec.Flags {
		if k := flagToKeyword(f); k != "" {
			out[k] = true
		}
	}
	return out
}

func rfc3339(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func addrList(in []dmail.Address) any {
	if len(in) == 0 {
		return nil
	}
	out := make([]any, 0, len(in))
	for _, a := range in {
		out = append(out, addrObj(a))
	}
	return out
}

func addrObj(a dmail.Address) map[string]any {
	var name any
	if a.Name != "" {
		name = a.Name
	}
	return map[string]any{"name": name, "email": a.Email}
}

func strList(in []string) any {
	if len(in) == 0 {
		return nil
	}
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// partID returns the JMAP partId for a part.
func partID(p *dmail.Part) string {
	if p.Path == "" {
		return "1"
	}
	return p.Path
}

// blobIDForPart returns the blob id for a leaf part.
func blobIDForPart(msgID uuid.UUID, p *dmail.Part) string {
	if p.Attachment != "" {
		return "a-" + p.Attachment
	}
	return "p-" + msgID.String() + "-" + partID(p)
}

// bodyPart converts a MIME part to an EmailBodyPart.
func bodyPart(msgID uuid.UUID, p *dmail.Part) map[string]any {
	typ := strings.ToLower(p.Type + "/" + p.Subtype)
	obj := map[string]any{
		"partId":      nil,
		"blobId":      nil,
		"size":        p.Size,
		"name":        nil,
		"type":        typ,
		"charset":     nil,
		"disposition": nil,
		"cid":         nil,
		"language":    nil,
		"location":    nil,
	}
	if p.Filename != "" {
		obj["name"] = p.Filename
	}
	if cs, ok := p.Params["charset"]; ok && cs != "" {
		obj["charset"] = cs
	} else if p.Type == "text" {
		obj["charset"] = "us-ascii"
	}
	if p.Disposition != "" {
		obj["disposition"] = p.Disposition
	}
	if p.ContentID != "" {
		obj["cid"] = p.ContentID
	}
	if len(p.Children) > 0 {
		subs := make([]any, 0, len(p.Children))
		for _, c := range p.Children {
			subs = append(subs, bodyPart(msgID, c))
		}
		obj["subParts"] = subs
		return obj
	}
	obj["partId"] = partID(p)
	obj["blobId"] = blobIDForPart(msgID, p)
	return obj
}

// leaves collects leaf parts in order.
func leaves(p *dmail.Part, out *[]*dmail.Part) {
	if p == nil {
		return
	}
	if len(p.Children) == 0 {
		*out = append(*out, p)
		return
	}
	for _, c := range p.Children {
		leaves(c, out)
	}
}

// rawHeaders parses the verbatim header block into (name, raw value) pairs.
func rawHeaders(block string) [][2]string {
	var out [][2]string
	lines := strings.Split(strings.ReplaceAll(block, "\r\n", "\n"), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		if (line[0] == ' ' || line[0] == '\t') && len(out) > 0 {
			out[len(out)-1][1] += "\n" + line
			continue
		}
		i := strings.Index(line, ":")
		if i <= 0 {
			continue
		}
		out = append(out, [2]string{line[:i], line[i+1:]})
	}
	return out
}

var msgIDRe = regexp.MustCompile(`<([^<>]+)>`)

// headerProperty evaluates a "header:Name[:asForm][:all]" property.
func headerProperty(prop string, doc *dmail.Document) (any, bool) {
	parts := strings.Split(prop, ":")
	if len(parts) < 2 || parts[0] != "header" || parts[1] == "" {
		return nil, false
	}
	name := parts[1]
	form := "asRaw"
	all := false
	for _, f := range parts[2:] {
		switch f {
		case "all":
			all = true
		case "asRaw", "asText", "asAddresses", "asGroupedAddresses", "asMessageIds", "asDate", "asURLs":
			form = f
		default:
			return nil, false
		}
	}
	var raws, decoded []string
	for _, h := range rawHeaders(doc.RawHeaders) {
		if strings.EqualFold(h[0], name) {
			raws = append(raws, h[1])
		}
	}
	for _, h := range doc.Headers {
		if strings.EqualFold(h.Name, name) {
			decoded = append(decoded, h.Value)
		}
	}
	conv := func(i int) any {
		raw, dec := "", ""
		if i < len(raws) {
			raw = raws[i]
		}
		if i < len(decoded) {
			dec = decoded[i]
		}
		switch form {
		case "asRaw":
			return raw
		case "asText":
			return strings.TrimSpace(dec)
		case "asAddresses", "asGroupedAddresses":
			list, err := mail.ParseAddressList(dec)
			if err != nil {
				return []any{}
			}
			out := make([]any, 0, len(list))
			for _, a := range list {
				out = append(out, addrObj(dmail.Address{Name: a.Name, Email: a.Address}))
			}
			if form == "asGroupedAddresses" {
				return []any{map[string]any{"name": nil, "addresses": out}}
			}
			return out
		case "asMessageIds":
			m := msgIDRe.FindAllStringSubmatch(dec, -1)
			if len(m) == 0 {
				return nil
			}
			out := make([]any, 0, len(m))
			for _, x := range m {
				out = append(out, x[1])
			}
			return out
		case "asDate":
			t, err := mail.ParseDate(strings.TrimSpace(dec))
			if err != nil {
				return nil
			}
			return rfc3339(t)
		case "asURLs":
			var out []any
			for _, u := range strings.Split(dec, ",") {
				u = strings.Trim(strings.TrimSpace(u), "<>")
				if u != "" {
					out = append(out, u)
				}
			}
			if out == nil {
				return nil
			}
			return out
		}
		return nil
	}
	n := len(raws)
	if len(decoded) > n {
		n = len(decoded)
	}
	if all {
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, conv(i))
		}
		return out, true
	}
	if n == 0 {
		return nil, true
	}
	return conv(n - 1), true
}

// emailObject builds the Email object for a record.
func (s *Server) emailObject(ctx context.Context, mb uuid.UUID, loc index.Location, rec index.Record, g getArgs, bodyOpts bodyOptions) (map[string]any, error) {
	id := rec.MessageID.String()
	obj := map[string]any{
		"id":         id,
		"blobId":     "m-" + id,
		"threadId":   rec.ThreadID,
		"mailboxIds": map[string]any{loc.FolderID: true},
		"keywords":   keywordsOf(&rec),
		"size":       rec.Size,
		"receivedAt": rfc3339(rec.InternalDate),
	}
	needDoc := g.properties == nil
	if !needDoc {
		for p := range g.properties {
			switch p {
			case "id", "blobId", "threadId", "mailboxIds", "keywords", "size", "receivedAt":
			default:
				needDoc = true
			}
		}
	}
	if !needDoc {
		return filterProps(obj, g.properties), nil
	}
	doc, err := s.mb.Document(ctx, mb, rec.MessageID)
	if err != nil {
		return nil, err
	}
	hdrs := make([]any, 0, len(doc.Headers))
	for _, h := range rawHeaders(doc.RawHeaders) {
		hdrs = append(hdrs, map[string]any{"name": h[0], "value": h[1]})
	}
	obj["headers"] = hdrs
	obj["messageId"] = nil
	if doc.MessageID != "" {
		obj["messageId"] = []any{doc.MessageID}
	}
	obj["inReplyTo"] = strList(doc.InReplyTo)
	obj["references"] = strList(doc.References)
	obj["sender"] = nil
	if doc.Sender != nil {
		obj["sender"] = []any{addrObj(*doc.Sender)}
	}
	obj["from"] = addrList(doc.From)
	obj["to"] = addrList(doc.To)
	obj["cc"] = addrList(doc.Cc)
	obj["bcc"] = addrList(doc.Bcc)
	obj["replyTo"] = addrList(doc.ReplyTo)
	obj["subject"] = doc.Subject
	obj["sentAt"] = rfc3339(doc.Date)
	obj["hasAttachment"] = doc.HasAttachments
	obj["preview"] = doc.Preview
	if doc.Root != nil {
		obj["bodyStructure"] = bodyPart(rec.MessageID, doc.Root)
		var lv []*dmail.Part
		leaves(doc.Root, &lv)
		textBody, htmlBody, attachments := []any{}, []any{}, []any{}
		bodyValues := map[string]any{}
		for _, p := range lv {
			bp := bodyPart(rec.MessageID, p)
			typ := strings.ToLower(p.Type + "/" + p.Subtype)
			switch {
			case p.IsBody && typ == "text/plain":
				textBody = append(textBody, bp)
				if bodyOpts.text || bodyOpts.all {
					bodyValues[partID(p)] = bodyValue(doc.TextBody, bodyOpts.max)
				}
			case p.IsBody && typ == "text/html":
				htmlBody = append(htmlBody, bp)
				if bodyOpts.html || bodyOpts.all {
					bodyValues[partID(p)] = bodyValue(doc.HTMLBody, bodyOpts.max)
				}
			default:
				attachments = append(attachments, bp)
			}
		}
		// RFC 8621: if there is no text/plain body but an html one, textBody
		// is the html part (clients convert) — expose the derived text instead.
		if len(textBody) == 0 && len(htmlBody) > 0 {
			textBody = htmlBody
		}
		obj["textBody"] = textBody
		obj["htmlBody"] = htmlBody
		obj["attachments"] = attachments
		obj["bodyValues"] = bodyValues
	}
	for _, hp := range g.headerProps() {
		if v, ok := headerProperty(hp, doc); ok {
			obj[hp] = v
		}
	}
	if g.properties == nil {
		return obj, nil
	}
	out := map[string]any{}
	for k, v := range obj {
		if g.properties[k] {
			out[k] = v
		}
	}
	return out, nil
}

type bodyOptions struct {
	text, html, all bool
	max             int
}

func bodyValue(v string, max int) map[string]any {
	truncated := false
	if max > 0 && len(v) > max {
		v = v[:max]
		for len(v) > 0 && !utf8Valid(v) {
			v = v[:len(v)-1]
		}
		truncated = true
	}
	return map[string]any{"value": v, "isEncodingProblem": false, "isTruncated": truncated}
}

func utf8Valid(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}
	return true
}

func (s *Server) emailGet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	if g.allIDs {
		return nil, &methodError{Type: "requestTooLarge", Description: "Email/get requires ids"}
	}
	if len(g.ids) > s.cfg.MaxObjectsInGet {
		return nil, &methodError{Type: "requestTooLarge"}
	}
	bo := bodyOptions{
		text: argBool(args, "fetchTextBodyValues", false),
		html: argBool(args, "fetchHTMLBodyValues", false),
		all:  argBool(args, "fetchAllBodyValues", false),
	}
	bo.max, merr = argInt(args, "maxBodyValueBytes", 0)
	if merr != nil {
		return nil, merr
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	list := []any{}
	notFound := []string{}
	for _, idStr := range g.ids {
		idStr = st.resolveCreatedID(idStr)
		id, err := uuid.Parse(idStr)
		if err != nil {
			notFound = append(notFound, idStr)
			continue
		}
		loc, rec, err := s.mb.Get(ctx, mb, id)
		if err != nil {
			if me := mapErr(err); me.Type == "notFound" {
				notFound = append(notFound, idStr)
				continue
			} else {
				return nil, me
			}
		}
		obj, err := s.emailObject(ctx, mb, loc, rec, g, bo)
		if err != nil {
			return nil, mapErr(err)
		}
		list = append(list, obj)
	}
	return map[string]any{
		"accountId": mb.String(),
		"state":     stateString(root.ModSeq),
		"list":      list,
		"notFound":  notFound,
	}, nil
}

func (s *Server) emailChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	sinceStr, ok := argString(args, "sinceState")
	if !ok {
		return nil, invalidArguments("sinceState is required")
	}
	since, merr := parseState(sinceStr)
	if merr != nil {
		return nil, merr
	}
	maxChanges, merr := argInt(args, "maxChanges", 0)
	if merr != nil {
		return nil, merr
	}
	created, updated, destroyed, newState, more, merr := s.changes(ctx, mb, since, maxChanges, index.KindEmail)
	if merr != nil {
		return nil, merr
	}
	return map[string]any{
		"accountId":      mb.String(),
		"oldState":       sinceStr,
		"newState":       newState,
		"hasMoreChanges": more,
		"created":        created,
		"updated":        updated,
		"destroyed":      destroyed,
	}, nil
}

// translateFilter converts a JMAP Email filter into a mailbox.Filter.
func translateFilter(f map[string]any) (mailbox.Filter, *methodError) {
	var out mailbox.Filter
	if f == nil {
		return out, nil
	}
	if op, ok := f["operator"].(string); ok {
		if strings.ToUpper(op) != "AND" {
			return out, &methodError{Type: "unsupportedFilter", Description: "only AND is supported"}
		}
		conds, _ := f["conditions"].([]any)
		for _, c := range conds {
			cm, ok := c.(map[string]any)
			if !ok {
				return out, invalidArguments("conditions must be objects")
			}
			sub, merr := translateFilter(cm)
			if merr != nil {
				return out, merr
			}
			out = mergeFilter(out, sub)
		}
		return out, nil
	}
	for k, v := range f {
		switch k {
		case "inMailbox":
			out.InFolder, _ = v.(string)
		case "text":
			out.Text, _ = v.(string)
		case "subject":
			out.Subject, _ = v.(string)
		case "from":
			out.From, _ = v.(string)
		case "hasKeyword":
			kw, _ := v.(string)
			out.HasKeyword = keywordFlag(kw)
		case "notKeyword":
			kw, _ := v.(string)
			out.NotKeyword = keywordFlag(kw)
		case "before", "after":
			ts, _ := v.(string)
			t, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				return out, invalidArguments(k + " must be an RFC 3339 date")
			}
			if k == "before" {
				out.Before = t
			} else {
				out.After = t
			}
		case "hasAttachment":
			b, ok := v.(bool)
			if !ok {
				return out, invalidArguments("hasAttachment must be boolean")
			}
			out.HasAttachment = &b
		case "minSize":
			n, _ := v.(float64)
			out.MinSize = int64(n)
		case "maxSize":
			n, _ := v.(float64)
			out.MaxSize = int64(n)
		default:
			return out, &methodError{Type: "unsupportedFilter", Description: "unsupported filter condition " + k}
		}
	}
	return out, nil
}

func mergeFilter(a, b mailbox.Filter) mailbox.Filter {
	if b.InFolder != "" {
		a.InFolder = b.InFolder
	}
	if b.Text != "" {
		a.Text = b.Text
	}
	if b.Subject != "" {
		a.Subject = b.Subject
	}
	if b.From != "" {
		a.From = b.From
	}
	if b.HasKeyword != "" {
		a.HasKeyword = b.HasKeyword
	}
	if b.NotKeyword != "" {
		a.NotKeyword = b.NotKeyword
	}
	if !b.Before.IsZero() {
		a.Before = b.Before
	}
	if !b.After.IsZero() {
		a.After = b.After
	}
	if b.HasAttachment != nil {
		a.HasAttachment = b.HasAttachment
	}
	if b.MinSize > 0 {
		a.MinSize = b.MinSize
	}
	if b.MaxSize > 0 {
		a.MaxSize = b.MaxSize
	}
	return a
}

func (s *Server) emailQuery(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	fm, merr := argMap(args, "filter")
	if merr != nil {
		return nil, merr
	}
	filter, merr := translateFilter(fm)
	if merr != nil {
		return nil, merr
	}
	q := mailbox.Query{Filter: filter, Sort: mailbox.SortReceivedAt, CollapseThreads: argBool(args, "collapseThreads", false)}
	if sortArg, ok := args["sort"].([]any); ok && len(sortArg) > 0 {
		c, ok := sortArg[0].(map[string]any)
		if !ok {
			return nil, invalidArguments("sort must contain comparators")
		}
		prop, _ := c["property"].(string)
		switch prop {
		case "receivedAt", "sentAt", "subject", "from", "size":
			q.Sort = prop
		default:
			return nil, &methodError{Type: "unsupportedSort", Description: "unsupported sort property " + prop}
		}
		q.Asc = argBool(c, "isAscending", true)
	}
	if q.Position, merr = argInt(args, "position", 0); merr != nil {
		return nil, merr
	}
	if q.Limit, merr = argInt(args, "limit", 0); merr != nil {
		return nil, merr
	}
	if _, ok := args["anchor"]; ok {
		return nil, &methodError{Type: "anchorNotFound", Description: "anchors are not supported"}
	}
	res, err := s.mb.Query(ctx, mb, q)
	if err != nil {
		return nil, mapErr(err)
	}
	ids := make([]string, 0, len(res.Records))
	for _, r := range res.Records {
		ids = append(ids, r.MessageID.String())
	}
	pos := q.Position
	if pos < 0 {
		pos = max(0, res.Total+pos)
	}
	if pos > res.Total {
		pos = res.Total
	}
	out := map[string]any{
		"accountId":           mb.String(),
		"queryState":          stateString(res.ModSeq),
		"canCalculateChanges": false,
		"position":            pos,
		"ids":                 ids,
	}
	if argBool(args, "calculateTotal", false) {
		out["total"] = res.Total
	}
	if q.Limit > 0 {
		out["limit"] = q.Limit
	}
	return out, nil
}

func (s *Server) emailQueryChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	if _, merr := s.accountFor(ctx, args); merr != nil {
		return nil, merr
	}
	return nil, &methodError{Type: "cannotCalculateChanges"}
}

func (s *Server) threadChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	if _, merr := s.accountFor(ctx, args); merr != nil {
		return nil, merr
	}
	return nil, &methodError{Type: "cannotCalculateChanges"}
}

// emailSet handles keyword and mailbox updates and destruction.
func (s *Server) emailSet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	oldState := stateString(root.ModSeq)
	if ifIn, ok := argString(args, "ifInState"); ok && ifIn != oldState {
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
	notCreated := map[string]any{}
	for cid := range create {
		notCreated[cid] = setError("invalidProperties", "Email/set create is not supported; upload the message and use Email/import")
	}
	updated, notUpdated := map[string]any{}, map[string]any{}
	for idStr, patchAny := range update {
		idStr = st.resolveCreatedID(idStr)
		patch, ok := patchAny.(map[string]any)
		if !ok {
			notUpdated[idStr] = setError("invalidProperties", "must be an object")
			continue
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			notUpdated[idStr] = setError("notFound", "")
			continue
		}
		if se := s.applyEmailPatch(ctx, st, mb, id, patch); se != nil {
			notUpdated[idStr] = se
			continue
		}
		updated[idStr] = nil
	}
	destroyed, notDestroyed := []string{}, map[string]any{}
	for _, idStr := range destroy {
		idStr = st.resolveCreatedID(idStr)
		id, err := uuid.Parse(idStr)
		if err != nil {
			notDestroyed[idStr] = setError("notFound", "")
			continue
		}
		if err := s.mb.Destroy(ctx, mb, id); err != nil {
			notDestroyed[idStr] = mapErr(err).toMap()
			continue
		}
		destroyed = append(destroyed, idStr)
	}
	return map[string]any{
		"accountId":    mb.String(),
		"oldState":     oldState,
		"newState":     s.state(ctx, mb),
		"created":      map[string]any{},
		"updated":      updated,
		"destroyed":    destroyed,
		"notCreated":   notCreated,
		"notUpdated":   notUpdated,
		"notDestroyed": notDestroyed,
	}, nil
}

// applyEmailPatch applies a PatchObject to one email.
func (s *Server) applyEmailPatch(ctx context.Context, st *requestState, mb uuid.UUID, id uuid.UUID, patch map[string]any) map[string]any {
	var flagOp *mailbox.FlagOp
	var moveTo string
	mailboxPatch := map[string]bool{} // folder id -> present
	mailboxPatched := false
	for k, v := range patch {
		switch {
		case k == "keywords":
			m, ok := v.(map[string]any)
			if !ok {
				return setError("invalidPatch", "keywords must be an object", "keywords")
			}
			if flagOp == nil {
				flagOp = &mailbox.FlagOp{}
			}
			flagOp.Replace = []string{}
			for kw, val := range m {
				if b, ok := val.(bool); !ok || !b {
					return setError("invalidPatch", "keyword values must be true", "keywords")
				}
				flagOp.Replace = append(flagOp.Replace, keywordFlag(kw))
			}
		case strings.HasPrefix(k, "keywords/"):
			kw := k[len("keywords/"):]
			if flagOp == nil {
				flagOp = &mailbox.FlagOp{}
			}
			if v == nil || v == false {
				flagOp.Remove = append(flagOp.Remove, keywordFlag(kw))
			} else {
				flagOp.Add = append(flagOp.Add, keywordFlag(kw))
			}
		case k == "mailboxIds":
			m, ok := v.(map[string]any)
			if !ok {
				return setError("invalidPatch", "mailboxIds must be an object", "mailboxIds")
			}
			mailboxPatched = true
			mailboxPatch = map[string]bool{}
			for fid, val := range m {
				if b, ok := val.(bool); ok && b {
					mailboxPatch[st.resolveCreatedID(fid)] = true
				}
			}
		case strings.HasPrefix(k, "mailboxIds/"):
			fid := st.resolveCreatedID(k[len("mailboxIds/"):])
			mailboxPatched = true
			if v == nil || v == false {
				mailboxPatch[fid] = false
			} else {
				mailboxPatch[fid] = true
			}
		default:
			return setError("invalidProperties", "property is not settable: "+k, k)
		}
	}
	if mailboxPatched {
		loc, _, err := s.mb.Get(ctx, mb, id)
		if err != nil {
			return mapErr(err).toMap()
		}
		// Compute the resulting set: start from the current folder, apply.
		current := map[string]bool{loc.FolderID: true}
		if _, full := patch["mailboxIds"]; full {
			current = map[string]bool{}
		}
		for fid, present := range mailboxPatch {
			if present {
				current[fid] = true
			} else {
				delete(current, fid)
			}
		}
		if len(current) != 1 {
			return setError("invalidProperties", "an email must be in exactly one mailbox", "mailboxIds")
		}
		for fid := range current {
			moveTo = fid
		}
		if moveTo != loc.FolderID {
			if _, err := s.mb.Move(ctx, mb, id, moveTo); err != nil {
				return mapErr(err).toMap()
			}
		}
	}
	if flagOp != nil {
		if _, err := s.mb.SetFlags(ctx, mb, id, *flagOp); err != nil {
			return mapErr(err).toMap()
		}
	}
	return nil
}

func (s *Server) emailImport(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	oldState := stateString(root.ModSeq)
	if ifIn, ok := argString(args, "ifInState"); ok && ifIn != oldState {
		return nil, &methodError{Type: "stateMismatch"}
	}
	emails, merr := argMap(args, "emails")
	if merr != nil {
		return nil, merr
	}
	if emails == nil {
		return nil, invalidArguments("emails is required")
	}
	if len(emails) > s.cfg.MaxObjectsInSet {
		return nil, &methodError{Type: "requestTooLarge"}
	}
	created, notCreated := map[string]any{}, map[string]any{}
	for cid, objAny := range emails {
		obj, ok := objAny.(map[string]any)
		if !ok {
			notCreated[cid] = setError("invalidProperties", "must be an object")
			continue
		}
		blobID, _ := obj["blobId"].(string)
		blob, ok := s.uploads.get(blobID)
		if !ok {
			notCreated[cid] = setError("blobNotFound", "")
			continue
		}
		mbIDs, _ := obj["mailboxIds"].(map[string]any)
		var folderID string
		n := 0
		for fid, v := range mbIDs {
			if b, ok := v.(bool); ok && b {
				folderID = st.resolveCreatedID(fid)
				n++
			}
		}
		if n != 1 {
			notCreated[cid] = setError("invalidProperties", "mailboxIds must contain exactly one mailbox", "mailboxIds")
			continue
		}
		var flags []string
		if kws, ok := obj["keywords"].(map[string]any); ok {
			for kw, v := range kws {
				if b, ok := v.(bool); ok && b {
					flags = append(flags, keywordFlag(kw))
				}
			}
		}
		var receivedAt time.Time
		if ra, ok := obj["receivedAt"].(string); ok {
			t, err := time.Parse(time.RFC3339, ra)
			if err != nil {
				notCreated[cid] = setError("invalidProperties", "receivedAt must be RFC 3339", "receivedAt")
				continue
			}
			receivedAt = t
		}
		d, err := s.del.Import(ctx, mb, folderID, blob.data, flags, receivedAt)
		if err != nil {
			notCreated[cid] = mapErr(err).toMap()
			continue
		}
		_, rec, err := s.mb.Get(ctx, mb, d.MessageID)
		if err != nil {
			notCreated[cid] = mapErr(err).toMap()
			continue
		}
		st.createdIds[cid] = d.MessageID.String()
		created[cid] = map[string]any{
			"id":       d.MessageID.String(),
			"blobId":   "m-" + d.MessageID.String(),
			"threadId": rec.ThreadID,
			"size":     rec.Size,
		}
	}
	return map[string]any{
		"accountId":  mb.String(),
		"oldState":   oldState,
		"newState":   s.state(ctx, mb),
		"created":    created,
		"notCreated": notCreated,
	}, nil
}

func (s *Server) threadGet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	if g.allIDs {
		return nil, &methodError{Type: "requestTooLarge", Description: "Thread/get requires ids"}
	}
	if len(g.ids) > s.cfg.MaxObjectsInGet {
		return nil, &methodError{Type: "requestTooLarge"}
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	list := []any{}
	notFound := []string{}
	for _, tid := range g.ids {
		tid = st.resolveCreatedID(tid)
		locs, err := s.mb.Thread(ctx, mb, tid)
		if err != nil {
			return nil, mapErr(err)
		}
		if len(locs) == 0 {
			notFound = append(notFound, tid)
			continue
		}
		var recs []index.Record
		for _, l := range locs {
			rec, err := s.mb.GetByUID(ctx, mb, l.FolderID, l.UID)
			if err != nil {
				continue
			}
			recs = append(recs, rec)
		}
		sort.SliceStable(recs, func(i, j int) bool { return recs[i].InternalDate.Before(recs[j].InternalDate) })
		ids := make([]string, 0, len(recs))
		for _, r := range recs {
			ids = append(ids, r.MessageID.String())
		}
		list = append(list, map[string]any{"id": tid, "emailIds": ids})
	}
	return map[string]any{
		"accountId": mb.String(),
		"state":     stateString(root.ModSeq),
		"list":      list,
		"notFound":  notFound,
	}, nil
}

func (s *Server) quotaGet(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	qi, err := s.mb.Quota(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	all := map[string]map[string]any{
		"size": {
			"id": "size", "resourceType": "octets", "used": qi.Usage.Bytes, "hardLimit": qi.Limits.HardBytes,
			"warnLimit": qi.Limits.SoftBytes, "scope": "account", "name": "Mailbox size", "description": "Storage used by this mailbox in bytes", "types": []string{"Email"},
		},
		"count": {
			"id": "count", "resourceType": "count", "used": qi.Usage.Messages, "hardLimit": qi.Limits.HardMessages,
			"warnLimit": qi.Limits.SoftMessages, "scope": "account", "name": "Message count", "description": "Number of messages in this mailbox", "types": []string{"Email"},
		},
	}
	list := []any{}
	notFound := []string{}
	ids := g.ids
	if g.allIDs {
		ids = []string{"size", "count"}
	}
	for _, id := range ids {
		if q, ok := all[id]; ok {
			list = append(list, filterProps(q, g.properties))
		} else {
			notFound = append(notFound, id)
		}
	}
	return map[string]any{
		"accountId": mb.String(),
		"state":     s.state(ctx, mb),
		"list":      list,
		"notFound":  notFound,
	}, nil
}

func (s *Server) identityGet(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	act := actor.From(ctx)
	ident := map[string]any{
		"id": "default", "name": act.Username, "email": act.Username, "replyTo": nil, "bcc": nil,
		"textSignature": "", "htmlSignature": "", "mayDelete": false,
	}
	list := []any{}
	notFound := []string{}
	if g.allIDs {
		list = append(list, filterProps(ident, g.properties))
	} else {
		for _, id := range g.ids {
			if id == "default" {
				list = append(list, filterProps(ident, g.properties))
			} else {
				notFound = append(notFound, id)
			}
		}
	}
	return map[string]any{"accountId": mb.String(), "state": "0", "list": list, "notFound": notFound}, nil
}

var _ = fmt.Sprintf
