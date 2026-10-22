package imap

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-sasl"
	"github.com/google/uuid"

	"kittiwake/metrics"
	"kittiwake/service/actor"
	authsvc "kittiwake/service/auth"
	"kittiwake/store/index"
)

const delim = '/'

// session is one IMAP connection.
type session struct {
	srv      *Server
	conn     *imapserver.Conn
	clientIP string
	ctx      context.Context // carries the actor after login
	act      actor.Actor
	mb       uuid.UUID
	sel      *selected
}

var (
	_ imapserver.Session            = (*session)(nil)
	_ imapserver.SessionNamespace   = (*session)(nil)
	_ imapserver.SessionMove        = (*session)(nil)
	_ imapserver.SessionSASL        = (*session)(nil)
	_ imapserver.SessionAppendLimit = (*session)(nil)
)

func (s *session) Close() error {
	metrics.Connections.WithLabelValues("imap").Dec()
	if s.sel != nil {
		s.sel.close()
		s.sel = nil
	}
	return nil
}

// ---- authentication ----

func (s *session) Login(username, password string) error {
	a, err := s.srv.auth.VerifyPassword(context.Background(), username, password, "imap", s.clientIP)
	if err != nil {
		return s.authErr(err)
	}
	return s.authenticated(a)
}

func (s *session) AuthenticateMechanisms() []string {
	return []string{saslPlain, saslOAuthBearer, saslXOAuth2}
}

func (s *session) Authenticate(mech string) (sasl.Server, error) {
	switch strings.ToUpper(mech) {
	case saslPlain:
		return sasl.NewPlainServer(func(identity, username, password string) error {
			if identity != "" && !strings.EqualFold(identity, username) {
				return noErr(imap.ResponseCodeAuthorizationFailed, "Proxy authentication is not supported")
			}
			return s.Login(username, password)
		}), nil
	case saslOAuthBearer:
		return sasl.NewOAuthBearerServer(func(opts sasl.OAuthBearerOptions) *sasl.OAuthBearerError {
			a, err := s.srv.auth.VerifyOAuthBearer(context.Background(), opts.Username, opts.Token, "imap", s.clientIP)
			if err != nil {
				return &sasl.OAuthBearerError{Status: "invalid_token", Schemes: "bearer"}
			}
			if err := s.authenticated(a); err != nil {
				return &sasl.OAuthBearerError{Status: "invalid_token", Schemes: "bearer"}
			}
			return nil
		}), nil
	case saslXOAuth2:
		return &xoauth2Server{auth: func(username, token string) error {
			a, err := s.srv.auth.VerifyOAuthBearer(context.Background(), username, token, "imap", s.clientIP)
			if err != nil {
				return s.authErr(err)
			}
			return s.authenticated(a)
		}}, nil
	}
	return nil, badErr("Unsupported authentication mechanism")
}

func (s *session) authErr(err error) error {
	switch {
	case errors.Is(err, authsvc.ErrTOTPRequired):
		return noErr(imap.ResponseCodeAuthenticationFailed, "This password requires a one-time code. Use an application password or OAUTHBEARER")
	case errors.Is(err, authsvc.ErrAccountDisabled):
		return noErr(imap.ResponseCodeAuthenticationFailed, "Account disabled")
	case errors.Is(err, authsvc.ErrServiceDisabled):
		return noErr(imap.ResponseCodeAuthenticationFailed, "IMAP is not enabled for this account")
	case errors.Is(err, authsvc.ErrInvalidCredentials), errors.Is(err, authsvc.ErrInvalidToken), errors.Is(err, authsvc.ErrTokenExpired):
		return imapserver.ErrAuthFailed
	}
	return s.srv.mapErr(err, "login")
}

// authenticated finishes login: binds the actor, checks the mailbox is usable.
func (s *session) authenticated(a actor.Actor) error {
	if a.MailboxID == uuid.Nil {
		return noErr(imap.ResponseCodeAuthorizationFailed, "Account has no mailbox")
	}
	ctx := actor.With(context.Background(), a)
	if _, err := s.srv.mb.Root(ctx, a.MailboxID); err != nil {
		return s.srv.mapErr(err, "login")
	}
	s.act, s.ctx, s.mb = a, ctx, a.MailboxID
	return nil
}

func (s *session) AppendLimit() uint32 {
	if s.srv.cfg.MaxLiteralSize > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(s.srv.cfg.MaxLiteralSize)
}

func (s *session) Namespace() (*imap.NamespaceData, error) {
	ns := &imap.NamespaceData{Personal: []imap.NamespaceDescriptor{{Prefix: "", Delim: delim}}}
	if s.srv.cfg.CompatPrefix != "" {
		ns.Personal = append(ns.Personal, imap.NamespaceDescriptor{Prefix: s.srv.cfg.CompatPrefix, Delim: rune(s.srv.cfg.CompatDelim[0])})
	}
	return ns, nil
}

// compatEnabled reports whether a client name or pattern addresses the
// compatibility namespace (it starts with the compat prefix).
func (s *session) inCompat(name string) bool {
	p := s.srv.cfg.CompatPrefix
	return p != "" && len(name) >= len(p) && strings.EqualFold(name[:len(p)], p)
}

// canonical turns a client mailbox name into the stored form. In the
// compatibility namespace the prefix goes, the compat delimiter becomes
// ours, and a compat-delimiter escaped as "\xx" becomes a literal.
func (s *session) canonical(name string) string {
	if s.inCompat(name) {
		rest := name[len(s.srv.cfg.CompatPrefix):]
		if rest == "" {
			return "INBOX"
		}
		cd := s.srv.cfg.CompatDelim
		parts := strings.Split(rest, cd)
		for i, p := range parts {
			parts[i] = index.EscapeComponent(index.UnescapeComponent(p))
		}
		return normalizeName(strings.Join(parts, string(delim)))
	}
	return normalizeName(name)
}

// compatName renders a stored name in the compatibility namespace: prefix
// plus the path with the compat delimiter, a literal compat delimiter in a
// component escaped as "\xx". INBOX stays INBOX; children of INBOX are not
// shown there (they would collide with top-level folders).
func (s *session) compatName(name string) (string, bool) {
	if name == "INBOX" {
		return "INBOX", true
	}
	if strings.HasPrefix(name, "INBOX"+string(delim)) {
		return "", false
	}
	cd := s.srv.cfg.CompatDelim
	parts := strings.Split(name, string(delim))
	for i, p := range parts {
		// p is the stored form ("/" and "\" already escaped); a literal
		// compat delimiter gets the same treatment.
		parts[i] = strings.ReplaceAll(p, cd, fmt.Sprintf("\\%02x", cd[0]))
	}
	return s.srv.cfg.CompatPrefix + strings.Join(parts, cd), true
}

// ---- folders ----

func normalizeName(name string) string {
	name = strings.TrimSuffix(name, string(delim))
	if strings.EqualFold(name, "INBOX") {
		return "INBOX"
	}
	if i := strings.Index(name, string(delim)); i > 0 && strings.EqualFold(name[:i], "INBOX") {
		return "INBOX" + name[i:]
	}
	return name
}

func (s *session) root() (index.Root, error) {
	root, err := s.srv.mb.Root(s.ctx, s.mb)
	if err != nil {
		return index.Root{}, s.srv.mapErr(err, "mailbox")
	}
	return root, nil
}

// folder resolves a mailbox name. It first consults the root as this
// process last saw it (no read); only a miss re-reads, so a stream of
// APPENDs costs no root reads for name resolution.
func (s *session) folder(name string) (index.Root, *index.Folder, error) {
	n := s.canonical(name)
	if root, err := s.srv.mb.RootCached(s.ctx, s.mb); err == nil {
		if f := root.FolderByName(n); f != nil {
			return root, f, nil
		}
	}
	root, err := s.root()
	if err != nil {
		return root, nil, err
	}
	f := root.FolderByName(n)
	if f == nil {
		return root, nil, noErr(imap.ResponseCodeNonExistent, "No such mailbox")
	}
	return root, f, nil
}

func roleAttr(role string) imap.MailboxAttr {
	switch role {
	case index.RoleDrafts:
		return imap.MailboxAttrDrafts
	case index.RoleSent:
		return imap.MailboxAttrSent
	case index.RoleJunk:
		return imap.MailboxAttrJunk
	case index.RoleTrash:
		return imap.MailboxAttrTrash
	case index.RoleArchive:
		return imap.MailboxAttrArchive
	}
	return ""
}

func hasChildren(root *index.Root, f *index.Folder) bool {
	for _, g := range root.Folders {
		if strings.HasPrefix(g.Name, f.Name+string(delim)) {
			return true
		}
	}
	return false
}

func (s *session) List(w *imapserver.ListWriter, ref string, patterns []string, options *imap.ListOptions) error {
	root, err := s.root()
	if err != nil {
		return err
	}
	folders := append([]*index.Folder(nil), root.Folders...)
	sort.Slice(folders, func(i, j int) bool { return folders[i].Name < folders[j].Name })
	// A pattern under the compat prefix lists the compat view of the same
	// folders (hidden otherwise, like Dovecot's alias namespace).
	var plain, compat []string
	for _, p := range patterns {
		if s.inCompat(ref + p) {
			compat = append(compat, p)
		} else {
			plain = append(plain, p)
		}
	}
	for _, f := range folders {
		if options.SelectSubscribed && !f.Subscribed {
			continue
		}
		if options.SelectSpecialUse && f.Role == "" {
			continue
		}
		shown := f.Name
		listDelim := delim
		matched := len(patterns) == 0
		for _, p := range plain {
			if imapserver.MatchList(f.Name, delim, ref, p) {
				matched = true
				break
			}
		}
		if !matched && len(compat) > 0 {
			if cn, ok := s.compatName(f.Name); ok {
				for _, p := range compat {
					if imapserver.MatchList(cn, rune(s.srv.cfg.CompatDelim[0]), ref, p) {
						matched, shown, listDelim = true, cn, rune(s.srv.cfg.CompatDelim[0])
						break
					}
				}
			}
		}
		if !matched {
			continue
		}
		data := &imap.ListData{Delim: listDelim, Mailbox: shown}
		if hasChildren(&root, f) {
			data.Attrs = append(data.Attrs, imap.MailboxAttrHasChildren)
		} else {
			data.Attrs = append(data.Attrs, imap.MailboxAttrHasNoChildren)
		}
		if f.Subscribed && (options.ReturnSubscribed || options.SelectSubscribed) {
			data.Attrs = append(data.Attrs, imap.MailboxAttrSubscribed)
		}
		if a := roleAttr(f.Role); a != "" {
			data.Attrs = append(data.Attrs, a)
		}
		if options.ReturnStatus != nil {
			data.Status = statusData(f, options.ReturnStatus)
			data.Status.Mailbox = shown
		}
		if err := w.WriteList(data); err != nil {
			return err
		}
	}
	return nil
}

func statusData(f *index.Folder, o *imap.StatusOptions) *imap.StatusData {
	d := &imap.StatusData{Mailbox: f.Name}
	if o.NumMessages {
		n := uint32(f.Total)
		d.NumMessages = &n
	}
	if o.UIDNext {
		d.UIDNext = imap.UID(f.UIDNext)
	}
	if o.UIDValidity {
		d.UIDValidity = f.UIDValidity
	}
	if o.NumUnseen {
		n := uint32(f.Unseen)
		d.NumUnseen = &n
	}
	if o.NumRecent {
		n := uint32(0)
		d.NumRecent = &n
	}
	if o.Size {
		sz := f.Size
		d.Size = &sz
	}
	if o.HighestModSeq {
		d.HighestModSeq = f.ModSeq
	}
	return d
}

func (s *session) Status(name string, options *imap.StatusOptions) (*imap.StatusData, error) {
	_, f, err := s.folder(name)
	if err != nil {
		return nil, err
	}
	d := statusData(f, options)
	d.Mailbox = name // echoed in the form the client used (compat or not)
	return d, nil
}

func (s *session) Create(name string, options *imap.CreateOptions) error {
	name = s.canonical(name)
	if _, err := s.srv.mb.CreateFolder(s.ctx, s.mb, name); err != nil {
		if errors.Is(err, index.ErrExists) || strings.Contains(err.Error(), "already exists") {
			return noErr(imap.ResponseCodeAlreadyExists, "Mailbox already exists")
		}
		return s.srv.mapErr(err, "create")
	}
	return nil
}

func (s *session) Delete(name string) error {
	_, f, err := s.folder(name)
	if err != nil {
		return err
	}
	if s.sel != nil && s.sel.folderID == f.ID {
		return noErr(imap.ResponseCodeInUse, "Mailbox is selected")
	}
	if err := s.srv.mb.DeleteFolder(s.ctx, s.mb, f.ID, true); err != nil {
		return s.srv.mapErr(err, "delete")
	}
	return nil
}

func (s *session) Rename(name, newName string, _ *imap.RenameOptions) error {
	_, f, err := s.folder(name)
	if err != nil {
		return err
	}
	if err := s.srv.mb.RenameFolder(s.ctx, s.mb, f.ID, s.canonical(newName)); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return noErr(imap.ResponseCodeAlreadyExists, "Mailbox already exists")
		}
		return s.srv.mapErr(err, "rename")
	}
	return nil
}

func (s *session) Subscribe(name string) error   { return s.subscribe(name, true) }
func (s *session) Unsubscribe(name string) error { return s.subscribe(name, false) }

func (s *session) subscribe(name string, on bool) error {
	_, f, err := s.folder(name)
	if err != nil {
		return err
	}
	if err := s.srv.mb.SetSubscribed(s.ctx, s.mb, f.ID, on); err != nil {
		return s.srv.mapErr(err, "subscribe")
	}
	return nil
}

// ---- selected state ----

func (s *session) Select(name string, options *imap.SelectOptions) (*imap.SelectData, error) {
	_, f, err := s.folder(name)
	if err != nil {
		return nil, err
	}
	if s.sel != nil {
		s.sel.close()
		s.sel = nil
	}
	sel, err := newSelected(s, f, options.ReadOnly)
	if err != nil {
		return nil, err
	}
	s.sel = sel
	flags := []imap.Flag{imap.FlagSeen, imap.FlagAnswered, imap.FlagFlagged, imap.FlagDeleted, imap.FlagDraft}
	for _, k := range f.Keywords {
		flags = append(flags, imap.Flag(k))
	}
	data := &imap.SelectData{
		Flags: flags, NumMessages: uint32(len(sel.uids)), FirstUnseenSeqNum: sel.firstUnseen(),
		UIDNext: imap.UID(f.UIDNext), UIDValidity: f.UIDValidity, HighestModSeq: f.ModSeq,
	}
	if sel.readOnly {
		data.PermanentFlags = []imap.Flag{}
	} else {
		data.PermanentFlags = append(append([]imap.Flag(nil), flags...), imap.FlagWildcard)
	}
	return data, nil
}

func (s *session) Unselect() error {
	if s.sel != nil {
		s.sel.close()
		s.sel = nil
	}
	return nil
}

func (s *session) selectedView() (*selected, error) {
	if s.sel == nil {
		return nil, badErr("No mailbox selected")
	}
	return s.sel, nil
}

func (s *session) Expunge(w *imapserver.ExpungeWriter, uids *imap.UIDSet) error {
	sel, err := s.selectedView()
	if err != nil {
		return err
	}
	return sel.expunge(w, uids)
}

func (s *session) Search(kind imapserver.NumKind, criteria *imap.SearchCriteria, options *imap.SearchOptions) (*imap.SearchData, error) {
	sel, err := s.selectedView()
	if err != nil {
		return nil, err
	}
	return sel.search(kind, criteria, options)
}

func (s *session) Fetch(w *imapserver.FetchWriter, numSet imap.NumSet, options *imap.FetchOptions) error {
	sel, err := s.selectedView()
	if err != nil {
		return err
	}
	return sel.fetch(w, numSet, options)
}

func (s *session) Store(w *imapserver.FetchWriter, numSet imap.NumSet, flags *imap.StoreFlags, options *imap.StoreOptions) error {
	sel, err := s.selectedView()
	if err != nil {
		return err
	}
	return sel.store(w, numSet, flags)
}

func (s *session) Copy(numSet imap.NumSet, dest string) (*imap.CopyData, error) {
	sel, err := s.selectedView()
	if err != nil {
		return nil, err
	}
	_, f, err := s.folder(dest)
	if err != nil {
		if errors.Is(err, imapserver.ErrAuthFailed) {
			return nil, err
		}
		return nil, noErr(imap.ResponseCodeTryCreate, "No such mailbox")
	}
	return sel.copy(numSet, f)
}

func (s *session) Move(w *imapserver.MoveWriter, numSet imap.NumSet, dest string) error {
	sel, err := s.selectedView()
	if err != nil {
		return err
	}
	_, f, err := s.folder(dest)
	if err != nil {
		return noErr(imap.ResponseCodeTryCreate, "No such mailbox")
	}
	return sel.move(w, numSet, f)
}

func (s *session) Append(name string, r imap.LiteralReader, options *imap.AppendOptions) (*imap.AppendData, error) {
	_, f, err := s.folder(name)
	if err != nil {
		return nil, noErr(imap.ResponseCodeTryCreate, "No such mailbox")
	}
	if r.Size() > s.srv.cfg.MaxLiteralSize {
		return nil, noErr(imap.ResponseCodeTooBig, "Message too large")
	}
	raw := make([]byte, 0, r.Size())
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		raw = append(raw, buf[:n]...)
		if err != nil {
			break
		}
	}
	var flags []string
	for _, fl := range options.Flags {
		flags = append(flags, string(fl))
	}
	t := options.Time
	if t.IsZero() {
		t = time.Now()
	}
	d, err := s.srv.del.Import(s.ctx, s.mb, f.ID, raw, flags, t)
	if err != nil {
		return nil, s.srv.mapErr(err, "append")
	}
	if s.sel != nil && s.sel.folderID == f.ID {
		s.sel.noteAppended(d.Record)
	}
	return &imap.AppendData{UID: imap.UID(d.UID), UIDValidity: f.UIDValidity}, nil
}

func (s *session) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	if s.sel == nil {
		return nil
	}
	return s.sel.sync(w, allowExpunge)
}

func (s *session) Idle(w *imapserver.UpdateWriter, stop <-chan struct{}) error {
	if s.sel == nil {
		<-stop
		return nil
	}
	events, cancel, err := s.srv.mb.Watch(s.ctx, s.mb)
	if err != nil {
		// Fall back to polling only.
		events, cancel = nil, func() {}
	}
	defer cancel()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(s.srv.cfg.IdleTimeout)
	defer timeout.Stop()
	for {
		select {
		case <-stop:
			return nil
		case <-timeout.C:
			return nil
		case <-ticker.C:
			if err := s.sel.sync(w, true); err != nil {
				return err
			}
		case _, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if err := s.sel.sync(w, true); err != nil {
				return err
			}
		}
	}
}
