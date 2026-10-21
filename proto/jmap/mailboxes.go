package jmap

import (
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"

	"dovetail/store/index"
)

// mailboxObject converts a folder to a JMAP Mailbox object.
func mailboxObject(root *index.Root, f *index.Folder, readOnly bool) map[string]any {
	name := f.Name
	var parentID any
	if i := strings.LastIndex(f.Name, index.Delimiter); i > 0 {
		name = f.Name[i+1:]
		if p := root.FolderByName(f.Name[:i]); p != nil {
			parentID = p.ID
		}
	}
	var role any
	if f.Role != "" {
		role = f.Role
	}
	isInbox := f.Role == index.RoleInbox
	return map[string]any{
		"id":            f.ID,
		"name":          name,
		"parentId":      parentID,
		"role":          role,
		"sortOrder":     0,
		"totalEmails":   f.Total,
		"unreadEmails":  f.Unseen,
		"totalThreads":  f.Total,
		"unreadThreads": f.Unseen,
		"myRights": map[string]any{
			"mayReadItems":   true,
			"mayAddItems":    !readOnly,
			"mayRemoveItems": !readOnly,
			"maySetSeen":     !readOnly,
			"maySetKeywords": !readOnly,
			"mayCreateChild": !readOnly,
			"mayRename":      !readOnly && !isInbox,
			"mayDelete":      !readOnly && f.Role == "",
			"maySubmit":      true,
		},
		"isSubscribed": f.Subscribed,
	}
}

func (s *Server) readOnly(ctx context.Context, mb uuid.UUID) bool {
	return s.cfg.HoldCheck != nil && s.cfg.HoldCheck(ctx, mb)
}

func (s *Server) mailboxGet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	g, merr := parseGetArgs(args)
	if merr != nil {
		return nil, merr
	}
	if len(g.ids) > s.cfg.MaxObjectsInGet {
		return nil, &methodError{Type: "requestTooLarge"}
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	ro := s.readOnly(ctx, mb)
	list := []any{}
	notFound := []string{}
	if g.allIDs {
		for _, f := range root.Folders {
			list = append(list, filterProps(mailboxObject(&root, f, ro), g.properties))
		}
	} else {
		for _, id := range g.ids {
			id = st.resolveCreatedID(id)
			if f := root.FolderByID(id); f != nil {
				list = append(list, filterProps(mailboxObject(&root, f, ro), g.properties))
			} else {
				notFound = append(notFound, id)
			}
		}
	}
	return map[string]any{
		"accountId": mb.String(),
		"state":     stateString(root.ModSeq),
		"list":      list,
		"notFound":  notFound,
	}, nil
}

func filterProps(obj map[string]any, props map[string]bool) map[string]any {
	if props == nil {
		return obj
	}
	out := map[string]any{}
	for k, v := range obj {
		if props[k] {
			out[k] = v
		}
	}
	return out
}

func (s *Server) mailboxChanges(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
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
	created, updated, destroyed, newState, more, merr := s.changes(ctx, mb, since, maxChanges, index.KindFolder)
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

// changes folds the change log into created/updated/destroyed id lists for
// one kind. The log is read in pages until maxChanges distinct ids were
// collected (or the log ends).
func (s *Server) changes(ctx context.Context, mb uuid.UUID, since uint64, maxChanges int, kind string) (created, updated, destroyed []string, newState string, more bool, merr *methodError) {
	createdSet := map[string]bool{}
	updatedSet := map[string]bool{}
	destroyedSet := map[string]bool{}
	var order []string
	cursor := since
	var current uint64
	for {
		page, pageMore, cur, err := s.mb.Changes(ctx, mb, cursor, 500)
		if err != nil {
			return nil, nil, nil, "", false, mapErr(err)
		}
		current = cur
		for _, c := range page {
			cursor = c.ModSeq
			if c.Kind != kind {
				continue
			}
			if maxChanges > 0 && len(order) >= maxChanges && !createdSet[c.ID] && !updatedSet[c.ID] && !destroyedSet[c.ID] {
				more = true
				cursor = c.ModSeq - 1
				goto done
			}
			if !createdSet[c.ID] && !updatedSet[c.ID] && !destroyedSet[c.ID] {
				order = append(order, c.ID)
			}
			switch c.Op {
			case index.OpCreated:
				createdSet[c.ID] = true
				delete(destroyedSet, c.ID)
			case index.OpUpdated:
				if !createdSet[c.ID] {
					updatedSet[c.ID] = true
				}
			case index.OpDestroyed:
				if createdSet[c.ID] {
					// Created and destroyed within the window: omit entirely.
					delete(createdSet, c.ID)
					delete(updatedSet, c.ID)
					continue
				}
				delete(updatedSet, c.ID)
				destroyedSet[c.ID] = true
			}
		}
		if !pageMore {
			break
		}
	}
done:
	created, updated, destroyed = []string{}, []string{}, []string{}
	for _, id := range order {
		switch {
		case createdSet[id]:
			created = append(created, id)
		case destroyedSet[id]:
			destroyed = append(destroyed, id)
		case updatedSet[id]:
			updated = append(updated, id)
		}
	}
	if more {
		newState = stateString(cursor)
	} else {
		newState = stateString(current)
	}
	return created, updated, destroyed, newState, more, nil
}

func (s *Server) mailboxQuery(ctx context.Context, _ *requestState, args map[string]any) (map[string]any, *methodError) {
	mb, merr := s.accountFor(ctx, args)
	if merr != nil {
		return nil, merr
	}
	root, err := s.mb.Root(ctx, mb)
	if err != nil {
		return nil, mapErr(err)
	}
	filter, merr := argMap(args, "filter")
	if merr != nil {
		return nil, merr
	}
	if filter != nil && filter["operator"] != nil {
		return nil, &methodError{Type: "unsupportedFilter", Description: "filter operators are not supported"}
	}
	var folders []*index.Folder
	for _, f := range root.Folders {
		if matchMailbox(&root, f, filter) {
			folders = append(folders, f)
		}
	}
	// Sort.
	sortArg, _ := args["sort"].([]any)
	byName := false
	asc := true
	if len(sortArg) > 0 {
		if c, ok := sortArg[0].(map[string]any); ok {
			prop, _ := c["property"].(string)
			switch prop {
			case "name":
				byName = true
			case "sortOrder", "":
			default:
				return nil, &methodError{Type: "unsupportedSort"}
			}
			if a, ok := c["isAscending"].(bool); ok {
				asc = a
			}
		}
	}
	sort.SliceStable(folders, func(i, j int) bool {
		a, b := folders[i], folders[j]
		var less bool
		if byName {
			less = strings.ToLower(leafName(a.Name)) < strings.ToLower(leafName(b.Name))
		} else {
			less = a.Name < b.Name
		}
		if !asc {
			return !less && a.Name != b.Name
		}
		return less
	})
	position, merr := argInt(args, "position", 0)
	if merr != nil {
		return nil, merr
	}
	limit, merr := argInt(args, "limit", 0)
	if merr != nil {
		return nil, merr
	}
	total := len(folders)
	if position < 0 {
		position = max(0, total+position)
	}
	if position > total {
		position = total
	}
	end := total
	if limit > 0 && position+limit < end {
		end = position + limit
	}
	ids := []string{}
	for _, f := range folders[position:end] {
		ids = append(ids, f.ID)
	}
	res := map[string]any{
		"accountId":           mb.String(),
		"queryState":          stateString(root.ModSeq),
		"canCalculateChanges": false,
		"position":            position,
		"ids":                 ids,
	}
	if argBool(args, "calculateTotal", false) {
		res["total"] = total
	}
	if limit > 0 {
		res["limit"] = limit
	}
	return res, nil
}

func leafName(name string) string {
	if i := strings.LastIndex(name, index.Delimiter); i >= 0 {
		return name[i+1:]
	}
	return name
}

func matchMailbox(root *index.Root, f *index.Folder, filter map[string]any) bool {
	if filter == nil {
		return true
	}
	if v, ok := filter["parentId"]; ok {
		var parent any
		if i := strings.LastIndex(f.Name, index.Delimiter); i > 0 {
			if p := root.FolderByName(f.Name[:i]); p != nil {
				parent = p.ID
			}
		}
		if v == nil {
			if parent != nil {
				return false
			}
		} else if ps, _ := v.(string); parent != ps {
			return false
		}
	}
	if v, ok := filter["name"].(string); ok && !strings.Contains(strings.ToLower(leafName(f.Name)), strings.ToLower(v)) {
		return false
	}
	if v, ok := filter["role"]; ok {
		if v == nil {
			if f.Role != "" {
				return false
			}
		} else if rs, _ := v.(string); rs != f.Role {
			return false
		}
	}
	if v, ok := filter["hasAnyRole"].(bool); ok && v != (f.Role != "") {
		return false
	}
	if v, ok := filter["isSubscribed"].(bool); ok && v != f.Subscribed {
		return false
	}
	return true
}

func setError(typ, desc string, props ...string) map[string]any {
	m := map[string]any{"type": typ}
	if desc != "" {
		m["description"] = desc
	}
	if len(props) > 0 {
		m["properties"] = props
	}
	return m
}

func (s *Server) mailboxSet(ctx context.Context, st *requestState, args map[string]any) (map[string]any, *methodError) {
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
	ro := s.readOnly(ctx, mb)
	created, notCreated := map[string]any{}, map[string]any{}
	updated, notUpdated := map[string]any{}, map[string]any{}
	destroyed, notDestroyed := []string{}, map[string]any{}

	// Create in a deterministic order so "#id" parent references to earlier
	// creations in the same call work.
	cids := make([]string, 0, len(create))
	for cid := range create {
		cids = append(cids, cid)
	}
	sort.Strings(cids)
	for _, cid := range cids {
		obj, ok := create[cid].(map[string]any)
		if !ok {
			notCreated[cid] = setError("invalidProperties", "must be an object")
			continue
		}
		name, _ := obj["name"].(string)
		if strings.TrimSpace(name) == "" || strings.Contains(name, index.Delimiter) {
			notCreated[cid] = setError("invalidProperties", "invalid name", "name")
			continue
		}
		full := name
		if pid, ok := obj["parentId"].(string); ok && pid != "" {
			pid = st.resolveCreatedID(pid)
			cur, err := s.mb.Root(ctx, mb)
			if err != nil {
				return nil, mapErr(err)
			}
			parent := cur.FolderByID(pid)
			if parent == nil {
				notCreated[cid] = setError("invalidProperties", "parent not found", "parentId")
				continue
			}
			full = parent.Name + index.Delimiter + name
		}
		for k := range obj {
			switch k {
			case "name", "parentId", "isSubscribed", "role", "sortOrder":
			default:
				notCreated[cid] = setError("invalidProperties", "unknown or server-set property "+k, k)
			}
		}
		if _, bad := notCreated[cid]; bad {
			continue
		}
		f, err := s.mb.CreateFolder(ctx, mb, full)
		if err != nil {
			notCreated[cid] = mapErr(err).toMap()
			continue
		}
		if sub, ok := obj["isSubscribed"].(bool); ok && !sub {
			_ = s.mb.SetSubscribed(ctx, mb, f.ID, false)
			f.Subscribed = false
		}
		st.createdIds[cid] = f.ID
		cur, _ := s.mb.Root(ctx, mb)
		out := mailboxObject(&cur, f, ro)
		delete(out, "name")
		delete(out, "parentId")
		delete(out, "isSubscribed")
		created[cid] = out
	}

	for id, patchAny := range update {
		patch, ok := patchAny.(map[string]any)
		if !ok {
			notUpdated[id] = setError("invalidProperties", "must be an object")
			continue
		}
		cur, err := s.mb.Root(ctx, mb)
		if err != nil {
			return nil, mapErr(err)
		}
		f := cur.FolderByID(id)
		if f == nil {
			notUpdated[id] = setError("notFound", "")
			continue
		}
		newLeaf := leafName(f.Name)
		parentName := ""
		if i := strings.LastIndex(f.Name, index.Delimiter); i > 0 {
			parentName = f.Name[:i]
		}
		rename := false
		var subscribe *bool
		bad := false
		for k, v := range patch {
			switch k {
			case "name":
				n, ok := v.(string)
				if !ok || strings.TrimSpace(n) == "" || strings.Contains(n, index.Delimiter) {
					notUpdated[id] = setError("invalidProperties", "invalid name", "name")
					bad = true
				}
				newLeaf, rename = n, true
			case "parentId":
				if v == nil {
					parentName = ""
				} else {
					pid, _ := v.(string)
					pid = st.resolveCreatedID(pid)
					p := cur.FolderByID(pid)
					if p == nil {
						notUpdated[id] = setError("invalidProperties", "parent not found", "parentId")
						bad = true
					} else {
						parentName = p.Name
					}
				}
				rename = true
			case "isSubscribed":
				b, ok := v.(bool)
				if !ok {
					notUpdated[id] = setError("invalidProperties", "isSubscribed must be boolean", "isSubscribed")
					bad = true
				}
				subscribe = &b
			case "sortOrder":
			default:
				notUpdated[id] = setError("invalidProperties", "property is not settable: "+k, k)
				bad = true
			}
		}
		if bad {
			continue
		}
		if rename {
			full := newLeaf
			if parentName != "" {
				full = parentName + index.Delimiter + newLeaf
			}
			if full != f.Name {
				if err := s.mb.RenameFolder(ctx, mb, id, full); err != nil {
					notUpdated[id] = mapErr(err).toMap()
					continue
				}
			}
		}
		if subscribe != nil {
			if err := s.mb.SetSubscribed(ctx, mb, id, *subscribe); err != nil {
				notUpdated[id] = mapErr(err).toMap()
				continue
			}
		}
		updated[id] = nil
	}

	removeEmails := argBool(args, "onDestroyRemoveEmails", false)
	for _, id := range destroy {
		id = st.resolveCreatedID(id)
		if err := s.mb.DeleteFolder(ctx, mb, id, removeEmails); err != nil {
			notDestroyed[id] = mapErr(err).toMap()
			continue
		}
		destroyed = append(destroyed, id)
	}

	return map[string]any{
		"accountId":    mb.String(),
		"oldState":     oldState,
		"newState":     s.state(ctx, mb),
		"created":      created,
		"updated":      updated,
		"destroyed":    destroyed,
		"notCreated":   notCreated,
		"notUpdated":   notUpdated,
		"notDestroyed": notDestroyed,
	}, nil
}
