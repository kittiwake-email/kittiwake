package mail

import (
	"html"
	"strings"
	"unicode"
)

var blockTags = map[string]bool{
	"p": true, "div": true, "br": true, "li": true, "tr": true, "h1": true, "h2": true, "h3": true,
	"h4": true, "h5": true, "h6": true, "blockquote": true, "table": true, "ul": true, "ol": true,
	"pre": true, "hr": true, "section": true, "article": true, "header": true, "footer": true,
	"title": true, "td": false, "th": false,
}

// htmlToText strips tags and decodes entities, keeping paragraph breaks.
func htmlToText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	skipUntil := "" // closing tag name whose content is skipped
	for i < len(s) {
		c := s[i]
		if c == '<' {
			end := strings.IndexByte(s[i:], '>')
			if end < 0 {
				break
			}
			tag := s[i+1 : i+end]
			i += end + 1
			if strings.HasPrefix(tag, "!--") {
				// comment: find -->
				if j := strings.Index(s[i-end-1:], "-->"); j >= 0 {
					i = i - end - 1 + j + 3
				} else {
					break
				}
				continue
			}
			closing := strings.HasPrefix(tag, "/")
			name := strings.ToLower(strings.TrimPrefix(tag, "/"))
			if j := strings.IndexAny(name, " \t\r\n/"); j >= 0 {
				name = name[:j]
			}
			if skipUntil != "" {
				if closing && name == skipUntil {
					skipUntil = ""
				}
				continue
			}
			if !closing && (name == "script" || name == "style" || name == "head") {
				skipUntil = name
				continue
			}
			if name == "td" || name == "th" {
				b.WriteByte(' ')
			} else if blockTags[name] {
				b.WriteByte('\n')
			}
			continue
		}
		if skipUntil != "" {
			i++
			continue
		}
		b.WriteByte(c)
		i++
	}
	return collapseText(html.UnescapeString(b.String()))
}

// collapseText collapses runs of horizontal whitespace to one space and runs
// of blank lines to one blank line; trims the result.
func collapseText(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	space, newlines := false, 0
	for _, r := range s {
		switch {
		case r == '\n' || r == '\r':
			if r == '\r' {
				continue
			}
			newlines++
			space = false
		case unicode.IsSpace(r) || r == ' ':
			space = true
		default:
			if newlines > 0 {
				if out.Len() > 0 {
					out.WriteByte('\n')
				}
				newlines = 0
				space = false
			} else if space {
				if out.Len() > 0 {
					out.WriteByte(' ')
				}
				space = false
			}
			out.WriteRune(r)
		}
	}
	return out.String()
}

// preview returns the first n runes of text with all whitespace collapsed.
func preview(s string, n int) string {
	var b strings.Builder
	space := false
	count := 0
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
			count++
		}
		space = false
		b.WriteRune(r)
		count++
		if count >= n {
			break
		}
	}
	return b.String()
}
