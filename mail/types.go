// Package mail parses raw RFC 5322 messages into Kittiwake's JSON document,
// extracts attachments for storage-wide deduplication and renders a message
// back from the document (Render). The original bytes are not kept: the
// document holds the verbatim headers (Received trace, Message-ID, DKIM) and
// every part's content, and Render produces a deterministic equivalent.
//
// See README.md: "Message parsing", "Raw message", "Attachments".
package mail

import "time"

// Options controls parsing.
type Options struct {
	// InlineMax: parts with encoded body <= this many bytes stay inline
	// (not externalized). Default 4096 if 0.
	InlineMax int64
}

// DefaultInlineMax is the InlineMax used when Options.InlineMax is 0.
const DefaultInlineMax = 4096

// Address is a decoded e-mail address.
type Address struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email"`
}

// Header is one decoded header field.
type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"` // RFC 2047-decoded UTF-8, folding removed
}

// Part is one node of the MIME tree.
type Part struct {
	Path        string            `json:"path"`
	Type        string            `json:"type"`
	Subtype     string            `json:"subtype"`
	Params      map[string]string `json:"params,omitempty"`
	Disposition string            `json:"disposition,omitempty"`
	Filename    string            `json:"filename,omitempty"`
	ContentID   string            `json:"content_id,omitempty"`
	Encoding    string            `json:"encoding,omitempty"`
	Size        int64             `json:"size"`
	EncodedSize int64             `json:"encoded_size"`
	IsBody      bool              `json:"is_body"`
	Attachment  string            `json:"attachment,omitempty"` // sha256 of the content when externalized to the attachment store
	Data        []byte            `json:"data,omitempty"`       // decoded content of a small non-body leaf kept inline (base64 in JSON)
	Children    []*Part           `json:"children,omitempty"`
}

// Document is the parsed JSON representation of a message.
type Document struct {
	Version        int       `json:"version"`
	Size           int64     `json:"size"`          // size of the rendered message (served by IMAP/JMAP)
	OriginalSize   int64     `json:"original_size"` // size of the bytes received
	RawHeaders     string    `json:"raw_headers"`
	Headers        []Header  `json:"headers"`
	Subject        string    `json:"subject"`
	From           []Address `json:"from,omitempty"`
	Sender         *Address  `json:"sender,omitempty"`
	ReplyTo        []Address `json:"reply_to,omitempty"`
	To             []Address `json:"to,omitempty"`
	Cc             []Address `json:"cc,omitempty"`
	Bcc            []Address `json:"bcc,omitempty"`
	MessageID      string    `json:"message_id,omitempty"`
	InReplyTo      []string  `json:"in_reply_to,omitempty"`
	References     []string  `json:"references,omitempty"`
	Date           time.Time `json:"date"`
	TextBody       string    `json:"text_body"`
	HTMLBody       string    `json:"html_body,omitempty"`
	Preview        string    `json:"preview"`
	HasAttachments bool      `json:"has_attachments"`
	Root           *Part     `json:"root"`
}

// Attachment is an externalized part's decoded content.
type Attachment struct {
	Hash        string
	Data        []byte
	ContentType string
	Filename    string
	ContentID   string
	Disposition string
	PartPath    string
}

// Parsed is the result of Parse.
type Parsed struct {
	Doc         *Document
	Attachments []Attachment // every externalized part, deduplicated by Hash within the message
	// Signed reports a multipart/signed (PGP/MIME, S/MIME) message whose
	// bytes must be kept verbatim for the signature to verify.
	Signed bool
}
