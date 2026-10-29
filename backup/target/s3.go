package target

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// S3Config describes an S3-compatible endpoint. Only the generic subset of
// the API is used (put, get with ranges, head, delete, list v2, object
// lock retention), so any provider works.
type S3Config struct {
	// Endpoint URL, e.g. "https://s3.eu-central-1.amazonaws.com" or
	// "http://[2a14:9b80::3144]:9000" for a lab MinIO.
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	// PathStyle puts the bucket in the path (MinIO, RGW, most
	// self-hosted stores); off uses virtual-host addressing (AWS).
	PathStyle bool
	// ObjectLock enables retention on every put (the bucket must have
	// Object Lock enabled).
	ObjectLock bool
	// Client overrides the HTTP client (tests).
	Client *http.Client
}

// S3 is the S3 target.
type S3 struct {
	cfg    S3Config
	base   *url.URL
	client *http.Client
	Now    func() time.Time
}

// NewS3 validates the configuration and returns the target. It does not
// touch the network.
func NewS3(cfg S3Config) (*S3, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("backup target: s3 endpoint and bucket are required")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("backup target: s3 access_key and secret_key are required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("backup target: s3 endpoint %q must be an http(s) URL", cfg.Endpoint)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	c := cfg.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Minute}
	}
	return &S3{cfg: cfg, base: u, client: c, Now: time.Now}, nil
}

// S3Error is a non-2xx answer from the store.
type S3Error struct {
	Status  int
	Code    string
	Message string
}

func (e *S3Error) Error() string {
	return fmt.Sprintf("s3: %d %s: %s", e.Status, e.Code, e.Message)
}

func (s *S3) objectURL(key string) *url.URL {
	u := *s.base
	raw := strings.TrimSuffix(u.EscapedPath(), "/") + "/" + escapeKey(key)
	if s.cfg.PathStyle {
		raw = strings.TrimSuffix(u.EscapedPath(), "/") + "/" + s.cfg.Bucket + "/" + escapeKey(key)
	} else {
		u.Host = s.cfg.Bucket + "." + u.Host
	}
	setPath(&u, raw)
	return &u
}

// setPath installs an already-escaped path so that EscapedPath returns it
// verbatim (the signature is computed over exactly these bytes).
func setPath(u *url.URL, raw string) {
	u.RawPath = raw
	u.Path, _ = url.PathUnescape(raw)
}

func (s *S3) bucketURL() *url.URL {
	u := *s.base
	raw := strings.TrimSuffix(u.EscapedPath(), "/") + "/"
	if s.cfg.PathStyle {
		raw += s.cfg.Bucket + "/"
	} else {
		u.Host = s.cfg.Bucket + "." + u.Host
	}
	setPath(&u, raw)
	return &u
}

// escapeKey percent-encodes a key the way SigV4 canonicalises it: every
// byte except unreserved characters and "/" is encoded.
func escapeKey(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// sign adds the SigV4 authorisation to req. payloadHash is the hex SHA-256
// of the body (or UNSIGNED-PAYLOAD).
func (s *S3) sign(req *http.Request, payloadHash string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("host", req.URL.Host)

	names := make([]string, 0, len(req.Header))
	for k := range req.Header {
		lk := strings.ToLower(k)
		if lk == "host" || strings.HasPrefix(lk, "x-amz-") || lk == "range" || lk == "content-type" || lk == "content-md5" {
			names = append(names, lk)
		}
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, n := range names {
		v := req.Header.Get(n)
		if n == "host" {
			v = req.URL.Host
		}
		canonHeaders.WriteString(n + ":" + strings.Join(strings.Fields(v), " ") + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	// Canonical query: sorted, each name and value URI-encoded.
	q := req.URL.Query()
	qk := make([]string, 0, len(q))
	for k := range q {
		qk = append(qk, k)
	}
	sort.Strings(qk)
	var canonQuery []string
	for _, k := range qk {
		vs := q[k]
		sort.Strings(vs)
		for _, v := range vs {
			canonQuery = append(canonQuery, escapeQuery(k)+"="+escapeQuery(v))
		}
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{req.Method, path, strings.Join(canonQuery, "&"), canonHeaders.String(), signedHeaders, payloadHash}, "\n")
	scope := date + "/" + s.cfg.Region + "/s3/aws4_request"
	toSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hexSHA256([]byte(canonical))}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(s.cfg.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(toSign)))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.cfg.AccessKey, scope, signedHeaders, sig))
}

func escapeQuery(s string) string {
	return strings.ReplaceAll(escapeKey(s), "/", "%2F")
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// do sends a signed request, retrying transient failures. body, when not
// nil, must be re-readable (a bytes.Reader or a file) because retries
// resend it.
func (s *S3) do(ctx context.Context, method string, u *url.URL, headers map[string]string, body io.ReadSeeker, size int64, payloadHash string) (*http.Response, error) {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 500 * time.Millisecond):
			}
			if body != nil {
				if _, err := body.Seek(0, io.SeekStart); err != nil {
					return nil, err
				}
			}
		}
		var rb io.Reader
		if body != nil {
			rb = body
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), rb)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.ContentLength = size
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		s.sign(req, payloadHash, s.Now())
		req.Header.Del("host") // the transport sets it from the URL
		resp, err := s.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			last = s.errorFrom(resp)
			continue
		}
		return resp, nil
	}
	return nil, last
}

type s3ErrorBody struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (s *S3) errorFrom(resp *http.Response) error {
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e s3ErrorBody
	_ = xml.Unmarshal(b, &e)
	if resp.StatusCode == http.StatusNotFound || e.Code == "NoSuchKey" {
		return ErrNotFound
	}
	if e.Message == "" {
		e.Message = strings.TrimSpace(string(b))
	}
	return &S3Error{Status: resp.StatusCode, Code: e.Code, Message: e.Message}
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) error {
	// The payload is hashed into the signature, which needs a second pass
	// over it: seekable sources are hashed in place, others are buffered.
	var body io.ReadSeeker
	if rs, ok := r.(io.ReadSeeker); ok {
		body = rs
	} else {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
		size = int64(len(b))
	}
	h := sha256.New()
	n, err := io.Copy(h, body)
	if err != nil {
		return err
	}
	if size < 0 {
		size = n
	} else if n != size {
		return io.ErrUnexpectedEOF
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	headers := map[string]string{}
	if opts.ContentType != "" {
		headers["content-type"] = opts.ContentType
	}
	if s.cfg.ObjectLock && !opts.RetainUntil.IsZero() {
		headers["x-amz-object-lock-mode"] = "COMPLIANCE"
		headers["x-amz-object-lock-retain-until-date"] = opts.RetainUntil.UTC().Format(time.RFC3339)
	}
	resp, err := s.do(ctx, http.MethodPut, s.objectURL(key), headers, body, size, hex.EncodeToString(h.Sum(nil)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return s.errorFrom(resp)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	headers := map[string]string{}
	if offset > 0 || length >= 0 {
		if length < 0 {
			headers["range"] = fmt.Sprintf("bytes=%d-", offset)
		} else if length == 0 {
			return io.NopCloser(bytes.NewReader(nil)), nil
		} else {
			headers["range"] = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		}
	}
	resp, err := s.do(ctx, http.MethodGet, s.objectURL(key), headers, nil, 0, emptySHA256)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, s.errorFrom(resp)
	}
	return resp.Body, nil
}

func (s *S3) Head(ctx context.Context, key string) (Object, error) {
	resp, err := s.do(ctx, http.MethodHead, s.objectURL(key), nil, nil, 0, emptySHA256)
	if err != nil {
		return Object{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusNotFound {
			return Object{}, ErrNotFound
		}
		return Object{}, &S3Error{Status: resp.StatusCode, Code: http.StatusText(resp.StatusCode)}
	}
	o := Object{Key: key, ETag: strings.Trim(resp.Header.Get("ETag"), `"`)}
	o.Size, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		o.Modified = t.UTC()
	}
	return o, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, s.objectURL(key), nil, nil, 0, emptySHA256)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	err = s.errorFrom(resp)
	var se *S3Error
	if errors.As(err, &se) && se.Status == http.StatusForbidden && strings.Contains(strings.ToLower(se.Message), "lock") {
		return ErrLocked
	}
	return err
}

type listResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
	} `xml:"Contents"`
}

func (s *S3) List(ctx context.Context, prefix string, fn func(Object) error) error {
	token := ""
	for {
		u := s.bucketURL()
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {"1000"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		u.RawQuery = q.Encode()
		resp, err := s.do(ctx, http.MethodGet, u, nil, nil, 0, emptySHA256)
		if err != nil {
			return err
		}
		if resp.StatusCode/100 != 2 {
			return s.errorFrom(resp)
		}
		var lr listResult
		err = xml.NewDecoder(resp.Body).Decode(&lr)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("s3: list: %w", err)
		}
		for _, c := range lr.Contents {
			o := Object{Key: c.Key, Size: c.Size, ETag: strings.Trim(c.ETag, `"`)}
			if t, err := time.Parse(time.RFC3339Nano, c.LastModified); err == nil {
				o.Modified = t.UTC()
			}
			if err := fn(o); err != nil {
				return err
			}
		}
		if !lr.IsTruncated || lr.NextContinuationToken == "" {
			return nil
		}
		token = lr.NextContinuationToken
	}
}

func (s *S3) Locking() bool { return s.cfg.ObjectLock }

// PresignGet returns a query-signed GET URL valid for the given duration
// (at most seven days, the SigV4 limit).
func (s *S3) PresignGet(key string, validity time.Duration) (string, error) {
	if validity <= 0 || validity > 7*24*time.Hour {
		validity = 7 * 24 * time.Hour
	}
	now := s.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := amzDate[:8]
	scope := date + "/" + s.cfg.Region + "/s3/aws4_request"
	u := s.objectURL(key)
	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", s.cfg.AccessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(int(validity/time.Second)))
	q.Set("X-Amz-SignedHeaders", "host")
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var canonQuery []string
	for _, k := range keys {
		canonQuery = append(canonQuery, escapeQuery(k)+"="+escapeQuery(q.Get(k)))
	}
	canonical := strings.Join([]string{http.MethodGet, u.EscapedPath(), strings.Join(canonQuery, "&"), "host:" + u.Host + "\n", "host", "UNSIGNED-PAYLOAD"}, "\n")
	toSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hexSHA256([]byte(canonical))}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(s.cfg.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(toSign)))
	u.RawQuery = strings.Join(canonQuery, "&") + "&X-Amz-Signature=" + sig
	return u.String(), nil
}
