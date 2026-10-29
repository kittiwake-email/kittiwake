package target

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func conformance(t *testing.T, tg Target) {
	t.Helper()
	ctx := context.Background()
	data := []byte("hello backup world")
	if err := tg.Put(ctx, "mb/a/run1.pack", bytes.NewReader(data), int64(len(data)), PutOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	if err := tg.Put(ctx, "mb/a/run1.manifest", strings.NewReader("{}"), -1, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := tg.Put(ctx, "mb/b/x", strings.NewReader("other"), 5, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := tg.Put(ctx, "blob/ab/weird key+name", strings.NewReader("w"), 1, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(ctx, tg, "mb/a/run1.pack")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read all: %q %v", got, err)
	}
	part, err := ReadRange(ctx, tg, "mb/a/run1.pack", 6, 6)
	if err != nil || string(part) != "backup" {
		t.Fatalf("range: %q %v", part, err)
	}
	tail, err := ReadAll(ctx, tg, "mb/a/run1.pack")
	if err != nil || len(tail) != len(data) {
		t.Fatal(err)
	}
	rc, err := tg.Get(ctx, "mb/a/run1.pack", 13, -1)
	if err != nil {
		t.Fatal(err)
	}
	rest, _ := io.ReadAll(rc)
	rc.Close()
	if string(rest) != "world" {
		t.Fatalf("open range: %q", rest)
	}
	o, err := tg.Head(ctx, "mb/a/run1.pack")
	if err != nil || o.Size != int64(len(data)) || o.Key != "mb/a/run1.pack" || o.ETag == "" {
		t.Fatalf("head: %+v %v", o, err)
	}
	if _, err := tg.Head(ctx, "mb/a/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("head missing: %v", err)
	}
	if _, err := tg.Get(ctx, "mb/a/nope", 0, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
	var keys []string
	if err := tg.List(ctx, "mb/a/", func(o Object) error {
		keys = append(keys, o.Key)
		if o.Size == 0 && o.Key != "mb/a/run1.manifest" {
			return fmt.Errorf("size missing on %s", o.Key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, ",") != "mb/a/run1.manifest,mb/a/run1.pack" {
		t.Fatalf("list: %v", keys)
	}
	keys = nil
	_ = tg.List(ctx, "", func(o Object) error { keys = append(keys, o.Key); return nil })
	if len(keys) != 4 || !sort.StringsAreSorted(keys) {
		t.Fatalf("list all: %v", keys)
	}
	stop := errors.New("stop")
	n := 0
	if err := tg.List(ctx, "", func(Object) error { n++; return stop }); !errors.Is(err, stop) || n != 1 {
		t.Fatalf("list stop: %d %v", n, err)
	}
	if err := tg.Delete(ctx, "mb/b/x"); err != nil {
		t.Fatal(err)
	}
	if err := tg.Delete(ctx, "mb/b/x"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
	if _, err := tg.Head(ctx, "mb/b/x"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted")
	}
	if got, err := ReadAll(ctx, tg, "blob/ab/weird key+name"); err != nil || string(got) != "w" {
		t.Fatalf("odd key: %q %v", got, err)
	}
}

func TestMemory(t *testing.T) { conformance(t, NewMemory()) }

func TestFS(t *testing.T) {
	fs, err := NewFS(t.TempDir() + "/backups")
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, fs)
	if _, err := fs.Head(context.Background(), "../etc/passwd"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal("path escape must be refused")
	}
}

func TestMemoryLock(t *testing.T) {
	m := NewMemory()
	m.Lock = true
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	m.Now = func() time.Time { return now }
	ctx := context.Background()
	_ = m.Put(ctx, "k", strings.NewReader("v"), 1, PutOptions{RetainUntil: now.Add(time.Hour)})
	if err := m.Delete(ctx, "k"); !errors.Is(err, ErrLocked) {
		t.Fatalf("locked: %v", err)
	}
	now = now.Add(2 * time.Hour)
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
}

// fakeS3 is a minimal S3 server: enough of the protocol for the target.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	locks   map[string]time.Time
	bucket  string
	calls   []string
	fail5xx int // remaining requests to answer 503
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKID/") || !strings.Contains(auth, "host;") || !strings.Contains(auth, "x-amz-content-sha256;x-amz-date") || r.Header.Get("x-amz-date") == "" {
		http.Error(w, "<Error><Code>AccessDenied</Code><Message>bad auth</Message></Error>", 403)
		return
	}
	if f.fail5xx > 0 {
		f.fail5xx--
		http.Error(w, "<Error><Code>SlowDown</Code><Message>try later</Message></Error>", 503)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/"+f.bucket+"/")
	if path == r.URL.Path {
		http.Error(w, "<Error><Code>NoSuchBucket</Code></Error>", 404)
		return
	}
	key, _ := url.PathUnescape(path)
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		start := 0
		if tok := r.URL.Query().Get("continuation-token"); tok != "" {
			start, _ = strconv.Atoi(tok)
		}
		end := start + 2 // tiny pages to exercise continuation
		truncated := end < len(keys)
		if !truncated {
			end = len(keys)
		}
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>` + strconv.FormatBool(truncated) + `</IsTruncated>`)
		if truncated {
			fmt.Fprintf(&b, "<NextContinuationToken>%d</NextContinuationToken>", end)
		}
		for _, k := range keys[start:end] {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-10-28T00:00:00.000Z</LastModified><ETag>"%x"</ETag></Contents>`, k, len(f.objects[k]), len(f.objects[k]))
		}
		b.WriteString("</ListBucketResult>")
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if cl := r.ContentLength; cl >= 0 && int(cl) != len(body) {
			http.Error(w, "<Error><Code>IncompleteBody</Code></Error>", 400)
			return
		}
		f.objects[key] = body
		if until := r.Header.Get("x-amz-object-lock-retain-until-date"); until != "" {
			t, _ := time.Parse(time.RFC3339, until)
			f.locks[key] = t
		}
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(200)
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
		data, ok := f.objects[key]
		if !ok {
			http.Error(w, "<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>", 404)
			return
		}
		status := 200
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int
			if n, _ := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); n == 2 {
				data = data[a:min(b+1, len(data))]
			} else if n, _ := fmt.Sscanf(rg, "bytes=%d-", &a); n == 1 {
				data = data[a:]
			}
			status = 206
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", "Tue, 28 Oct 2026 00:00:00 GMT")
		w.WriteHeader(status)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case r.Method == http.MethodDelete:
		if until, ok := f.locks[key]; ok && time.Now().Before(until) {
			http.Error(w, "<Error><Code>AccessDenied</Code><Message>Object is under retention lock</Message></Error>", 403)
			return
		}
		delete(f.objects, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "nope", 405)
	}
}

func newFakeS3(t *testing.T) (*fakeS3, *S3) {
	t.Helper()
	f := &fakeS3{objects: map[string][]byte{}, locks: map[string]time.Time{}, bucket: "kw"}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s, err := NewS3(S3Config{Endpoint: srv.URL, Bucket: "kw", AccessKey: "AKID", SecretKey: "secret", PathStyle: true, ObjectLock: true, Client: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return f, s
}

func TestS3(t *testing.T) {
	f, s := newFakeS3(t)
	conformance(t, s)
	ctx := context.Background()
	if err := s.Put(ctx, "locked", strings.NewReader("x"), 1, PutOptions{RetainUntil: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "locked"); !errors.Is(err, ErrLocked) {
		t.Fatalf("locked: %v", err)
	}
	if !s.Locking() {
		t.Fatal("locking")
	}
	// Transient 5xx answers are retried with the body resent.
	f.fail5xx = 2
	if err := s.Put(ctx, "retry", bytes.NewReader([]byte("again")), 5, PutOptions{}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, _ := ReadAll(ctx, s, "retry"); string(got) != "again" {
		t.Fatalf("after retry: %q", got)
	}
	if _, err := NewS3(S3Config{Endpoint: "ftp://x", Bucket: "b", AccessKey: "a", SecretKey: "s"}); err == nil {
		t.Fatal("endpoint validation")
	}
}

func TestS3VirtualHostPaths(t *testing.T) {
	s, _ := NewS3(S3Config{Endpoint: "https://s3.example.net", Bucket: "kw", AccessKey: "a", SecretKey: "s"})
	if u := s.objectURL("mb/a b/c"); u.String() != "https://kw.s3.example.net/mb/a%20b/c" {
		t.Fatalf("virtual host: %s", u)
	}
	s, _ = NewS3(S3Config{Endpoint: "http://[::1]:9000", Bucket: "kw", AccessKey: "a", SecretKey: "s", PathStyle: true})
	if u := s.objectURL("x/y"); u.String() != "http://[::1]:9000/kw/x/y" {
		t.Fatalf("path style: %s", u)
	}
}

// TestSigV4Vector checks the signer against the worked example in the AWS
// documentation (GET object with a Range header, virtual-host style).
func TestSigV4Vector(t *testing.T) {
	s, _ := NewS3(S3Config{Endpoint: "https://s3.amazonaws.com", Bucket: "examplebucket", Region: "us-east-1", AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
	req, _ := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("range", "bytes=0-9")
	s.sign(req, emptySHA256, time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("signature\n got %s\nwant %s", got, want)
	}
}

// TestPresignVector checks query-string signing against the AWS worked
// example (presigned GET, 24 hours).
func TestPresignVector(t *testing.T) {
	s, _ := NewS3(S3Config{Endpoint: "https://s3.amazonaws.com", Bucket: "examplebucket", Region: "us-east-1", AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
	s.Now = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }
	u, err := s.PresignGet("test.txt", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if u != want {
		t.Fatalf("presign\n got %s\nwant %s", u, want)
	}
}
