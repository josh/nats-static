package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// putData stores an object through the NATS put path with optional extra
// request headers (canonical name -> value).
func putData(t *testing.T, nc *nats.Conn, path string, data []byte, headers map[string]string) {
	t.Helper()
	m := nats.NewMsg(putSubject)
	m.Header.Set("Path", path)
	for k, v := range headers {
		m.Header.Set(k, v)
	}
	m.Data = data
	resp, err := nc.RequestMsg(m, 5*time.Second)
	if err != nil {
		t.Fatalf("put %s: %v", path, err)
	}
	if got := string(resp.Data); got[:2] != "OK" {
		t.Fatalf("put %s = %q", path, got)
	}
}

func get(srv *server, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, req)
	return rec
}

func TestConditionalGETETag(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "a.txt", []byte("x"), nil)

	etag := get(srv, "/a.txt", nil).Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag on plain GET")
	}

	rec := get(srv, "/a.txt", map[string]string{"If-None-Match": etag})
	if rec.Code != http.StatusNotModified {
		t.Fatalf("matching If-None-Match status = %d, want 304", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("304 body = %q, want empty", rec.Body.String())
	}
	if rec.Header().Get("ETag") != etag {
		t.Errorf("304 ETag = %q, want %q", rec.Header().Get("ETag"), etag)
	}

	if rec := get(srv, "/a.txt", map[string]string{"If-None-Match": "*"}); rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match: * status = %d, want 304", rec.Code)
	}
	if rec := get(srv, "/a.txt", map[string]string{"If-None-Match": `"nope"`}); rec.Code != http.StatusOK {
		t.Fatalf("non-matching If-None-Match status = %d, want 200", rec.Code)
	}
}

func TestConditionalGETModifiedSince(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "b.txt", []byte("x"), nil)

	info, err := obs.GetInfo("b.txt")
	if err != nil {
		t.Fatal(err)
	}

	future := info.ModTime.Add(time.Hour).UTC().Format(http.TimeFormat)
	if rec := get(srv, "/b.txt", map[string]string{"If-Modified-Since": future}); rec.Code != http.StatusNotModified {
		t.Fatalf("future If-Modified-Since status = %d, want 304", rec.Code)
	}
	past := info.ModTime.Add(-time.Hour).UTC().Format(http.TimeFormat)
	if rec := get(srv, "/b.txt", map[string]string{"If-Modified-Since": past}); rec.Code != http.StatusOK {
		t.Fatalf("past If-Modified-Since status = %d, want 200", rec.Code)
	}
}

func TestRangeRequest(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "r.txt", []byte("0123456789"), nil)

	rec := get(srv, "/r.txt", map[string]string{"Range": "bytes=0-3"})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("Range status = %d, want 206", rec.Code)
	}
	if rec.Body.String() != "0123" {
		t.Errorf("Range body = %q, want 0123", rec.Body.String())
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes 0-3/10" {
		t.Errorf("Content-Range = %q, want bytes 0-3/10", cr)
	}
}

func TestResponseHeaderReplay(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "h.txt", []byte("x"), map[string]string{
		"Cache-Control":       "max-age=3600",
		"Content-Disposition": `attachment; filename="h.txt"`,
		"X-Evil":              "nope", // not on the allowlist
	})

	rec := get(srv, "/h.txt", nil)
	if got := rec.Header().Get("Cache-Control"); got != "max-age=3600" {
		t.Errorf("Cache-Control = %q, want max-age=3600", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="h.txt"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	if rec.Header().Get("X-Evil") != "" {
		t.Error("non-allowlisted header X-Evil should not be replayed")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	_, obs := setupWrites(t)
	srv := &server{obs: obs}

	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x.txt", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Errorf("Allow = %q, want GET, HEAD", allow)
	}
}

func TestContentTypeFallback(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "style.css", []byte("body{}"), nil) // no Content-Type
	putData(t, nc, "noext", []byte("data"), nil)

	if ct := get(srv, "/style.css", nil).Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/css") {
		t.Errorf("style.css Content-Type = %q, want text/css...", ct)
	}
	if ct := get(srv, "/noext", nil).Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("noext Content-Type = %q, want application/octet-stream", ct)
	}
}

func TestConditionalHEAD(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putData(t, nc, "h.bin", []byte("hello"), nil)

	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/h.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d, want 200", rec.Code)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "5" {
		t.Errorf("HEAD Content-Length = %q, want 5", cl)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD body = %q, want empty", rec.Body.String())
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("HEAD missing ETag")
	}

	req := httptest.NewRequest(http.MethodHead, "/h.bin", nil)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("conditional HEAD status = %d, want 304", rec.Code)
	}
}

func TestDeleteMissingIdempotent(t *testing.T) {
	nc, _ := setupWrites(t)
	m := nats.NewMsg(deleteSubject)
	m.Header.Set("Path", "ghost.txt")
	resp, err := nc.RequestMsg(m, 5*time.Second)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if string(resp.Data) != "OK" {
		t.Fatalf("delete of missing key = %q, want OK (idempotent)", resp.Data)
	}
}

func TestMissingPathHeader(t *testing.T) {
	nc, _ := setupWrites(t)
	for _, subject := range []string{putSubject, deleteSubject} {
		resp, err := nc.RequestMsg(nats.NewMsg(subject), 5*time.Second)
		if err != nil {
			t.Fatalf("%s: %v", subject, err)
		}
		if !strings.Contains(string(resp.Data), "missing Path header") {
			t.Errorf("%s without Path = %q, want ERR missing Path header", subject, resp.Data)
		}
	}
}

func TestMultipartHeaderReplayParity(t *testing.T) {
	nc, _, srv := setupMultipart(t, 30*time.Second)

	hdr := beginHeader("doc.txt", "text/plain")
	hdr.Set("Cache-Control", "max-age=600")
	subject := mpBegin(t, nc, hdr)
	if _, err := mpChunk(nc, subject, 0, []byte("hi")); err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if _, err := mpEOF(nc, subject); err != nil {
		t.Fatalf("EOF: %v", err)
	}

	rec := get(srv, "/doc.txt", nil)
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "max-age=600" {
		t.Errorf("Cache-Control = %q, want max-age=600", cc)
	}
}
