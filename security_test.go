package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

// TestNormalizeKey locks the containment behavior: cleaned keys never retain
// ".." segments, so a request can't escape the object-store namespace.
func TestNormalizeKey(t *testing.T) {
	cases := map[string]string{
		"":                    "index.html",
		"/":                   "index.html",
		"index.html":          "index.html",
		"/a/b.txt":            "a/b.txt",
		"./foo":               "foo",
		"../../../etc/passwd": "etc/passwd",
		"/../secret":          "secret",
		"a/../../../x":        "x",
	}
	for in, want := range cases {
		if got := normalizeKey(in); got != want {
			t.Errorf("normalizeKey(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(normalizeKey(in), "..") {
			t.Errorf("normalizeKey(%q) leaked a .. segment: %q", in, normalizeKey(in))
		}
	}
}

// TestTraversalNeutralizedOnWrite confirms a traversal Path header is cleaned to
// a contained key and never written verbatim.
func TestTraversalNeutralizedOnWrite(t *testing.T) {
	nc, obs := setupWrites(t)

	if r := putReq(t, nc, putSubject, "../../../etc/passwd"); !strings.HasPrefix(r, "OK") {
		t.Fatalf("put = %q, want OK", r)
	}
	if _, err := obs.GetInfo("etc/passwd"); err != nil {
		t.Fatalf("expected contained key etc/passwd: %v", err)
	}
	if _, err := obs.GetInfo("../../../etc/passwd"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("raw traversal key must not exist, got %v", err)
	}
}

// TestTraversalNeutralizedOnRead exercises normalizeKey in the GET path directly
// (bypassing ServeMux, which also cleans the URL as a first layer of defense).
func TestTraversalNeutralizedOnRead(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putReq(t, nc, putSubject, "etc/passwd")

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.URL.Path = "/../../etc/passwd" // dirty path straight into the handler
	rec := httptest.NewRecorder()
	srv.serveHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("traversal GET = %d, want 200 (resolves to etc/passwd)", rec.Code)
	}
	if rec.Body.String() != "x" {
		t.Errorf("body = %q, want x", rec.Body.String())
	}
}

// TestPrefixEscapeViaTraversalRejected confirms normalize-then-withinPrefix
// ordering: a traversal inside a scoped prefix can't escape it.
func TestPrefixEscapeViaTraversalRejected(t *testing.T) {
	nc, obs := setupWrites(t)
	for _, p := range []string{"blog/../secret/x", "blog/a/../../../etc/x"} {
		if r := putReq(t, nc, putSubject+".blog", p); !strings.Contains(r, "outside permitted prefix") {
			t.Fatalf("put %q = %q, want ERR outside permitted prefix", p, r)
		}
	}
	if _, err := obs.GetInfo("secret/x"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("escaped object must not exist, got %v", err)
	}
}

// TestRootServesIndex confirms a bare "/" GET resolves to index.html.
func TestRootServesIndex(t *testing.T) {
	nc, obs := setupWrites(t)
	srv := &server{obs: obs}
	putReq(t, nc, putSubject, "index.html")

	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "x" {
		t.Errorf("GET / body = %q, want x", rec.Body.String())
	}
}
