package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// setupWrites wires the full write-subject tree (bare + prefix wildcard per
// verb) exactly as main does, so tests exercise real subject routing.
func setupWrites(t *testing.T) (*nats.Conn, nats.ObjectStore) {
	t.Helper()
	nc, err := nats.Connect(startEmbeddedNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "static"})
	if err != nil {
		t.Fatalf("object store: %v", err)
	}

	srv := &server{obs: obs}
	mgr := newSessionManager(nc, obs, 64, 30*time.Second)
	subs := map[string]nats.MsgHandler{
		putSubject:           srv.put,
		putSubject + ".>":    srv.put,
		deleteSubject:        srv.del,
		deleteSubject + ".>": srv.del,
		beginSubject:         mgr.begin,
		beginSubject + ".>":  mgr.begin,
	}
	for subject, h := range subs {
		if _, err := nc.QueueSubscribe(subject, queueGroup, h); err != nil {
			t.Fatalf("subscribe %s: %v", subject, err)
		}
	}
	return nc, obs
}

func putReq(t *testing.T, nc *nats.Conn, subject, path string) string {
	t.Helper()
	m := nats.NewMsg(subject)
	m.Header.Set("Path", path)
	m.Data = []byte("x")
	resp, err := nc.RequestMsg(m, 5*time.Second)
	if err != nil {
		t.Fatalf("put req %s: %v", subject, err)
	}
	return string(resp.Data)
}

func TestPrefixScopedPut(t *testing.T) {
	nc, obs := setupWrites(t)

	if r := putReq(t, nc, putSubject+".blog", "blog/post.html"); !strings.HasPrefix(r, "OK") {
		t.Fatalf("in-prefix put = %q, want OK", r)
	}
	if _, err := obs.GetInfo("blog/post.html"); err != nil {
		t.Fatalf("in-prefix object should exist: %v", err)
	}

	if r := putReq(t, nc, putSubject+".blog.img", "blog/img/logo.png"); !strings.HasPrefix(r, "OK") {
		t.Fatalf("nested-prefix put = %q, want OK", r)
	}

	if r := putReq(t, nc, putSubject+".blog", "other/x.html"); !strings.Contains(r, "outside permitted prefix") {
		t.Fatalf("out-of-prefix put = %q, want ERR outside permitted prefix", r)
	}
	if _, err := obs.GetInfo("other/x.html"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("out-of-prefix object must not exist, got err %v", err)
	}

	// A prefix token must not match as a mere string prefix of a sibling dir.
	if r := putReq(t, nc, putSubject+".blog", "blogspot/x.html"); !strings.Contains(r, "outside permitted prefix") {
		t.Fatalf("sibling-dir put = %q, want ERR (blogspot is not under blog)", r)
	}

	if r := putReq(t, nc, putSubject, "anywhere/file.txt"); !strings.HasPrefix(r, "OK") {
		t.Fatalf("bare put = %q, want OK", r)
	}
}

func TestPrefixScopedDelete(t *testing.T) {
	nc, obs := setupWrites(t)
	putReq(t, nc, putSubject, "blog/a.html")
	putReq(t, nc, putSubject, "secret/b.html")

	out := nats.NewMsg(deleteSubject + ".blog")
	out.Header.Set("Path", "secret/b.html")
	resp, err := nc.RequestMsg(out, 5*time.Second)
	if err != nil {
		t.Fatalf("delete req: %v", err)
	}
	if !strings.Contains(string(resp.Data), "outside permitted prefix") {
		t.Fatalf("out-of-prefix delete = %q, want ERR", resp.Data)
	}
	if _, err := obs.GetInfo("secret/b.html"); err != nil {
		t.Errorf("secret object must still exist: %v", err)
	}

	in := nats.NewMsg(deleteSubject + ".blog")
	in.Header.Set("Path", "blog/a.html")
	resp, err = nc.RequestMsg(in, 5*time.Second)
	if err != nil {
		t.Fatalf("delete req: %v", err)
	}
	if string(resp.Data) != "OK" {
		t.Fatalf("in-prefix delete = %q, want OK", resp.Data)
	}
	if _, err := obs.GetInfo("blog/a.html"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("blog object should be deleted, got %v", err)
	}
}

func TestPrefixScopedBegin(t *testing.T) {
	nc, _ := setupWrites(t)

	// Out-of-prefix begin is rejected up front.
	out := nats.NewMsg(beginSubject + ".blog")
	out.Header.Set("Path", "other/big.bin")
	resp, err := nc.RequestMsg(out, 5*time.Second)
	if err != nil {
		t.Fatalf("begin req: %v", err)
	}
	if !strings.Contains(string(resp.Data), "outside permitted prefix") {
		t.Fatalf("out-of-prefix begin = %q, want ERR", resp.Data)
	}

	// In-prefix begin opens a session; finalize it to clean up.
	in := nats.NewMsg(beginSubject + ".blog")
	in.Header.Set("Path", "blog/big.bin")
	resp, err = nc.RequestMsg(in, 5*time.Second)
	if err != nil {
		t.Fatalf("begin req: %v", err)
	}
	if !strings.HasPrefix(string(resp.Data), "OK ") {
		t.Fatalf("in-prefix begin = %q, want OK", resp.Data)
	}
	if _, err := mpEOF(nc, parseKV(string(resp.Data))["subject"]); err != nil {
		t.Fatalf("finalize: %v", err)
	}
}
