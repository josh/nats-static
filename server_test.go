package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startEmbeddedNATS boots an in-process NATS server with JetStream enabled and
// returns its client URL. The server and its on-disk store are torn down at the
// end of the test.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

// TestPutGetDelete exercises a single put -> get -> delete round-trip against a
// real (embedded) NATS JetStream object store — no external services or mocks.
func TestPutGetDelete(t *testing.T) {
	nc, err := nats.Connect(startEmbeddedNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// In production NACK provisions this bucket; the server only opens it. Here
	// the test stands in for NACK and creates it.
	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "static"})
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}

	srv := &server{obs: obs}
	if _, err := nc.QueueSubscribe(putSubject, queueGroup, srv.put); err != nil {
		t.Fatalf("subscribe %s: %v", putSubject, err)
	}
	if _, err := nc.QueueSubscribe(deleteSubject, queueGroup, srv.del); err != nil {
		t.Fatalf("subscribe %s: %v", deleteSubject, err)
	}

	body := []byte("<h1>hello</h1>")

	putMsg := nats.NewMsg(putSubject)
	putMsg.Header.Set("Path", "index.html")
	putMsg.Header.Set("Content-Type", "text/html")
	putMsg.Data = body
	resp, err := nc.RequestMsg(putMsg, 5*time.Second)
	if err != nil {
		t.Fatalf("put request: %v", err)
	}
	if !strings.HasPrefix(string(resp.Data), "OK") {
		t.Fatalf("put response = %q, want OK...", resp.Data)
	}

	info, err := obs.GetInfo("index.html")
	if err != nil {
		t.Fatalf("get info after put: %v", err)
	}
	if info.Size != uint64(len(body)) {
		t.Errorf("stored size = %d, want %d", info.Size, len(body))
	}
	if got := info.Metadata["content-type"]; got != "text/html" {
		t.Errorf("stored content-type = %q, want text/html", got)
	}

	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/index.html", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != string(body) {
		t.Errorf("GET body = %q, want %q", rec.Body.String(), body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html" {
		t.Errorf("GET Content-Type = %q, want text/html", ct)
	}
	if rec.Header().Get("ETag") == "" {
		t.Error("GET response missing ETag")
	}

	delMsg := nats.NewMsg(deleteSubject)
	delMsg.Header.Set("Path", "index.html")
	resp, err = nc.RequestMsg(delMsg, 5*time.Second)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	if string(resp.Data) != "OK" {
		t.Fatalf("delete response = %q, want OK", resp.Data)
	}

	if _, err := obs.GetInfo("index.html"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("get info after delete = %v, want ErrObjectNotFound", err)
	}

	rec = httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/index.html", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET after delete status = %d, want 404", rec.Code)
	}
}
