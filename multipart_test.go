package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// setupMultipart wires an embedded NATS object store, a session manager (with
// static.put.begin subscribed), and a server for GET checks.
func setupMultipart(t *testing.T, idleTTL time.Duration) (*nats.Conn, nats.ObjectStore, *server) {
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
		t.Fatalf("create object store: %v", err)
	}

	mgr := newSessionManager(nc, obs, 64, idleTTL)
	if _, err := nc.Subscribe(beginSubject, mgr.begin); err != nil {
		t.Fatalf("subscribe begin: %v", err)
	}
	return nc, obs, &server{obs: obs}
}

func parseKV(s string) map[string]string {
	m := map[string]string{}
	for _, f := range strings.Fields(s) {
		if k, v, ok := strings.Cut(f, "="); ok {
			m[k] = v
		}
	}
	return m
}

// mpBegin opens a session and returns its chunk subject.
func mpBegin(t *testing.T, nc *nats.Conn, hdr nats.Header) string {
	t.Helper()
	req := nats.NewMsg(beginSubject)
	req.Header = hdr
	resp, err := nc.RequestMsg(req, 5*time.Second)
	if err != nil {
		t.Fatalf("begin request: %v", err)
	}
	body := string(resp.Data)
	if !strings.HasPrefix(body, "OK ") {
		t.Fatalf("begin reply = %q, want OK...", body)
	}
	subject := parseKV(body)["subject"]
	if subject == "" {
		t.Fatalf("begin reply missing subject: %q", body)
	}
	return subject
}

func mpChunk(nc *nats.Conn, subject string, seq uint64, data []byte) (string, error) {
	req := nats.NewMsg(subject)
	req.Header.Set("Seq", strconv.FormatUint(seq, 10))
	req.Data = data
	resp, err := nc.RequestMsg(req, 5*time.Second)
	if err != nil {
		return "", err
	}
	return string(resp.Data), nil
}

func mpEOF(nc *nats.Conn, subject string) (string, error) {
	req := nats.NewMsg(subject)
	req.Header.Set("EOF", "true")
	resp, err := nc.RequestMsg(req, 5*time.Second)
	if err != nil {
		return "", err
	}
	return string(resp.Data), nil
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func beginHeader(path, contentType string) nats.Header {
	h := nats.Header{}
	h.Set("Path", path)
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	return h
}

// TestMultipartHappyPath uploads a multi-chunk object and serves it back.
func TestMultipartHappyPath(t *testing.T) {
	nc, obs, srv := setupMultipart(t, 30*time.Second)

	body := payload(300 * 1024) // 300 KB
	const chunkSize = 100 * 1024

	subject := mpBegin(t, nc, beginHeader("big.bin", "application/octet-stream"))

	var seq uint64
	for off := 0; off < len(body); off += chunkSize {
		end := min(off+chunkSize, len(body))
		ack, err := mpChunk(nc, subject, seq, body[off:end])
		if err != nil {
			t.Fatalf("chunk %d: %v", seq, err)
		}
		if want := "OK seq=" + strconv.FormatUint(seq, 10); ack != want {
			t.Fatalf("chunk %d ack = %q, want %q", seq, ack, want)
		}
		seq++
	}

	final, err := mpEOF(nc, subject)
	if err != nil {
		t.Fatalf("EOF: %v", err)
	}
	if !strings.HasPrefix(final, "OK size=") || !strings.Contains(final, "digest=SHA-256=") {
		t.Fatalf("EOF reply = %q, want OK size=.. digest=SHA-256=..", final)
	}

	info, err := obs.GetInfo("big.bin")
	if err != nil {
		t.Fatalf("get info: %v", err)
	}
	if info.Size != uint64(len(body)) {
		t.Errorf("stored size = %d, want %d", info.Size, len(body))
	}

	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/big.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != string(body) {
		t.Errorf("GET body mismatch (len %d, want %d)", rec.Body.Len(), len(body))
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	if rec.Header().Get("ETag") == "" {
		t.Error("missing ETag")
	}
}

// TestMultipartOutOfOrder aborts the session on a sequence gap and leaves no object.
func TestMultipartOutOfOrder(t *testing.T) {
	nc, obs, _ := setupMultipart(t, 30*time.Second)

	subject := mpBegin(t, nc, beginHeader("gap.bin", ""))

	if ack, err := mpChunk(nc, subject, 0, payload(1024)); err != nil || ack != "OK seq=0" {
		t.Fatalf("chunk 0 = %q, err %v", ack, err)
	}
	// Skip seq 1; send seq 2.
	resp, err := mpChunk(nc, subject, 2, payload(1024))
	if err != nil {
		t.Fatalf("chunk 2 request: %v", err)
	}
	if !strings.HasPrefix(resp, "ERR") || !strings.Contains(resp, "out-of-order") {
		t.Fatalf("chunk 2 reply = %q, want ERR out-of-order...", resp)
	}

	if _, err := obs.GetInfo("gap.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("get info = %v, want ErrObjectNotFound (object must not exist)", err)
	}
}

// TestMultipartDigestMismatch rolls back the object when the committed digest
// does not match the digest the client declared in begin.
func TestMultipartDigestMismatch(t *testing.T) {
	nc, obs, _ := setupMultipart(t, 30*time.Second)

	hdr := beginHeader("verify.bin", "")
	hdr.Set("Digest", "SHA-256=deadbeefwrong")
	subject := mpBegin(t, nc, hdr)

	if ack, err := mpChunk(nc, subject, 0, payload(2048)); err != nil || ack != "OK seq=0" {
		t.Fatalf("chunk 0 = %q, err %v", ack, err)
	}
	final, err := mpEOF(nc, subject)
	if err != nil {
		t.Fatalf("EOF: %v", err)
	}
	if !strings.HasPrefix(final, "ERR") || !strings.Contains(final, "digest mismatch") {
		t.Fatalf("EOF reply = %q, want ERR digest mismatch...", final)
	}

	if _, err := obs.GetInfo("verify.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("get info = %v, want ErrObjectNotFound (object must be rolled back)", err)
	}
}

// TestMultipartIdleTimeout tears down a stalled session.
func TestMultipartIdleTimeout(t *testing.T) {
	nc, obs, _ := setupMultipart(t, 150*time.Millisecond)

	subject := mpBegin(t, nc, beginHeader("stall.bin", ""))
	if ack, err := mpChunk(nc, subject, 0, payload(1024)); err != nil || ack != "OK seq=0" {
		t.Fatalf("chunk 0 = %q, err %v", ack, err)
	}

	time.Sleep(500 * time.Millisecond) // exceed idle timeout

	// The session subject is now unsubscribed: a further request either gets no
	// responder or an ERR.
	resp, err := mpEOF(nc, subject)
	if err == nil && !strings.HasPrefix(resp, "ERR") {
		t.Fatalf("EOF after idle = %q (err %v), want no-responder or ERR", resp, err)
	}

	if _, err := obs.GetInfo("stall.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("get info = %v, want ErrObjectNotFound", err)
	}
}

// TestMultipartConcurrent runs two sessions at once to distinct keys.
func TestMultipartConcurrent(t *testing.T) {
	nc, obs, srv := setupMultipart(t, 30*time.Second)

	upload := func(key string, body []byte) error {
		subject := mpBegin(t, nc, beginHeader(key, ""))
		const chunkSize = 64 * 1024
		var seq uint64
		for off := 0; off < len(body); off += chunkSize {
			end := min(off+chunkSize, len(body))
			if _, err := mpChunk(nc, subject, seq, body[off:end]); err != nil {
				return err
			}
			seq++
		}
		final, err := mpEOF(nc, subject)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(final, "OK size=") {
			return errors.New("final: " + final)
		}
		return nil
	}

	a, b := payload(150*1024), payload(220*1024)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = upload("a.bin", a) }()
	go func() { defer wg.Done(); errs[1] = upload("b.bin", b) }()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
	}

	for _, tc := range []struct {
		key  string
		want []byte
	}{{"a.bin", a}, {"b.bin", b}} {
		if info, err := obs.GetInfo(tc.key); err != nil || info.Size != uint64(len(tc.want)) {
			t.Errorf("%s info = %+v err %v, want size %d", tc.key, info, err, len(tc.want))
		}
		rec := httptest.NewRecorder()
		srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/"+tc.key, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != string(tc.want) {
			t.Errorf("%s GET status %d body-match %v", tc.key, rec.Code, rec.Body.String() == string(tc.want))
		}
	}
}

// newManagerEnv builds an embedded NATS object store + session manager with
// injectable limits, subscribing the begin verb bare and prefix-scoped.
func newManagerEnv(t *testing.T, maxSessions int, idleTTL time.Duration) (*nats.Conn, nats.ObjectStore, *sessionManager) {
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
	mgr := newSessionManager(nc, obs, maxSessions, idleTTL)
	for _, s := range []string{beginSubject, beginSubject + ".>"} {
		if _, err := nc.QueueSubscribe(s, queueGroup, mgr.begin); err != nil {
			t.Fatalf("subscribe %s: %v", s, err)
		}
	}
	return nc, obs, mgr
}

// reqStr sends a request and returns the reply body, failing on transport error.
func reqStr(t *testing.T, nc *nats.Conn, m *nats.Msg) string {
	t.Helper()
	resp, err := nc.RequestMsg(m, 5*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", m.Subject, err)
	}
	return string(resp.Data)
}

func beginMsg(hdr nats.Header) *nats.Msg {
	m := nats.NewMsg(beginSubject)
	m.Header = hdr
	return m
}

func TestMultipartChunkTooLarge(t *testing.T) {
	nc, obs, mgr := newManagerEnv(t, 64, 30*time.Second)
	mgr.maxChunk = 8

	kv := parseKV(reqStr(t, nc, beginMsg(beginHeader("big.bin", ""))))
	if kv["max_chunk"] != "8" {
		t.Errorf("advertised max_chunk = %q, want 8", kv["max_chunk"])
	}
	ack, err := mpChunk(nc, kv["subject"], 0, payload(64))
	if err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if !strings.Contains(ack, "chunk too large") {
		t.Fatalf("ack = %q, want ERR chunk too large", ack)
	}
	if _, err := obs.GetInfo("big.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("aborted object must not exist, got %v", err)
	}
}

func TestMultipartSizeMismatch(t *testing.T) {
	nc, obs, _ := newManagerEnv(t, 64, 30*time.Second)

	hdr := beginHeader("sz.bin", "")
	hdr.Set("Size", "5") // declare 5, send 10
	subject := parseKV(reqStr(t, nc, beginMsg(hdr)))["subject"]

	if ack, err := mpChunk(nc, subject, 0, payload(10)); err != nil || !strings.HasPrefix(ack, "OK") {
		t.Fatalf("chunk = %q err %v", ack, err)
	}
	final, err := mpEOF(nc, subject)
	if err != nil {
		t.Fatalf("EOF: %v", err)
	}
	if !strings.Contains(final, "size mismatch") {
		t.Fatalf("final = %q, want ERR size mismatch", final)
	}
	if _, err := obs.GetInfo("sz.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("rolled-back object must not exist, got %v", err)
	}
}

func TestMultipartMaxSessions(t *testing.T) {
	nc, _, _ := newManagerEnv(t, 1, 30*time.Second)

	first := mpBegin(t, nc, beginHeader("a.bin", "")) // occupies the only slot

	if r := reqStr(t, nc, beginMsg(beginHeader("b.bin", ""))); !strings.Contains(r, "too many active upload sessions") {
		t.Fatalf("second begin = %q, want ERR too many active upload sessions", r)
	}

	if _, err := mpEOF(nc, first); err != nil { // finalize frees the slot
		t.Fatalf("finalize first: %v", err)
	}
	third := mpBegin(t, nc, beginHeader("c.bin", "")) // succeeds now
	if _, err := mpEOF(nc, third); err != nil {
		t.Fatalf("finalize third: %v", err)
	}
}

func TestMultipartMalformed(t *testing.T) {
	nc, _, _ := newManagerEnv(t, 64, 30*time.Second)

	if r := reqStr(t, nc, nats.NewMsg(beginSubject)); !strings.Contains(r, "missing Path header") {
		t.Errorf("missing Path = %q", r)
	}

	mode := beginHeader("m.bin", "")
	mode.Set("Mode", "stream")
	if r := reqStr(t, nc, beginMsg(mode)); !strings.Contains(r, "unsupported mode") {
		t.Errorf("bad mode = %q", r)
	}

	size := beginHeader("m.bin", "")
	size.Set("Size", "abc")
	if r := reqStr(t, nc, beginMsg(size)); !strings.Contains(r, "invalid Size header") {
		t.Errorf("bad size = %q", r)
	}

	// Invalid Seq aborts an otherwise-valid session.
	subject := mpBegin(t, nc, beginHeader("m.bin", ""))
	bad := nats.NewMsg(subject)
	bad.Header.Set("Seq", "abc")
	bad.Data = payload(4)
	if r := reqStr(t, nc, bad); !strings.Contains(r, "invalid Seq header") {
		t.Errorf("bad seq = %q", r)
	}
}

func TestMultipartDuplicateSeqAborts(t *testing.T) {
	nc, obs, _ := newManagerEnv(t, 64, 30*time.Second)
	subject := mpBegin(t, nc, beginHeader("dup.bin", ""))

	if ack, err := mpChunk(nc, subject, 0, payload(8)); err != nil || ack != "OK seq=0" {
		t.Fatalf("chunk 0 = %q err %v", ack, err)
	}
	// A resend of seq 0 (lost-ack retry) is out-of-order and aborts the upload.
	ack, err := mpChunk(nc, subject, 0, payload(8))
	if err != nil {
		t.Fatalf("dup chunk: %v", err)
	}
	if !strings.Contains(ack, "out-of-order") {
		t.Fatalf("dup ack = %q, want ERR out-of-order", ack)
	}
	if _, err := obs.GetInfo("dup.bin"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Errorf("aborted object must not exist, got %v", err)
	}
}

func TestMultipartEmptyUpload(t *testing.T) {
	nc, obs, _ := newManagerEnv(t, 64, 30*time.Second)
	srv := &server{obs: obs}

	subject := mpBegin(t, nc, beginHeader("empty.txt", "text/plain"))
	final, err := mpEOF(nc, subject)
	if err != nil {
		t.Fatalf("EOF: %v", err)
	}
	if !strings.HasPrefix(final, "OK size=0 ") {
		t.Fatalf("final = %q, want OK size=0 ...", final)
	}
	info, err := obs.GetInfo("empty.txt")
	if err != nil {
		t.Fatalf("get info: %v", err)
	}
	if info.Size != 0 {
		t.Errorf("size = %d, want 0", info.Size)
	}
	rec := httptest.NewRecorder()
	srv.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/empty.txt", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Errorf("GET empty = %d, body len %d, want 200/0", rec.Code, rec.Body.Len())
	}
}
