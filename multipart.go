package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// Multipart upload protocol (request/reply, mode "acked"):
//
//   1. Client requests static.put.begin (or static.put.begin.<prefix> to stay
//      within a path prefix) with a Path header (optional Content-Type, Size,
//      Digest, Mode). The server replies
//      "OK session=<id> subject=static.put.session.<id> max_chunk=<n> mode=acked".
//   2. The owning replica plain-Subscribes the unique session subject, so all
//      chunks for the session land on that one replica (no other replica
//      subscribes). The server streams the upload into the object store via an
//      io.Pipe fed to ObjectStore.Put — peak memory is ~one chunk, not the file.
//   3. The client sends each chunk as a request to the session subject with a
//      monotonic Seq header (from 0); the server acks "OK seq=<n>". Any
//      gap/duplicate/out-of-order Seq aborts the session.
//   4. The client sends a final request with header "EOF: true" and empty body.
//      The server closes the pipe, lets Put finish, optionally verifies the
//      Size/Digest from begin, and replies "OK size=<n> digest=<SHA-256=...>".

const (
	beginSubject       = "static.put.begin"
	sessionSubjectTmpl = "static.put.session.%s"
	// headerHeadroom is reserved below the connection max payload (for NATS
	// headers + subject) when advertising the usable chunk size to clients.
	headerHeadroom = 4 * 1024
)

type putResult struct {
	info *nats.ObjectInfo
	err  error
}

// session is a single in-flight chunked upload, owned by one replica.
type session struct {
	id           string
	key          string
	pw           *io.PipeWriter
	done         chan putResult // Put goroutine delivers its result here (cap 1)
	sub          *nats.Subscription
	idle         *time.Timer
	expectSeq    uint64 // touched only by the (serialized) chunk handler
	expectSize   uint64 // 0 = client did not declare a size
	expectDigest string // "" = client did not declare a digest
	closed       atomic.Bool
}

// sessionManager handles static.put.begin and owns the active sessions on this
// replica.
type sessionManager struct {
	nc       *nats.Conn
	obs      nats.ObjectStore
	maxChunk int
	idleTTL  time.Duration

	sem      chan struct{} // bounds concurrent sessions (and thus memory)
	mu       sync.Mutex
	sessions map[string]*session
}

func newSessionManager(nc *nats.Conn, obs nats.ObjectStore, maxSessions int, idleTTL time.Duration) *sessionManager {
	maxChunk := int(nc.MaxPayload()) - headerHeadroom
	if maxChunk < 1 {
		maxChunk = int(nc.MaxPayload())
	}
	return &sessionManager{
		nc:       nc,
		obs:      obs,
		maxChunk: maxChunk,
		idleTTL:  idleTTL,
		sem:      make(chan struct{}, maxSessions),
		sessions: make(map[string]*session),
	}
}

// begin starts a new upload session in response to a static.put.begin request.
func (m *sessionManager) begin(msg *nats.Msg) {
	path := msg.Header.Get("Path")
	if path == "" {
		msg.Respond([]byte("ERR missing Path header"))
		return
	}
	if mode := msg.Header.Get("Mode"); mode != "" && mode != "acked" {
		msg.Respond([]byte("ERR unsupported mode " + mode))
		return
	}
	key := normalizeKey(path)
	if !withinPrefix(key, subjectPrefix(msg.Subject, beginSubject)) {
		msg.Respond([]byte("ERR path outside permitted prefix"))
		return
	}

	var expectSize uint64
	if s := msg.Header.Get("Size"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			msg.Respond([]byte("ERR invalid Size header"))
			return
		}
		expectSize = n
	}

	// Reserve a session slot without blocking the NATS callback.
	select {
	case m.sem <- struct{}{}:
	default:
		msg.Respond([]byte("ERR too many active upload sessions"))
		return
	}

	meta := &nats.ObjectMeta{Name: key, Metadata: uploadMetadata(msg.Header)}

	pr, pw := io.Pipe()
	sess := &session{
		id:           nuid.Next(),
		key:          key,
		pw:           pw,
		done:         make(chan putResult, 1),
		expectSize:   expectSize,
		expectDigest: msg.Header.Get("Digest"),
	}

	// Stream the pipe into the object store. Put reads in 128 KB chunks and
	// publishes as it goes, so memory stays bounded; on a pipe error it purges
	// the partial object.
	go func() {
		info, err := m.obs.Put(meta, pr)
		sess.done <- putResult{info: info, err: err}
	}()

	subject := fmt.Sprintf(sessionSubjectTmpl, sess.id)
	sub, err := m.nc.Subscribe(subject, func(cm *nats.Msg) { m.chunk(sess, cm) })
	if err != nil {
		pw.CloseWithError(err)
		<-sess.done
		<-m.sem
		msg.Respond([]byte("ERR " + err.Error()))
		return
	}
	sess.sub = sub
	sess.idle = time.AfterFunc(m.idleTTL, func() { m.abortIdle(sess) })

	m.mu.Lock()
	m.sessions[sess.id] = sess
	m.mu.Unlock()

	msg.Respond([]byte(fmt.Sprintf("OK session=%s subject=%s max_chunk=%d mode=acked",
		sess.id, subject, m.maxChunk)))
}

// chunk handles one message on a session subject. Callbacks for a single
// subscription are delivered serially, so the only concurrent actor is the idle
// timer; sess.closed (CAS) ensures the session is finished exactly once.
func (m *sessionManager) chunk(sess *session, msg *nats.Msg) {
	if sess.closed.Load() {
		msg.Respond([]byte("ERR session closed"))
		return
	}
	if sess.idle != nil {
		sess.idle.Reset(m.idleTTL)
	}

	if msg.Header.Get("EOF") == "true" {
		m.finalize(sess, msg)
		return
	}

	seq, err := strconv.ParseUint(msg.Header.Get("Seq"), 10, 64)
	if err != nil {
		m.abortChunk(sess, msg, "ERR invalid Seq header")
		return
	}
	if seq != sess.expectSeq {
		m.abortChunk(sess, msg, fmt.Sprintf("ERR out-of-order chunk: got seq %d want %d", seq, sess.expectSeq))
		return
	}
	if len(msg.Data) > m.maxChunk {
		m.abortChunk(sess, msg, fmt.Sprintf("ERR chunk too large: %d > %d", len(msg.Data), m.maxChunk))
		return
	}
	// Write blocks until Put consumes the chunk — natural backpressure. A write
	// error means Put failed or a concurrent abort closed the pipe.
	if _, err := sess.pw.Write(msg.Data); err != nil {
		m.abortChunk(sess, msg, "ERR "+err.Error())
		return
	}
	sess.expectSeq++
	msg.Respond([]byte(fmt.Sprintf("OK seq=%d", seq)))
}

// finalize closes the upload, waits for Put, verifies any declared size/digest,
// and replies with the committed result.
func (m *sessionManager) finalize(sess *session, msg *nats.Msg) {
	if !sess.closed.CompareAndSwap(false, true) {
		msg.Respond([]byte("ERR session closed"))
		return
	}
	sess.pw.Close() // EOF to Put
	res := <-sess.done
	m.release(sess)

	if res.err != nil {
		msg.Respond([]byte("ERR " + res.err.Error()))
		return
	}
	info := res.info
	if sess.expectSize != 0 && info.Size != sess.expectSize {
		m.obs.Delete(sess.key)
		msg.Respond([]byte(fmt.Sprintf("ERR size mismatch: got %d want %d", info.Size, sess.expectSize)))
		return
	}
	if sess.expectDigest != "" && info.Digest != sess.expectDigest {
		m.obs.Delete(sess.key)
		msg.Respond([]byte(fmt.Sprintf("ERR digest mismatch: got %s want %s", info.Digest, sess.expectDigest)))
		return
	}
	msg.Respond([]byte(fmt.Sprintf("OK size=%d digest=%s", info.Size, info.Digest)))
}

// abortChunk fails the session in response to a bad or failed chunk, always
// replying ERR to the offending request.
func (m *sessionManager) abortChunk(sess *session, msg *nats.Msg, reason string) {
	if sess.closed.CompareAndSwap(false, true) {
		sess.pw.CloseWithError(errors.New(reason))
		<-sess.done
		m.release(sess)
	}
	msg.Respond([]byte(reason))
}

// abortIdle tears down a session that stalled past the idle timeout.
func (m *sessionManager) abortIdle(sess *session) {
	if sess.closed.CompareAndSwap(false, true) {
		sess.pw.CloseWithError(errors.New("idle timeout"))
		<-sess.done
		m.release(sess)
	}
}

// release frees a finished session's resources. Called exactly once, by the
// goroutine that won the closed CAS.
func (m *sessionManager) release(sess *session) {
	if sess.idle != nil {
		sess.idle.Stop()
	}
	if sess.sub != nil {
		sess.sub.Unsubscribe()
	}
	m.mu.Lock()
	delete(m.sessions, sess.id)
	m.mu.Unlock()
	<-m.sem
}
